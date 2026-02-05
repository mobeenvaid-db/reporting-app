"""
OAuth Middleware for Databricks Apps On-Behalf-Of Authentication
Extracts user context from OAuth tokens and validates permissions
Reference: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth
"""

from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, Dict, Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
import os
import logging
from datetime import datetime, timedelta
from functools import lru_cache

logger = logging.getLogger(__name__)

# Security scheme for Bearer token
security = HTTPBearer(auto_error=False)


class UserContext:
    """Represents authenticated user context"""
    
    def __init__(
        self,
        email: str,
        user_id: str,
        groups: list[str],
        is_admin: bool = False,
        workspace_client: Optional[WorkspaceClient] = None
    ):
        self.email = email
        self.user_id = user_id
        self.groups = groups
        self.is_admin = is_admin
        self.workspace_client = workspace_client
        self.authenticated_at = datetime.utcnow()
    
    def has_group(self, group_name: str) -> bool:
        """Check if user belongs to a specific group"""
        return group_name in self.groups
    
    def is_session_valid(self, max_age_minutes: int = 60) -> bool:
        """Check if session is still valid"""
        age = datetime.utcnow() - self.authenticated_at
        return age < timedelta(minutes=max_age_minutes)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "email": self.email,
            "user_id": self.user_id,
            "groups": self.groups,
            "is_admin": self.is_admin,
            "authenticated_at": self.authenticated_at.isoformat()
        }


class AuthenticationError(HTTPException):
    """Custom exception for authentication errors"""
    
    def __init__(self, detail: str):
        super().__init__(status_code=401, detail=detail)


class AuthorizationError(HTTPException):
    """Custom exception for authorization errors"""
    
    def __init__(self, detail: str):
        super().__init__(status_code=403, detail=detail)


def get_workspace_client_for_user(token: str) -> WorkspaceClient:
    """
    Create a WorkspaceClient with the user's OAuth token
    This enables on-behalf-of operations
    """
    host = os.getenv("DATABRICKS_HOST")
    if not host:
        host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        if host and not host.startswith("https://"):
            host = f"https://{host}"
    
    if not host:
        raise AuthenticationError("Databricks host not configured")
    
    # Create config with user's token
    config = Config(
        host=host,
        token=token
    )
    
    return WorkspaceClient(config=config)


async def extract_user_from_token(token: str) -> UserContext:
    """
    Extract user context from OAuth token
    
    In Databricks Apps, the OAuth token contains user identity.
    This function validates the token and extracts user information.
    """
    try:
        # Create workspace client with user's token
        client = get_workspace_client_for_user(token)
        
        # Get current user information
        current_user = client.current_user.me()
        
        # Extract user details
        email = current_user.user_name or current_user.emails[0].value if current_user.emails else "unknown"
        user_id = current_user.id
        
        # Get user's groups
        groups = []
        if current_user.groups:
            groups = [g.display for g in current_user.groups]
        
        # Check if user is admin (workspace admin or in admin group)
        is_admin = any(
            group in ["admins", "account admins", "workspace admins"] 
            for group in [g.lower() for g in groups]
        )
        
        logger.info(f"Authenticated user: {email} (admin: {is_admin})")
        
        return UserContext(
            email=email,
            user_id=user_id,
            groups=groups,
            is_admin=is_admin,
            workspace_client=client
        )
        
    except Exception as e:
        logger.error(f"Failed to extract user from token: {str(e)}")
        raise AuthenticationError(f"Invalid or expired token: {str(e)}")


async def get_user_context(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> UserContext:
    """
    FastAPI dependency to extract and validate user context
    
    Usage in endpoints:
        @app.get("/protected")
        async def protected_route(user: UserContext = Depends(get_user_context)):
            return {"user": user.email}
    """
    # Check for token in Authorization header
    if not credentials:
        # Check if we're in dev mode with a test token
        if os.getenv("DEV_MODE") == "true":
            test_email = os.getenv("DEV_USER_EMAIL", "dev@example.com")
            logger.warning(f"DEV MODE: Using test user {test_email}")
            return UserContext(
                email=test_email,
                user_id="dev-user-id",
                groups=["users", "admins"],
                is_admin=True
            )
        
        raise AuthenticationError("Missing authentication token")
    
    token = credentials.credentials
    
    # Extract user context from token
    user_context = await extract_user_from_token(token)
    
    # Validate session
    if not user_context.is_session_valid():
        raise AuthenticationError("Session expired, please re-authenticate")
    
    # Store user context in request state for later use
    request.state.user = user_context
    
    return user_context


async def get_admin_user(
    user: UserContext = Depends(get_user_context)
) -> UserContext:
    """
    FastAPI dependency that requires admin privileges
    
    Usage:
        @app.post("/admin/endpoint")
        async def admin_only(user: UserContext = Depends(get_admin_user)):
            return {"status": "admin access granted"}
    """
    if not user.is_admin:
        raise AuthorizationError(
            f"Admin privileges required. User {user.email} is not an admin."
        )
    return user


async def require_group(group_name: str):
    """
    Factory function to create a dependency that requires specific group membership
    
    Usage:
        @app.get("/analytics")
        async def analytics(user: UserContext = Depends(require_group("analytics_team"))):
            return {"data": "sensitive"}
    """
    async def check_group(user: UserContext = Depends(get_user_context)) -> UserContext:
        if not user.has_group(group_name) and not user.is_admin:
            raise AuthorizationError(
                f"Group '{group_name}' membership required. User {user.email} is not a member."
            )
        return user
    return check_group


def get_user_from_request(request: Request) -> Optional[UserContext]:
    """
    Helper function to get user context from request state
    Useful in middleware and exception handlers
    """
    return getattr(request.state, "user", None)


# Session cache for performance (in production, use Redis or similar)
# This prevents re-validating the same token on every request
_session_cache: Dict[str, tuple[UserContext, datetime]] = {}
_cache_ttl_minutes = 5


def cache_user_session(token: str, user: UserContext):
    """Cache user session for performance"""
    _session_cache[token] = (user, datetime.utcnow())


def get_cached_session(token: str) -> Optional[UserContext]:
    """Get cached user session if still valid"""
    if token not in _session_cache:
        return None
    
    user, cached_at = _session_cache[token]
    age = datetime.utcnow() - cached_at
    
    if age < timedelta(minutes=_cache_ttl_minutes):
        return user
    
    # Cache expired, remove it
    del _session_cache[token]
    return None


async def get_user_context_cached(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> UserContext:
    """
    Cached version of get_user_context for better performance
    Use this in high-traffic endpoints
    """
    if not credentials:
        if os.getenv("DEV_MODE") == "true":
            test_email = os.getenv("DEV_USER_EMAIL", "dev@example.com")
            return UserContext(
                email=test_email,
                user_id="dev-user-id",
                groups=["users", "admins"],
                is_admin=True
            )
        raise AuthenticationError("Missing authentication token")
    
    token = credentials.credentials
    
    # Check cache first
    cached_user = get_cached_session(token)
    if cached_user:
        request.state.user = cached_user
        return cached_user
    
    # Not in cache, extract from token
    user_context = await extract_user_from_token(token)
    
    # Cache for future requests
    cache_user_session(token, user_context)
    
    request.state.user = user_context
    return user_context
