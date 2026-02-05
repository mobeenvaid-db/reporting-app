"""
User Context and Session Management
Handles user identity and data access scope
"""

from typing import Optional, Set, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class AccessLevel(Enum):
    """Define access levels for data"""
    NONE = "none"
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


@dataclass
class DataAccessScope:
    """
    Defines what data a user can access
    Based on Unity Catalog permissions and row-level security rules
    """
    
    # Catalogs user has access to
    accessible_catalogs: Set[str] = field(default_factory=set)
    
    # Schemas user has access to (format: "catalog.schema")
    accessible_schemas: Set[str] = field(default_factory=set)
    
    # Tables user has access to (format: "catalog.schema.table")
    accessible_tables: Set[str] = field(default_factory=set)
    
    # Row-level filters to apply (table_name -> filter_expression)
    row_level_filters: Dict[str, str] = field(default_factory=dict)
    
    # Column-level restrictions (table_name -> set of restricted columns)
    restricted_columns: Dict[str, Set[str]] = field(default_factory=dict)
    
    # Access level per resource
    access_levels: Dict[str, AccessLevel] = field(default_factory=dict)
    
    def can_access_catalog(self, catalog: str) -> bool:
        """Check if user can access a catalog"""
        return catalog in self.accessible_catalogs or "*" in self.accessible_catalogs
    
    def can_access_schema(self, catalog: str, schema: str) -> bool:
        """Check if user can access a schema"""
        full_name = f"{catalog}.{schema}"
        return (
            full_name in self.accessible_schemas or
            f"{catalog}.*" in self.accessible_schemas or
            "*.*" in self.accessible_schemas
        )
    
    def can_access_table(self, catalog: str, schema: str, table: str) -> bool:
        """Check if user can access a table"""
        full_name = f"{catalog}.{schema}.{table}"
        return (
            full_name in self.accessible_tables or
            f"{catalog}.{schema}.*" in self.accessible_tables or
            f"{catalog}.*.*" in self.accessible_tables or
            "*.*.*" in self.accessible_tables
        )
    
    def get_row_filter(self, table: str) -> Optional[str]:
        """Get row-level filter for a table"""
        return self.row_level_filters.get(table)
    
    def get_restricted_columns(self, table: str) -> Set[str]:
        """Get restricted columns for a table"""
        return self.restricted_columns.get(table, set())
    
    def get_access_level(self, resource: str) -> AccessLevel:
        """Get access level for a resource"""
        return self.access_levels.get(resource, AccessLevel.NONE)


@dataclass
class UserSession:
    """
    Complete user session information including identity and access scope
    """
    
    # User identity
    email: str
    user_id: str
    display_name: Optional[str] = None
    
    # Groups and roles
    groups: list[str] = field(default_factory=list)
    roles: list[str] = field(default_factory=list)
    
    # Permissions
    is_admin: bool = False
    is_analyst: bool = False
    is_viewer: bool = False
    
    # Data access scope
    data_scope: DataAccessScope = field(default_factory=DataAccessScope)
    
    # Session metadata
    session_id: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_activity: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    
    # Audit trail
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    
    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = datetime.utcnow()
    
    def is_expired(self) -> bool:
        """Check if session has expired"""
        if not self.expires_at:
            return False
        return datetime.utcnow() > self.expires_at
    
    def has_role(self, role: str) -> bool:
        """Check if user has a specific role"""
        return role in self.roles or self.is_admin
    
    def has_group(self, group: str) -> bool:
        """Check if user belongs to a group"""
        return group in self.groups or self.is_admin
    
    def can_access_admin_panel(self) -> bool:
        """Check if user can access admin panel"""
        return self.is_admin or self.has_role("dashboard_admin")
    
    def can_configure_queries(self) -> bool:
        """Check if user can configure queries"""
        return self.is_admin or self.has_role("query_admin")
    
    def can_configure_filters(self) -> bool:
        """Check if user can configure filters"""
        return self.is_admin or self.has_role("filter_admin")
    
    def can_view_analytics(self) -> bool:
        """Check if user can view analytics"""
        return self.is_admin or self.is_analyst or self.is_viewer
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            "email": self.email,
            "user_id": self.user_id,
            "display_name": self.display_name,
            "groups": self.groups,
            "roles": self.roles,
            "is_admin": self.is_admin,
            "is_analyst": self.is_analyst,
            "is_viewer": self.is_viewer,
            "session_id": self.session_id,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
        }
    
    def to_audit_log(self) -> Dict[str, Any]:
        """Convert to audit log format"""
        return {
            "user_id": self.user_id,
            "email": self.email,
            "session_id": self.session_id,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "timestamp": datetime.utcnow().isoformat(),
        }


def determine_user_roles(groups: list[str]) -> tuple[bool, bool, bool]:
    """
    Determine user roles based on group membership
    
    Returns:
        (is_admin, is_analyst, is_viewer)
    """
    groups_lower = [g.lower() for g in groups]
    
    is_admin = any(
        admin_group in groups_lower
        for admin_group in ["admins", "workspace admins", "account admins", "dashboard_admins"]
    )
    
    is_analyst = any(
        analyst_group in groups_lower
        for analyst_group in ["analysts", "data_analysts", "analytics_team"]
    ) or is_admin
    
    is_viewer = len(groups) > 0 or is_analyst or is_admin
    
    return is_admin, is_analyst, is_viewer


def create_user_session(
    email: str,
    user_id: str,
    groups: list[str],
    display_name: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None
) -> UserSession:
    """
    Factory function to create a user session
    """
    is_admin, is_analyst, is_viewer = determine_user_roles(groups)
    
    # Generate session ID
    import uuid
    session_id = str(uuid.uuid4())
    
    # Determine roles from groups
    roles = []
    if is_admin:
        roles.extend(["dashboard_admin", "query_admin", "filter_admin"])
    if is_analyst:
        roles.append("analyst")
    if is_viewer:
        roles.append("viewer")
    
    return UserSession(
        email=email,
        user_id=user_id,
        display_name=display_name or email.split("@")[0],
        groups=groups,
        roles=roles,
        is_admin=is_admin,
        is_analyst=is_analyst,
        is_viewer=is_viewer,
        session_id=session_id,
        ip_address=ip_address,
        user_agent=user_agent
    )
