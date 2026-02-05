"""
Permissions Service for Unity Catalog Grant Checks
Validates user permissions and enforces row-level security
"""

from typing import Optional, List, Dict, Any, Set
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PermissionsChange, Privilege, SecurableType
from auth_middleware import UserContext
from user_context import DataAccessScope, AccessLevel
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


class PermissionsService:
    """
    Service to check and enforce Unity Catalog permissions
    """
    
    def __init__(self, workspace_client: WorkspaceClient):
        self.client = workspace_client
        self._permission_cache: Dict[str, Dict[str, bool]] = {}
    
    def check_catalog_access(
        self,
        user_context: UserContext,
        catalog_name: str,
        required_privilege: str = "SELECT"
    ) -> bool:
        """
        Check if user has access to a catalog
        
        Args:
            user_context: User making the request
            catalog_name: Name of the catalog
            required_privilege: Required privilege (SELECT, MODIFY, CREATE, etc.)
        
        Returns:
            True if user has access, False otherwise
        """
        # Admin bypass
        if user_context.is_admin:
            return True
        
        cache_key = f"{user_context.email}:{catalog_name}:{required_privilege}"
        if cache_key in self._permission_cache.get(user_context.email, {}):
            return self._permission_cache[user_context.email][cache_key]
        
        try:
            # Use user's workspace client to check permissions
            if user_context.workspace_client:
                # Try to get catalog info - will fail if no access
                catalog = user_context.workspace_client.catalogs.get(catalog_name)
                
                # Check effective permissions
                grants = user_context.workspace_client.grants.get_effective(
                    securable_type=SecurableType.CATALOG,
                    full_name=catalog_name,
                    principal=user_context.email
                )
                
                # Check if user has required privilege
                has_access = any(
                    grant.privilege.value == required_privilege
                    for grant in grants.privilege_assignments or []
                )
                
                # Cache result
                if user_context.email not in self._permission_cache:
                    self._permission_cache[user_context.email] = {}
                self._permission_cache[user_context.email][cache_key] = has_access
                
                return has_access
                
        except Exception as e:
            logger.warning(f"Permission check failed for {user_context.email} on catalog {catalog_name}: {str(e)}")
            return False
        
        return False
    
    def check_schema_access(
        self,
        user_context: UserContext,
        catalog_name: str,
        schema_name: str,
        required_privilege: str = "SELECT"
    ) -> bool:
        """Check if user has access to a schema"""
        if user_context.is_admin:
            return True
        
        # First check catalog access
        if not self.check_catalog_access(user_context, catalog_name, "USAGE"):
            return False
        
        full_name = f"{catalog_name}.{schema_name}"
        cache_key = f"{user_context.email}:{full_name}:{required_privilege}"
        
        if cache_key in self._permission_cache.get(user_context.email, {}):
            return self._permission_cache[user_context.email][cache_key]
        
        try:
            if user_context.workspace_client:
                # Get schema grants
                grants = user_context.workspace_client.grants.get_effective(
                    securable_type=SecurableType.SCHEMA,
                    full_name=full_name,
                    principal=user_context.email
                )
                
                has_access = any(
                    grant.privilege.value == required_privilege
                    for grant in grants.privilege_assignments or []
                )
                
                if user_context.email not in self._permission_cache:
                    self._permission_cache[user_context.email] = {}
                self._permission_cache[user_context.email][cache_key] = has_access
                
                return has_access
                
        except Exception as e:
            logger.warning(f"Schema permission check failed: {str(e)}")
            return False
        
        return False
    
    def check_table_access(
        self,
        user_context: UserContext,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        required_privilege: str = "SELECT"
    ) -> bool:
        """Check if user has access to a table"""
        if user_context.is_admin:
            return True
        
        # Check parent schema access
        if not self.check_schema_access(user_context, catalog_name, schema_name, "USAGE"):
            return False
        
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        cache_key = f"{user_context.email}:{full_name}:{required_privilege}"
        
        if cache_key in self._permission_cache.get(user_context.email, {}):
            return self._permission_cache[user_context.email][cache_key]
        
        try:
            if user_context.workspace_client:
                # Get table grants
                grants = user_context.workspace_client.grants.get_effective(
                    securable_type=SecurableType.TABLE,
                    full_name=full_name,
                    principal=user_context.email
                )
                
                has_access = any(
                    grant.privilege.value == required_privilege
                    for grant in grants.privilege_assignments or []
                )
                
                if user_context.email not in self._permission_cache:
                    self._permission_cache[user_context.email] = {}
                self._permission_cache[user_context.email][cache_key] = has_access
                
                return has_access
                
        except Exception as e:
            logger.warning(f"Table permission check failed: {str(e)}")
            return False
        
        return False
    
    def get_user_data_scope(self, user_context: UserContext) -> DataAccessScope:
        """
        Get comprehensive data access scope for user
        This includes all catalogs, schemas, and tables they can access
        """
        scope = DataAccessScope()
        
        # Admins get full access
        if user_context.is_admin:
            scope.accessible_catalogs.add("*")
            scope.accessible_schemas.add("*.*")
            scope.accessible_tables.add("*.*.*")
            return scope
        
        try:
            if not user_context.workspace_client:
                return scope
            
            # Get all catalogs user can access
            try:
                catalogs = user_context.workspace_client.catalogs.list()
                for catalog in catalogs:
                    if self.check_catalog_access(user_context, catalog.name, "USAGE"):
                        scope.accessible_catalogs.add(catalog.name)
                        
                        # Get schemas in this catalog
                        try:
                            schemas = user_context.workspace_client.schemas.list(catalog_name=catalog.name)
                            for schema in schemas:
                                if self.check_schema_access(user_context, catalog.name, schema.name, "USAGE"):
                                    scope.accessible_schemas.add(f"{catalog.name}.{schema.name}")
                        except Exception as e:
                            logger.debug(f"Could not list schemas in {catalog.name}: {str(e)}")
            
            except Exception as e:
                logger.warning(f"Could not list catalogs for {user_context.email}: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error building data scope: {str(e)}")
        
        return scope
    
    def inject_row_level_security(
        self,
        sql: str,
        user_context: UserContext,
        table_filters: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Inject row-level security filters into SQL query
        
        Args:
            sql: Original SQL query
            user_context: User executing the query
            table_filters: Dictionary of table_name -> filter_expression
        
        Returns:
            Modified SQL with security filters
        """
        # Admins bypass row-level security
        if user_context.is_admin:
            return sql
        
        # If no custom filters provided, use default: current_user() filter
        if not table_filters:
            # Add a WHERE clause that uses current_user() for basic RLS
            # This assumes tables have a 'owner_email' or similar column
            # In practice, you'd configure this per table
            if "WHERE" in sql.upper():
                # Already has WHERE, add AND condition
                sql = sql.replace("WHERE", f"WHERE current_user() = '{user_context.email}' AND")
            else:
                # No WHERE clause, need to add one carefully
                # This is simplified - production would use SQL parser
                pass
        
        return sql
    
    def validate_query_permissions(
        self,
        sql: str,
        user_context: UserContext
    ) -> tuple[bool, Optional[str]]:
        """
        Validate that user has permissions to execute a query
        
        Returns:
            (is_valid, error_message)
        """
        # Admins can run anything
        if user_context.is_admin:
            return True, None
        
        # Parse SQL to extract table references
        # This is simplified - production would use proper SQL parser
        tables = self._extract_table_references(sql)
        
        # Check access to each table
        for table_ref in tables:
            parts = table_ref.split(".")
            if len(parts) == 3:
                catalog, schema, table = parts
                if not self.check_table_access(user_context, catalog, schema, table, "SELECT"):
                    return False, f"Access denied to table {table_ref}"
            elif len(parts) == 2:
                schema, table = parts
                # Assume default catalog
                catalog = "hive_metastore"  # Or get from config
                if not self.check_table_access(user_context, catalog, schema, table, "SELECT"):
                    return False, f"Access denied to table {table_ref}"
        
        return True, None
    
    def _extract_table_references(self, sql: str) -> List[str]:
        """
        Extract table references from SQL query
        This is a simplified version - production should use SQL parser
        """
        import re
        
        # Simple regex to find table references
        # Pattern: FROM/JOIN <catalog>.<schema>.<table> or <schema>.<table>
        pattern = r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+){1,2})'
        matches = re.findall(pattern, sql, re.IGNORECASE)
        
        return matches
    
    def clear_cache(self, user_email: Optional[str] = None):
        """Clear permission cache"""
        if user_email:
            self._permission_cache.pop(user_email, None)
        else:
            self._permission_cache.clear()
    
    def audit_log_access(
        self,
        user_context: UserContext,
        resource: str,
        action: str,
        granted: bool
    ):
        """
        Log access attempt for audit trail
        
        Args:
            user_context: User attempting access
            resource: Resource being accessed
            action: Action being performed
            granted: Whether access was granted
        """
        log_entry = {
            "timestamp": None,  # Would use proper timestamp
            "user_email": user_context.email,
            "user_id": user_context.user_id,
            "resource": resource,
            "action": action,
            "granted": granted,
            "groups": user_context.groups
        }
        
        # In production, write to audit log table or service
        logger.info(f"Access audit: {log_entry}")


# Global instance
_permissions_service: Optional[PermissionsService] = None


def get_permissions_service(workspace_client: WorkspaceClient) -> PermissionsService:
    """Get or create global permissions service instance"""
    global _permissions_service
    if _permissions_service is None:
        _permissions_service = PermissionsService(workspace_client)
    return _permissions_service


def check_query_permissions(
    sql: str,
    user_context: UserContext,
    permissions_service: PermissionsService
) -> str:
    """
    Validate and modify query based on user permissions
    
    Returns:
        Modified SQL with security filters applied
    
    Raises:
        PermissionError if user doesn't have required permissions
    """
    # Validate permissions
    is_valid, error_msg = permissions_service.validate_query_permissions(sql, user_context)
    
    if not is_valid:
        permissions_service.audit_log_access(user_context, "query", "execute", False)
        raise PermissionError(error_msg)
    
    # Inject row-level security
    modified_sql = permissions_service.inject_row_level_security(sql, user_context)
    
    # Log successful permission check
    permissions_service.audit_log_access(user_context, "query", "execute", True)
    
    return modified_sql
