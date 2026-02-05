"""
Configuration Service - Loads configuration from Unity Catalog tables
Replaces the YAML-based config_manager.py
"""

from typing import Dict, List, Optional, Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


@dataclass
class QueryConfig:
    """Represents a configured dashboard query"""
    id: str
    name: str
    description: Optional[str]
    category: str
    sql_template: str
    parameters: List[Dict[str, Any]]
    output_schema: List[Dict[str, Any]]
    required_permissions: List[str]
    allow_drill_down: bool
    drill_down_query_id: Optional[str]
    cache_ttl_seconds: int
    tags: List[str]
    is_active: bool


@dataclass
class FilterConfig:
    """Represents a filter configuration"""
    id: str
    filter_name: str
    label: str
    filter_type: str  # 'global' or 'local'
    data_type: str  # 'select', 'multiselect', 'daterange', 'text'
    data_source: str  # 'static', 'dynamic', 'query'
    static_options: List[Dict[str, str]]
    options_query: Optional[str]
    default_value: Optional[str]
    applies_to_tabs: List[str]
    applies_to_queries: List[str]
    filter_expression_template: Optional[str]
    display_order: int
    is_required: bool
    is_active: bool


@dataclass
class VisualizationConfig:
    """Represents a visualization configuration"""
    id: str
    viz_name: str
    viz_type: str
    query_id: str
    data_key: str
    x_axis_field: Optional[str]
    y_axis_field: Optional[str]
    color_scheme: List[str]
    title: Optional[str]
    subtitle: Optional[str]
    allow_drill_down: bool
    drill_down_config: Optional[Dict[str, Any]]
    chart_options: Dict[str, str]
    default_for_tab: Optional[str]
    display_order: int
    is_active: bool


class ConfigService:
    """
    Service to load and manage configuration from Unity Catalog tables
    """
    
    def __init__(
        self,
        workspace_client: WorkspaceClient,
        config_catalog: str = "mv_catalog",
        config_schema: str = "config_schema",
        warehouse_id: str = None
    ):
        self.client = workspace_client
        self.config_catalog = config_catalog
        self.config_schema = config_schema
        self.warehouse_id = warehouse_id
        self._cache: Dict[str, tuple[Any, datetime]] = {}
        self._cache_ttl = timedelta(minutes=5)
    
    def _get_full_table_name(self, table: str) -> str:
        """Get fully qualified table name"""
        return f"{self.config_catalog}.{self.config_schema}.{table}"
    
    def _execute_query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results"""
        try:
            response = self.client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=sql,
                catalog=self.config_catalog,
                schema=self.config_schema,
                wait_timeout="30s"
            )
            
            if response.status.state != StatementState.SUCCEEDED:
                raise Exception(f"Query failed: {response.status.state}")
            
            if not response.result or not response.result.data_array:
                return []
            
            columns = [col.name for col in response.manifest.schema.columns]
            
            results = []
            for row in response.result.data_array:
                row_dict = {}
                for i, value in enumerate(row):
                    if i < len(columns):
                        row_dict[columns[i]] = value
                results.append(row_dict)
            
            return results
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def _get_cached(self, key: str) -> Optional[Any]:
        """Get cached value if still valid"""
        if key in self._cache:
            value, cached_at = self._cache[key]
            if datetime.utcnow() - cached_at < self._cache_ttl:
                return value
            else:
                del self._cache[key]
        return None
    
    def _set_cached(self, key: str, value: Any):
        """Cache a value"""
        self._cache[key] = (value, datetime.utcnow())
    
    def get_query_config(self, query_id: str) -> Optional[QueryConfig]:
        """Get configuration for a specific query"""
        cache_key = f"query_{query_id}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        sql = f"""
            SELECT *
            FROM {self._get_full_table_name('dashboard_queries')}
            WHERE id = '{query_id}' AND is_active = true
        """
        
        results = self._execute_query(sql)
        if not results:
            return None
        
        row = results[0]
        config = QueryConfig(
            id=row['id'],
            name=row['name'],
            description=row.get('description'),
            category=row['category'],
            sql_template=row['sql_template'],
            parameters=row.get('parameters', []),
            output_schema=row.get('output_schema', []),
            required_permissions=row.get('required_permissions', []),
            allow_drill_down=row.get('allow_drill_down', False),
            drill_down_query_id=row.get('drill_down_query_id'),
            cache_ttl_seconds=row.get('cache_ttl_seconds', 300),
            tags=row.get('tags', []),
            is_active=row.get('is_active', True)
        )
        
        self._set_cached(cache_key, config)
        return config
    
    def get_all_queries(self, category: Optional[str] = None) -> List[QueryConfig]:
        """Get all active query configurations"""
        cache_key = f"queries_{category or 'all'}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        sql = f"""
            SELECT *
            FROM {self._get_full_table_name('dashboard_queries')}
            WHERE is_active = true
        """
        
        if category:
            sql += f" AND category = '{category}'"
        
        sql += " ORDER BY name"
        
        results = self._execute_query(sql)
        configs = [
            QueryConfig(
                id=row['id'],
                name=row['name'],
                description=row.get('description'),
                category=row['category'],
                sql_template=row['sql_template'],
                parameters=row.get('parameters', []),
                output_schema=row.get('output_schema', []),
                required_permissions=row.get('required_permissions', []),
                allow_drill_down=row.get('allow_drill_down', False),
                drill_down_query_id=row.get('drill_down_query_id'),
                cache_ttl_seconds=row.get('cache_ttl_seconds', 300),
                tags=row.get('tags', []),
                is_active=row.get('is_active', True)
            )
            for row in results
        ]
        
        self._set_cached(cache_key, configs)
        return configs
    
    def get_filter_configs(
        self,
        filter_type: Optional[str] = None,
        tab: Optional[str] = None
    ) -> List[FilterConfig]:
        """Get filter configurations"""
        cache_key = f"filters_{filter_type or 'all'}_{tab or 'all'}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        sql = f"""
            SELECT *
            FROM {self._get_full_table_name('filter_definitions')}
            WHERE is_active = true
        """
        
        if filter_type:
            sql += f" AND filter_type = '{filter_type}'"
        
        sql += " ORDER BY display_order, filter_name"
        
        results = self._execute_query(sql)
        
        configs = []
        for row in results:
            # Filter by tab if specified
            if tab and tab not in row.get('applies_to_tabs', []):
                continue
            
            configs.append(FilterConfig(
                id=row['id'],
                filter_name=row['filter_name'],
                label=row['label'],
                filter_type=row['filter_type'],
                data_type=row['data_type'],
                data_source=row['data_source'],
                static_options=row.get('static_options', []),
                options_query=row.get('options_query'),
                default_value=row.get('default_value'),
                applies_to_tabs=row.get('applies_to_tabs', []),
                applies_to_queries=row.get('applies_to_queries', []),
                filter_expression_template=row.get('filter_expression_template'),
                display_order=row.get('display_order', 0),
                is_required=row.get('is_required', False),
                is_active=row.get('is_active', True)
            ))
        
        self._set_cached(cache_key, configs)
        return configs
    
    def get_viz_configs(self, tab: Optional[str] = None) -> List[VisualizationConfig]:
        """Get visualization configurations"""
        cache_key = f"viz_{tab or 'all'}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        sql = f"""
            SELECT *
            FROM {self._get_full_table_name('visualization_configs')}
            WHERE is_active = true
        """
        
        if tab:
            sql += f" AND default_for_tab = '{tab}'"
        
        sql += " ORDER BY display_order, viz_name"
        
        results = self._execute_query(sql)
        configs = [
            VisualizationConfig(
                id=row['id'],
                viz_name=row['viz_name'],
                viz_type=row['viz_type'],
                query_id=row['query_id'],
                data_key=row['data_key'],
                x_axis_field=row.get('x_axis_field'),
                y_axis_field=row.get('y_axis_field'),
                color_scheme=row.get('color_scheme', []),
                title=row.get('title'),
                subtitle=row.get('subtitle'),
                allow_drill_down=row.get('allow_drill_down', False),
                drill_down_config=row.get('drill_down_config'),
                chart_options=row.get('chart_options', {}),
                default_for_tab=row.get('default_for_tab'),
                display_order=row.get('display_order', 0),
                is_active=row.get('is_active', True)
            )
            for row in results
        ]
        
        self._set_cached(cache_key, configs)
        return configs
    
    def get_system_config(self, key: str, default: Any = None) -> Any:
        """Get system configuration value"""
        cache_key = f"system_config_{key}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        sql = f"""
            SELECT config_value, config_type
            FROM {self._get_full_table_name('system_config')}
            WHERE config_key = '{key}'
        """
        
        results = self._execute_query(sql)
        if not results:
            return default
        
        row = results[0]
        value = row['config_value']
        config_type = row['config_type']
        
        # Convert value to appropriate type
        if config_type == 'int':
            value = int(value)
        elif config_type == 'boolean':
            value = value.lower() == 'true'
        elif config_type == 'json':
            value = json.loads(value)
        
        self._set_cached(cache_key, value)
        return value
    
    def build_query_from_template(
        self,
        query_id: str,
        params: Dict[str, Any]
    ) -> str:
        """
        Build SQL query from template with parameters
        
        Args:
            query_id: Query configuration ID
            params: Dictionary of parameter values
        
        Returns:
            SQL query with parameters substituted
        """
        config = self.get_query_config(query_id)
        if not config:
            raise ValueError(f"Query configuration not found: {query_id}")
        
        sql = config.sql_template
        
        # Substitute parameters using ${param_name} syntax
        for param_def in config.parameters:
            param_name = param_def['name']
            param_value = params.get(param_name)
            
            # Use default if not provided
            if param_value is None:
                if param_def.get('required', False):
                    raise ValueError(f"Required parameter missing: {param_name}")
                param_value = param_def.get('default_value')
            
            # Substitute in SQL
            if param_value is not None:
                sql = sql.replace(f"${{{param_name}}}", str(param_value))
        
        return sql
    
    def clear_cache(self):
        """Clear configuration cache"""
        self._cache.clear()
    
    def validate_config_tables_exist(self) -> bool:
        """Validate that all configuration tables exist"""
        required_tables = [
            'dashboard_queries',
            'filter_definitions',
            'visualization_configs',
            'dashboard_tabs',
            'system_config'
        ]
        
        try:
            for table in required_tables:
                sql = f"SELECT COUNT(*) as cnt FROM {self._get_full_table_name(table)} LIMIT 1"
                self._execute_query(sql)
            
            logger.info("All configuration tables exist and are accessible")
            return True
            
        except Exception as e:
            logger.error(f"Configuration tables validation failed: {str(e)}")
            return False


# Global config service instance
_config_service: Optional[ConfigService] = None


def get_config_service(
    workspace_client: WorkspaceClient,
    warehouse_id: str,
    config_catalog: str = "mv_catalog",
    config_schema: str = "config_schema"
) -> ConfigService:
    """Get or create global configuration service instance"""
    global _config_service
    if _config_service is None:
        _config_service = ConfigService(
            workspace_client,
            config_catalog,
            config_schema,
            warehouse_id
        )
        # Validate tables exist
        if not _config_service.validate_config_tables_exist():
            logger.warning("Configuration tables may not be properly set up")
    
    return _config_service
