"""
Configuration Manager for Health Insurance Dashboard
Reads data_config.yaml and provides mapped queries
"""

import yaml
import os
from typing import Dict, List, Optional, Any


class DataConfig:
    """Manages data configuration and field mappings"""
    
    def __init__(self, config_path: str = "data_config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            print(f"[Config] Loaded configuration from {self.config_path}")
            return config
        except FileNotFoundError:
            raise Exception(
                f"Configuration file not found: {self.config_path}\n"
                "Please create data_config.yaml with your data mappings."
            )
        except yaml.YAMLError as e:
            raise Exception(f"Invalid YAML in configuration file: {e}")
    
    def _validate_config(self):
        """Validate required configuration sections exist"""
        required_sections = ['connection', 'views', 'field_mappings']
        for section in required_sections:
            if section not in self.config:
                raise Exception(
                    f"Missing required section '{section}' in {self.config_path}"
                )
        print("[Config] Configuration validated successfully")
    
    # Connection Properties
    @property
    def catalog(self) -> str:
        return self.config['connection']['catalog']
    
    @property
    def schema(self) -> str:
        return self.config['connection']['schema']
    
    @property
    def warehouse_id(self) -> str:
        return self.config['connection'].get('warehouse_id', os.getenv('SQL_WAREHOUSE_ID'))
    
    # View Name Getters
    def get_view_name(self, view_key: str) -> str:
        """Get the actual view/table name for a logical view key"""
        view_config = self.config['views'].get(view_key)
        if not view_config:
            raise Exception(f"View '{view_key}' not configured in data_config.yaml")
        return view_config['source']
    
    def get_full_table_name(self, view_key: str) -> str:
        """Get fully qualified table name: catalog.schema.table"""
        view_name = self.get_view_name(view_key)
        return f"{self.catalog}.{self.schema}.{view_name}"
    
    # Field Mapping
    def get_field_mapping(self, view_key: str) -> Dict[str, str]:
        """Get field mappings for a specific view"""
        mappings = self.config['field_mappings'].get(view_key, {})
        if not mappings:
            print(f"[Warning] No field mappings found for '{view_key}', using default field names")
        return mappings
    
    def map_field(self, view_key: str, dashboard_field: str) -> str:
        """Map a dashboard field name to the actual column name"""
        mapping = self.get_field_mapping(view_key)
        return mapping.get(dashboard_field, dashboard_field)
    
    def build_select_clause(self, view_key: str, fields: List[str]) -> str:
        """
        Build a SELECT clause with field mappings
        
        Args:
            view_key: The logical view name (e.g., 'membership_kpis')
            fields: List of dashboard field names to select
        
        Returns:
            SQL SELECT clause with mapped field names
        """
        mapping = self.get_field_mapping(view_key)
        select_parts = []
        
        for field in fields:
            actual_field = mapping.get(field, field)
            if actual_field != field:
                # Field name is different, use alias
                select_parts.append(f"{actual_field} as {field}")
            else:
                # Field name matches, no alias needed
                select_parts.append(field)
        
        return ", ".join(select_parts)
    
    def build_query(self, view_key: str, fields: List[str], 
                   where: Optional[str] = None, 
                   order_by: Optional[str] = None,
                   limit: Optional[int] = None) -> str:
        """
        Build a complete SQL query with field mappings
        
        Args:
            view_key: The logical view name
            fields: List of dashboard field names to select
            where: Optional WHERE clause (use dashboard field names)
            order_by: Optional ORDER BY clause (use dashboard field names)
            limit: Optional LIMIT value
        
        Returns:
            Complete SQL query string
        """
        table_name = self.get_full_table_name(view_key)
        select_clause = self.build_select_clause(view_key, fields)
        
        query = f"SELECT {select_clause} FROM {table_name}"
        
        if where:
            # Map field names in WHERE clause
            mapped_where = self._map_fields_in_clause(view_key, where)
            query += f" WHERE {mapped_where}"
        
        if order_by:
            # Map field names in ORDER BY clause
            mapped_order = self._map_fields_in_clause(view_key, order_by)
            query += f" ORDER BY {mapped_order}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        return query
    
    def _map_fields_in_clause(self, view_key: str, clause: str) -> str:
        """Helper to map dashboard field names in WHERE/ORDER BY clauses"""
        mapping = self.get_field_mapping(view_key)
        mapped_clause = clause
        
        # Replace each dashboard field with actual field name
        for dashboard_field, actual_field in mapping.items():
            if dashboard_field != actual_field:
                # Only replace if field names are different
                mapped_clause = mapped_clause.replace(dashboard_field, actual_field)
        
        return mapped_clause
    
    def get_custom_query(self, view_key: str) -> Optional[str]:
        """Get a custom SQL query if defined"""
        custom_queries = self.config.get('custom_queries', {})
        if not custom_queries.get('enabled', False):
            return None
        
        query = custom_queries.get(view_key)
        if query:
            # Replace placeholders
            mapping = self.get_field_mapping(view_key)
            query = query.replace('{catalog}', self.catalog)
            query = query.replace('{schema}', self.schema)
            
            # Replace field placeholders
            for dashboard_field, actual_field in mapping.items():
                query = query.replace(f'{{{dashboard_field}}}', actual_field)
        
        return query
    
    def get_all_views(self) -> List[str]:
        """Get list of all configured views"""
        return list(self.config['views'].keys())
    
    def get_filter_config(self) -> Dict:
        """Get filter configuration"""
        return self.config.get('filters', {'enabled': False})
    
    def get_enabled_filters(self) -> List[str]:
        """Get list of enabled filter names"""
        filters = self.get_filter_config()
        if not filters.get('enabled', False):
            return []
        
        enabled = []
        for filter_name, filter_config in filters.items():
            if filter_name != 'enabled' and isinstance(filter_config, dict):
                if filter_config.get('enabled', False):
                    enabled.append(filter_name)
        return enabled
    
    def get_filter_details(self, filter_name: str) -> Optional[Dict]:
        """Get configuration for a specific filter"""
        filters = self.get_filter_config()
        return filters.get(filter_name)
    
    def print_config_summary(self):
        """Print a summary of the loaded configuration"""
        print("\n" + "="*60)
        print("DATA CONFIGURATION SUMMARY")
        print("="*60)
        print(f"Catalog: {self.catalog}")
        print(f"Schema: {self.schema}")
        print(f"Warehouse ID: {self.warehouse_id}")
        print(f"\nConfigured Views ({len(self.get_all_views())}):")
        for view_key in self.get_all_views():
            view_name = self.get_view_name(view_key)
            print(f"  - {view_key}: {self.catalog}.{self.schema}.{view_name}")
        
        enabled_filters = self.get_enabled_filters()
        if enabled_filters:
            print(f"\nEnabled Filters ({len(enabled_filters)}):")
            for filter_name in enabled_filters:
                filter_config = self.get_filter_details(filter_name)
                print(f"  - {filter_name}: {filter_config.get('label', filter_name)} (source: {filter_config.get('source', 'unknown')})")
        
        print("="*60 + "\n")


# Global config instance
_config_instance = None

def get_config() -> DataConfig:
    """Get or create the global configuration instance"""
    global _config_instance
    if _config_instance is None:
        _config_instance = DataConfig()
        _config_instance.print_config_summary()
    return _config_instance

