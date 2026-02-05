"""
FastAPI Backend for Health Insurance Membership Dashboard
Enterprise-grade analytics platform with on-behalf-of auth, Genie, AI reports, and more
Based on: https://www.databricks.com/blog/building-databricks-apps-react-and-mosaic-ai-agents-enterprise-chat-solutions
"""

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, AsyncGenerator
import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# Setup logging FIRST before other imports
from logging_config import setup_logging, get_logger, LogTimer
setup_logging()  # Auto-detects environment and configures logging

# Import new modules
from auth_middleware import get_user_context, get_admin_user, UserContext
from permissions_service import get_permissions_service, check_query_permissions
from config_service import get_config_service
from genie_integration import get_genie_service
from ai_reports import get_report_generator

# Create component-specific loggers
logger = get_logger('app')
api_logger = get_logger('api')
query_logger = get_logger('query')

# Try to import old config manager for backward compatibility
try:
    from config_manager import get_config as get_old_config
    old_config_available = True
except:
    old_config_available = False

# Initialize FastAPI apps
api_app = FastAPI(title="Health Dashboard API")
app = FastAPI(title="Health Insurance Dashboard")

# Add request logging middleware
@api_app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all API requests with timing"""
    start_time = time.time()
    
    # Log incoming request
    api_logger.debug(f"Incoming request: {request.method} {request.url.path}")
    
    # Process request
    response = await call_next(request)
    
    # Calculate duration
    duration_ms = (time.time() - start_time) * 1000
    
    # Get user email if available (from auth header)
    user_email = request.headers.get('X-User-Email', None)
    
    # Log API call with metrics
    api_logger.api_call(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code,
        duration_ms=duration_ms,
        user_email=user_email
    )
    
    return response

# Configuration
CATALOG_NAME = os.getenv("CATALOG_NAME", "mv_catalog")
SCHEMA_NAME = os.getenv("SCHEMA_NAME", "demo_health")
SQL_WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID", "")
CONFIG_CATALOG = os.getenv("CONFIG_CATALOG", "mv_catalog")
CONFIG_SCHEMA = os.getenv("CONFIG_SCHEMA", "config_schema")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
AI_MODEL_ENDPOINT = os.getenv("AI_MODEL_ENDPOINT", "databricks-dbrx-instruct")

# Load old YAML config if available (for backward compatibility during migration)
data_config = None
if old_config_available:
    try:
        data_config = get_old_config()
        logger.info("Loaded legacy YAML config for backward compatibility")
    except Exception as e:
        logger.debug(f"No legacy YAML config: {e}")

# Get other environment variables
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME", "")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "") or f"https://{DATABRICKS_SERVER_HOSTNAME}"
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")

# Log configuration
logger.info(f"Application configuration loaded", extra={
    'context': {
        'host': DATABRICKS_HOST,
        'warehouse_id': SQL_WAREHOUSE_ID[:8] + '...' if SQL_WAREHOUSE_ID else 'not set',
        'catalog': f"{CATALOG_NAME}.{SCHEMA_NAME}",
        'genie_enabled': bool(GENIE_SPACE_ID),
        'ai_model': AI_MODEL_ENDPOINT
    }
})


# Pydantic models for API requests/responses
class QueryRequest(BaseModel):
    sql: str
    warehouse_id: Optional[str] = None


class QueryResponse(BaseModel):
    columns: List[str]
    data: List[Dict[str, Any]]
    row_count: int


# Dependency to get Databricks client
def get_databricks_client() -> WorkspaceClient:
    """Create and return a Databricks workspace client"""
    # When running in Databricks Apps, the SDK auto-detects:
    # - DATABRICKS_HOST (or DATABRICKS_SERVER_HOSTNAME)
    # - DATABRICKS_CLIENT_ID
    # - DATABRICKS_CLIENT_SECRET
    # These are automatically provided by Databricks Apps runtime
    
    # Ensure host has proper format
    host = os.getenv("DATABRICKS_HOST")
    if host and not host.startswith("https://"):
        host = f"https://{host}"
    
    return WorkspaceClient(host=host)


@api_app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "backend": "FastAPI + Uvicorn",
        "version": "2.0.0",
        "config_mode": "data_config.yaml" if data_config else "environment variables",
        "databricks": {
            "host": DATABRICKS_HOST or DATABRICKS_SERVER_HOSTNAME,
            "warehouse_id": SQL_WAREHOUSE_ID,
            "catalog": CATALOG_NAME,
            "schema": SCHEMA_NAME,
        },
        "timestamp": str(os.popen('date').read().strip())
    }


@api_app.get("/config/summary")
async def get_config_summary():
    """Get current configuration summary"""
    if not data_config:
        return {
            "status": "warning",
            "message": "Using environment variables only. Create data_config.yaml for advanced configuration.",
            "catalog": CATALOG_NAME,
            "schema": SCHEMA_NAME,
            "warehouse_id": SQL_WAREHOUSE_ID
        }
    
    return {
        "status": "success",
        "config_source": "data_config.yaml",
        "connection": {
            "catalog": data_config.catalog,
            "schema": data_config.schema,
            "warehouse_id": data_config.warehouse_id
        },
        "views_configured": data_config.get_all_views(),
        "filters_enabled": data_config.get_enabled_filters(),
        "example_mappings": {
            "membership_kpis": data_config.get_field_mapping('membership_kpis')
        }
    }


@api_app.get("/filters/options")
async def get_filter_options(client: WorkspaceClient = Depends(get_databricks_client)):
    """Get filter options from configuration"""
    logger.debug("Loading filter options")
    
    if not data_config:
        logger.info("Filter options disabled - no data config")
        return {"enabled": False}
    
    filter_config = data_config.get_filter_config()
    if not filter_config.get('enabled', False):
        logger.info("Filter options disabled in config")
        return {"enabled": False}
    
    options = {"enabled": True, "filters": {}}
    enabled_filters = data_config.get_enabled_filters()
    logger.info(f"Loading {len(enabled_filters)} filter options", extra={'context': {'filters': enabled_filters}})
    
    for filter_name in enabled_filters:
        filter_details = data_config.get_filter_details(filter_name)
        
        if filter_details.get('source') == 'static':
            # Static values from config
            options['filters'][filter_name] = {
                "label": filter_details.get('label', filter_name),
                "values": filter_details.get('static_values', []),
                "default": filter_details.get('default', 'all')
            }
            logger.debug(f"Loaded static filter: {filter_name}")
        elif filter_details.get('source') == 'dynamic':
            # Query database for distinct values
            try:
                view_key = filter_details.get('query_view')
                field = filter_details.get('query_field')
                mapped_field = data_config.map_field(view_key, field)
                
                sql = f"""
                    SELECT DISTINCT {mapped_field} as {field}
                    FROM {data_config.get_full_table_name(view_key)}
                    WHERE {mapped_field} IS NOT NULL
                    ORDER BY {mapped_field}
                """
                
                with LogTimer(query_logger, f"Dynamic filter query: {filter_name}", filter=filter_name, field=mapped_field):
                    response = client.statement_execution.execute_statement(
                        warehouse_id=SQL_WAREHOUSE_ID,
                        statement=sql,
                        catalog=CATALOG_NAME,
                        schema=SCHEMA_NAME,
                        wait_timeout="30s"
                    )
                
                if response.status.state == StatementState.SUCCEEDED:
                    values = []
                    if response.result and response.result.data_array:
                        values = [row[0] for row in response.result.data_array if row[0]]
                    
                    options['filters'][filter_name] = {
                        "label": filter_details.get('label', filter_name),
                        "values": values,
                        "default": filter_details.get('default', 'all')
                    }
                    logger.info(f"Loaded dynamic filter: {filter_name} ({len(values)} values)")
                else:
                    logger.warning(f"Dynamic filter query failed: {filter_name}", extra={'context': {'state': response.status.state}})
                    options['filters'][filter_name] = {
                        "label": filter_details.get('label', filter_name),
                        "values": [],
                        "default": filter_details.get('default', 'all')
                    }
            except Exception as e:
                logger.error(f"Failed to load dynamic filter: {filter_name}", exc_info=True, extra={'context': {'filter': filter_name}})
                options['filters'][filter_name] = {
                    "label": filter_details.get('label', filter_name),
                    "values": [],
                    "default": filter_details.get('default', 'all')
                }
    
    return options


@api_app.get("/debug/config")
async def debug_config():
    """Debug endpoint to show configuration"""
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "")
    
    return {
        "env_vars": {
            "DATABRICKS_HOST": DATABRICKS_HOST or "(not set)",
            "DATABRICKS_SERVER_HOSTNAME": DATABRICKS_SERVER_HOSTNAME or "(not set)",
            "DATABRICKS_HTTP_PATH": DATABRICKS_HTTP_PATH or "(not set)",
            "SQL_WAREHOUSE_ID": SQL_WAREHOUSE_ID or "(not set)",
            "DATABRICKS_TOKEN": "***" + DATABRICKS_TOKEN[-4:] if DATABRICKS_TOKEN else "(not set)",
            "DATABRICKS_CLIENT_ID": client_id[:8] + "..." if client_id else "(not set)",
            "DATABRICKS_CLIENT_SECRET": "***" + client_secret[-4:] if client_secret else "(not set)",
            "CATALOG_NAME": CATALOG_NAME,
            "SCHEMA_NAME": SCHEMA_NAME,
        },
        "computed": {
            "full_host": f"https://{DATABRICKS_HOST}" if DATABRICKS_HOST and not DATABRICKS_HOST.startswith("https://") else DATABRICKS_HOST,
            "auth_method": "OAuth M2M" if client_id else "Token (if set)" if DATABRICKS_TOKEN else "None detected"
        }
    }


@api_app.get("/debug/test-connection")
async def test_connection(client: WorkspaceClient = Depends(get_databricks_client)):
    """Test Databricks connection"""
    try:
        # Try to list warehouses to verify connection
        warehouses = list(client.warehouses.list())
        return {
            "status": "success",
            "message": "Connected to Databricks successfully",
            "warehouses_found": len(warehouses),
            "current_warehouse": SQL_WAREHOUSE_ID
        }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__,
            "traceback": traceback.format_exc()
        }


@api_app.get("/debug/test-query")
async def test_query(client: WorkspaceClient = Depends(get_databricks_client)):
    """Test a simple query"""
    try:
        sql = f"SELECT 1 as test_column"
        
        response = client.statement_execution.execute_statement(
            warehouse_id=SQL_WAREHOUSE_ID,
            statement=sql,
            catalog=CATALOG_NAME,
            schema=SCHEMA_NAME,
            wait_timeout="30s"
        )
        
        return {
            "status": "success",
            "query": sql,
            "state": str(response.status.state),
            "result": "Query executed successfully"
        }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__,
            "traceback": traceback.format_exc()
        }


@api_app.get("/debug/test-view")
async def test_view(client: WorkspaceClient = Depends(get_databricks_client)):
    """Test querying an actual view"""
    try:
        sql = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.v_membership_kpis LIMIT 1"
        
        response = client.statement_execution.execute_statement(
            warehouse_id=SQL_WAREHOUSE_ID,
            statement=sql,
            catalog=CATALOG_NAME,
            schema=SCHEMA_NAME,
            wait_timeout="30s"
        )
        
        # Check if query succeeded
        if response.status.state != StatementState.SUCCEEDED:
            error_message = "Unknown error"
            if response.status.error:
                error_message = f"{response.status.error.error_code}: {response.status.error.message}"
            
            return {
                "status": "error",
                "query": sql,
                "state": str(response.status.state),
                "error_details": error_message,
                "statement_id": response.statement_id
            }
        
        return {
            "status": "success",
            "query": sql,
            "state": str(response.status.state),
            "row_count": len(response.result.data_array) if response.result and response.result.data_array else 0,
            "columns": [col.name for col in response.manifest.schema.columns] if response.manifest else [],
            "sample_row": response.result.data_array[0] if response.result and response.result.data_array else None
        }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__,
            "query": sql,
            "traceback": traceback.format_exc()
        }


@api_app.get("/debug/list-tables")
async def list_tables(client: WorkspaceClient = Depends(get_databricks_client)):
    """List tables/views in the schema to verify they exist"""
    try:
        # Try to list tables in the schema
        sql = f"SHOW TABLES IN {CATALOG_NAME}.{SCHEMA_NAME}"
        
        response = client.statement_execution.execute_statement(
            warehouse_id=SQL_WAREHOUSE_ID,
            statement=sql,
            catalog=CATALOG_NAME,
            schema=SCHEMA_NAME,
            wait_timeout="30s"
        )
        
        if response.status.state != StatementState.SUCCEEDED:
            return {
                "status": "error",
                "message": "Failed to list tables",
                "state": str(response.status.state)
            }
        
        # Extract table names
        tables = []
        if response.result and response.result.data_array:
            for row in response.result.data_array:
                tables.append({
                    "database": row[0] if len(row) > 0 else None,
                    "table_name": row[1] if len(row) > 1 else None,
                    "is_temporary": row[2] if len(row) > 2 else None
                })
        
        return {
            "status": "success",
            "catalog": CATALOG_NAME,
            "schema": SCHEMA_NAME,
            "tables_found": len(tables),
            "tables": tables
        }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__,
            "traceback": traceback.format_exc()
        }


@api_app.get("/debug/check-years")
async def check_available_years(client: WorkspaceClient = Depends(get_databricks_client)):
    """Check what years of data are available"""
    try:
        sql = f"""
        SELECT 
            YEAR(month_start) as year,
            MIN(month_start) as earliest_month,
            MAX(month_start) as latest_month,
            COUNT(*) as month_count,
            SUM(total_members) as total_members,
            SUM(new_enrollments) as total_enrollments,
            SUM(terminations) as total_terminations
        FROM {CATALOG_NAME}.{SCHEMA_NAME}.v_membership_kpis
        GROUP BY YEAR(month_start)
        ORDER BY year DESC
        """
        
        response = client.statement_execution.execute_statement(
            warehouse_id=SQL_WAREHOUSE_ID,
            statement=sql,
            catalog=CATALOG_NAME,
            schema=SCHEMA_NAME,
            wait_timeout="30s"
        )
        
        if response.status.state != StatementState.SUCCEEDED:
            error_message = "Unknown error"
            if response.status.error:
                error_message = f"{response.status.error.error_code}: {response.status.error.message}"
            
            return {
                "status": "error",
                "query": sql,
                "error_details": error_message,
            }
        
        # Parse results
        years_data = []
        if response.result and response.result.data_array:
            for row in response.result.data_array:
                years_data.append({
                    "year": row[0],
                    "earliest_month": row[1],
                    "latest_month": row[2],
                    "month_count": row[3],
                    "total_members": row[4],
                    "total_enrollments": row[5],
                    "total_terminations": row[6],
                })
        
        return {
            "status": "success",
            "years_available": years_data,
            "recommendation": "Use the two most recent years with data for YTD comparison" if len(years_data) >= 2 else "Only one year of data available"
        }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__,
            "traceback": traceback.format_exc()
        }


@api_app.get("/debug/test-membership-trend")
async def test_membership_trend(client: WorkspaceClient = Depends(get_databricks_client)):
    """Test the membership trend query that the frontend uses"""
    try:
        sql = f"""
        SELECT 
            month_start,
            COUNT(DISTINCT CASE WHEN is_active THEN member_id END) as active_members,
            SUM(CASE WHEN is_new_enrollment THEN 1 ELSE 0 END) as new_enrollments,
            SUM(CASE WHEN is_termination THEN 1 ELSE 0 END) as terminations,
            AVG(risk_score) as avg_risk_score,
            AVG(pmpm_cost) as avg_pmpm
        FROM {CATALOG_NAME}.{SCHEMA_NAME}.fact_membership_monthly
        WHERE month_start >= DATE_SUB(CURRENT_DATE(), 360)
        GROUP BY month_start
        ORDER BY month_start DESC
        LIMIT 12
        """
        
        response = client.statement_execution.execute_statement(
            warehouse_id=SQL_WAREHOUSE_ID,
            statement=sql,
            catalog=CATALOG_NAME,
            schema=SCHEMA_NAME,
            wait_timeout="30s"
        )
        
        # Check if query succeeded
        if response.status.state != StatementState.SUCCEEDED:
            error_message = "Unknown error"
            if response.status.error:
                error_message = f"{response.status.error.error_code}: {response.status.error.message}"
            
            return {
                "status": "error",
                "query": sql,
                "state": str(response.status.state),
                "error_details": error_message,
            }
        
        # Parse results
        rows = []
        if response.result and response.result.data_array:
            for row in response.result.data_array:
                rows.append({
                    "month_start": row[0] if len(row) > 0 else None,
                    "active_members": row[1] if len(row) > 1 else None,
                    "new_enrollments": row[2] if len(row) > 2 else None,
                    "terminations": row[3] if len(row) > 3 else None,
                    "avg_risk_score": row[4] if len(row) > 4 else None,
                    "avg_pmpm": row[5] if len(row) > 5 else None,
                })
        
        return {
            "status": "success",
            "query": sql,
            "row_count": len(rows),
            "columns": [col.name for col in response.manifest.schema.columns] if response.manifest else [],
            "sample_data": rows[:3],  # First 3 rows
            "all_data": rows  # All rows for debugging
        }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__,
            "query": sql if 'sql' in locals() else "Query not generated",
            "traceback": traceback.format_exc()
        }


@api_app.post("/query", response_model=QueryResponse)
async def execute_query(
    request: QueryRequest,
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """
    Execute a SQL query against Databricks SQL warehouse
    """
    try:
        warehouse_id = request.warehouse_id or SQL_WAREHOUSE_ID
        
        if not warehouse_id:
            raise HTTPException(status_code=400, detail="SQL warehouse ID not configured")

        # Execute the query
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=request.sql,
            catalog=CATALOG_NAME,
            schema=SCHEMA_NAME,
            wait_timeout="30s"
        )

        # Check if query succeeded
        if response.status.state != StatementState.SUCCEEDED:
            raise HTTPException(
                status_code=500,
                detail=f"Query failed with state: {response.status.state}"
            )

        # Parse results
        if not response.result or not response.result.data_array:
            return QueryResponse(columns=[], data=[], row_count=0)

        columns = [col.name for col in response.manifest.schema.columns] if response.manifest else []
        
        # Convert data array to list of dicts
        data = []
        for row in response.result.data_array:
            row_dict = {}
            for i, value in enumerate(row):
                if i < len(columns):
                    row_dict[columns[i]] = value
            data.append(row_dict)

        return QueryResponse(
            columns=columns,
            data=data,
            row_count=len(data)
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api_app.get("/views/membership_kpis")
async def get_membership_kpis(client: WorkspaceClient = Depends(get_databricks_client)):
    """Get membership KPIs data"""
    if data_config:
        # Use config-driven query
        fields = ['month_start', 'total_members', 'new_enrollments', 'terminations', 'avg_risk_score', 'avg_tenure']
        sql = data_config.build_query('membership_kpis', fields, order_by='month_start DESC', limit=12)
    else:
        # Fallback to hardcoded query
        sql = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.v_membership_kpis ORDER BY month_start DESC LIMIT 12"
    
    request = QueryRequest(sql=sql)
    return await execute_query(request, client)


@api_app.get("/views/product_mix")
async def get_product_mix(client: WorkspaceClient = Depends(get_databricks_client)):
    """Get product mix data"""
    if data_config:
        fields = ['product_line', 'members', 'avg_age', 'avg_risk']
        sql = data_config.build_query('product_mix', fields, order_by='members DESC')
    else:
        sql = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.v_product_mix ORDER BY members DESC"
    
    request = QueryRequest(sql=sql)
    return await execute_query(request, client)


@api_app.get("/views/age_distribution")
async def get_age_distribution(client: WorkspaceClient = Depends(get_databricks_client)):
    """Get age distribution data"""
    if data_config:
        fields = ['age_range', 'members', 'avg_risk', 'high_risk_pct']
        order_clause = """CASE 
          WHEN age_range = '0-17' THEN 1
          WHEN age_range = '18-34' THEN 2
          WHEN age_range = '35-50' THEN 3
          WHEN age_range = '50-64' THEN 4
          WHEN age_range = '65+' THEN 5
        END"""
        sql = data_config.build_query('age_distribution', fields, order_by=order_clause)
    else:
        sql = f"""SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.v_age_distribution
                  ORDER BY CASE WHEN age_range = '0-17' THEN 1
                          WHEN age_range = '18-34' THEN 2
                          WHEN age_range = '35-50' THEN 3
                          WHEN age_range = '50-64' THEN 4
                          WHEN age_range = '65+' THEN 5 END"""
    
    request = QueryRequest(sql=sql)
    return await execute_query(request, client)


@api_app.get("/views/region_summary")
async def get_region_summary(client: WorkspaceClient = Depends(get_databricks_client)):
    """Get region summary data"""
    if data_config:
        fields = ['region', 'members', 'avg_age', 'avg_risk']
        sql = data_config.build_query('region_summary', fields, order_by='members DESC')
    else:
        sql = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.v_region_summary ORDER BY members DESC"
    
    request = QueryRequest(sql=sql)
    return await execute_query(request, client)


@api_app.get("/views/chronic_conditions")
async def get_chronic_conditions(client: WorkspaceClient = Depends(get_databricks_client)):
    """Get chronic conditions data"""
    if data_config:
        fields = ['condition_name', 'prevalence', 'members', 'avg_cost']
        sql = data_config.build_query('chronic_conditions', fields, order_by='prevalence DESC')
    else:
        sql = f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.v_chronic_conditions ORDER BY prevalence DESC"
    
    request = QueryRequest(sql=sql)
    return await execute_query(request, client)


@api_app.post("/drilldown")
async def execute_drilldown(
    viz_id: str,
    context: Dict[str, Any],
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """Execute drill-down query with permission checks"""
    try:
        # Get config service
        config_svc = get_config_service(client, SQL_WAREHOUSE_ID, CONFIG_CATALOG, CONFIG_SCHEMA)
        
        # Get visualization config
        viz_config = config_svc.get_viz_configs()
        viz = next((v for v in viz_config if v.id == viz_id), None)
        
        if not viz or not viz.allow_drill_down:
            raise HTTPException(status_code=400, detail="Drill-down not available for this visualization")
        
        # Get drill-down query
        if viz.drill_down_config and viz.drill_down_config.get('query_id'):
            drill_query_id = viz.drill_down_config['query_id']
            query_config = config_svc.get_query_config(drill_query_id)
            
            if not query_config:
                raise HTTPException(status_code=404, detail="Drill-down query not found")
            
            # Build query with context parameters
            params = {**context}
            params['catalog'] = CATALOG_NAME
            params['schema'] = SCHEMA_NAME
            
            sql = config_svc.build_query_from_template(drill_query_id, params)
        else:
            raise HTTPException(status_code=400, detail="No drill-down query configured")
        
        # Check permissions
        permissions_svc = get_permissions_service(client)
        sql = check_query_permissions(sql, user, permissions_svc)
        
        # Execute query
        response = client.statement_execution.execute_statement(
            warehouse_id=SQL_WAREHOUSE_ID,
            statement=sql,
            catalog=CATALOG_NAME,
            schema=SCHEMA_NAME,
            wait_timeout="30s"
        )
        
        if response.status.state != StatementState.SUCCEEDED:
            raise HTTPException(status_code=500, detail="Drill-down query failed")
        
        columns = [col.name for col in response.manifest.schema.columns] if response.manifest else []
        data = []
        if response.result and response.result.data_array:
            for row in response.result.data_array:
                row_dict = {columns[i]: val for i, val in enumerate(row) if i < len(columns)}
                data.append(row_dict)
        
        return {"columns": columns, "data": data, "row_count": len(data)}
        
    except Exception as e:
        logger.error(f"Drilldown failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@api_app.post("/genie/ask")
async def ask_genie(
    question: str,
    space_id: Optional[str] = None,
    conversation_id: Optional[str] = None,
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """Ask Databricks Genie a question"""
    try:
        genie_svc = get_genie_service(client, GENIE_SPACE_ID)
        result = await genie_svc.ask_question(
            question=question,
            space_id=space_id,
            user_context=user,
            conversation_id=conversation_id
        )
        return result
    except Exception as e:
        logger.error(f"Genie query failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@api_app.get("/genie/spaces")
async def list_genie_spaces(
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """List available Genie spaces"""
    try:
        genie_svc = get_genie_service(client, GENIE_SPACE_ID)
        spaces = await genie_svc.list_spaces(user_context=user)
        return {"spaces": spaces}
    except Exception as e:
        logger.error(f"Failed to list Genie spaces: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@api_app.post("/ai/generate-report")
async def generate_ai_report(
    report_type: str,
    dashboard_data: Dict[str, Any],
    user_prompt: Optional[str] = None,
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """Generate AI-powered report"""
    try:
        report_gen = get_report_generator(client, AI_MODEL_ENDPOINT)
        result = await report_gen.generate_report(
            report_type=report_type,
            dashboard_data=dashboard_data,
            user_prompt=user_prompt,
            user_context=user
        )
        return result
    except Exception as e:
        logger.error(f"AI report generation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@api_app.post("/export/csv")
async def export_csv_streaming(
    query_id: str,
    filters: Dict[str, Any],
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """Stream large CSV export"""
    async def generate_csv():
        try:
            # Get query config
            config_svc = get_config_service(client, SQL_WAREHOUSE_ID, CONFIG_CATALOG, CONFIG_SCHEMA)
            query_config = config_svc.get_query_config(query_id)
            
            if not query_config:
                yield "Error: Query not found\n"
                return
            
            # Build and check permissions
            params = {**filters, 'catalog': CATALOG_NAME, 'schema': SCHEMA_NAME}
            sql = config_svc.build_query_from_template(query_id, params)
            
            permissions_svc = get_permissions_service(client)
            sql = check_query_permissions(sql, user, permissions_svc)
            
            # Execute query
            response = client.statement_execution.execute_statement(
                warehouse_id=SQL_WAREHOUSE_ID,
                statement=sql,
                catalog=CATALOG_NAME,
                schema=SCHEMA_NAME,
                wait_timeout="60s"
            )
            
            if response.status.state != StatementState.SUCCEEDED:
                yield "Error: Query failed\n"
                return
            
            # Write headers
            columns = [col.name for col in response.manifest.schema.columns] if response.manifest else []
            yield ','.join(f'"{col}"' for col in columns) + '\n'
            
            # Stream rows
            if response.result and response.result.data_array:
                for row in response.result.data_array:
                    csv_row = ','.join(f'"{str(val)}"' for val in row)
                    yield csv_row + '\n'
                    
        except Exception as e:
            logger.error(f"CSV export failed: {str(e)}")
            yield f"Error: {str(e)}\n"
    
    return StreamingResponse(
        generate_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=export_{query_id}.csv"}
    )


# Admin Endpoints
@api_app.get("/admin/stats")
async def get_admin_stats(user: UserContext = Depends(get_admin_user)):
    """Get admin dashboard statistics"""
    return {
        "total_queries": 0,
        "active_queries": 0,
        "total_filters": 0,
        "total_visualizations": 0,
        "recent_errors": 0,
        "last_config_update": "Never"
    }


@api_app.get("/admin/queries")
async def list_admin_queries(
    user: UserContext = Depends(get_admin_user),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """List all query configurations"""
    try:
        config_svc = get_config_service(client, SQL_WAREHOUSE_ID, CONFIG_CATALOG, CONFIG_SCHEMA)
        queries = config_svc.get_all_queries()
        return [q.__dict__ for q in queries]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api_app.get("/admin/filters")
async def list_admin_filters(
    user: UserContext = Depends(get_admin_user),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """List all filter configurations"""
    try:
        config_svc = get_config_service(client, SQL_WAREHOUSE_ID, CONFIG_CATALOG, CONFIG_SCHEMA)
        filters = config_svc.get_filter_configs()
        return [f.__dict__ for f in filters]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# AI Dashboard Generation Endpoints
class AIDashboardRequest(BaseModel):
    prompt: str

class AIVisualizationRequest(BaseModel):
    data_summary: Dict[str, Any]
    user_question: str

@api_app.post("/ai/generate-dashboard")
async def generate_dashboard_with_ai(
    request: AIDashboardRequest,
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """
    Generate a complete dashboard configuration from a natural language prompt.
    Uses Databricks Foundation Models to create tabs, visualizations, queries, and filters.
    """
    try:
        from ai_dashboard_generator import AIDashboardGenerator
        
        generator = AIDashboardGenerator(client)
        
        # Get list of tables user has access to
        # For now, we'll use a simplified approach - in production, query permissions_service
        available_tables = [
            "v_membership_kpis",
            "v_product_mix",
            "v_age_distribution",
            "dim_member",
            "fact_membership_monthly"
        ]
        
        result = await generator.generate_dashboard_from_prompt(
            user_context=user,
            prompt=request.prompt,
            available_tables=available_tables,
            catalog=CATALOG_NAME,
            schema=SCHEMA_NAME
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error generating dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_app.post("/ai/suggest-visualization")
async def suggest_visualization_with_ai(
    request: AIVisualizationRequest,
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """
    Suggest the best visualization type for given data and user question.
    """
    try:
        from ai_dashboard_generator import AIDashboardGenerator
        
        generator = AIDashboardGenerator(client)
        
        result = await generator.suggest_visualization(
            user_context=user,
            data_summary=request.data_summary,
            user_question=request.user_question
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error suggesting visualization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Unity Catalog Browser Endpoints (with Authorization)
# ============================================================================

@api_app.get("/catalog/catalogs")
async def list_catalogs(
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """List catalogs accessible to the user"""
    try:
        logger.info(f"Listing catalogs for user: {user.email}")
        
        catalogs = []
        for catalog in client.catalogs.list():
            catalogs.append({
                "name": catalog.name,
                "comment": catalog.comment or "",
                "owner": catalog.owner or ""
            })
        
        logger.info(f"Found {len(catalogs)} catalogs for user {user.email}")
        return {"catalogs": catalogs}
        
    except Exception as e:
        logger.error(f"Failed to list catalogs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@api_app.get("/catalog/schemas")
async def list_schemas(
    catalog: str,
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """List schemas in a catalog accessible to the user"""
    try:
        logger.info(f"Listing schemas in catalog '{catalog}' for user: {user.email}")
        
        schemas = []
        for schema in client.schemas.list(catalog_name=catalog):
            schemas.append({
                "name": schema.name,
                "catalog": catalog,
                "comment": schema.comment or "",
                "owner": schema.owner or ""
            })
        
        logger.info(f"Found {len(schemas)} schemas in '{catalog}' for user {user.email}")
        return {"schemas": schemas}
        
    except Exception as e:
        logger.error(f"Failed to list schemas in '{catalog}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@api_app.get("/catalog/tables")
async def list_tables(
    catalog: str,
    schema: str,
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """List tables in a schema accessible to the user"""
    try:
        logger.info(f"Listing tables in '{catalog}.{schema}' for user: {user.email}")
        
        tables = []
        for table in client.tables.list(catalog_name=catalog, schema_name=schema):
            tables.append({
                "name": table.name,
                "catalog": catalog,
                "schema": schema,
                "table_type": table.table_type.value if table.table_type else "TABLE",
                "comment": table.comment or "",
                "owner": table.owner or ""
            })
        
        logger.info(f"Found {len(tables)} tables in '{catalog}.{schema}' for user {user.email}")
        return {"tables": tables}
        
    except Exception as e:
        logger.error(f"Failed to list tables in '{catalog}.{schema}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@api_app.get("/catalog/table-schema")
async def get_table_schema(
    catalog: str,
    schema: str,
    table: str,
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """Get table schema (columns) for a specific table"""
    try:
        logger.info(f"Getting schema for '{catalog}.{schema}.{table}' for user: {user.email}")
        
        table_info = client.tables.get(full_name=f"{catalog}.{schema}.{table}")
        
        columns = []
        if table_info.columns:
            for col in table_info.columns:
                columns.append({
                    "name": col.name,
                    "type": col.type_name.value if col.type_name else "STRING",
                    "comment": col.comment or "",
                    "nullable": col.nullable if col.nullable is not None else True
                })
        
        logger.info(f"Retrieved {len(columns)} columns for '{catalog}.{schema}.{table}'")
        return {
            "columns": columns,
            "table_type": table_info.table_type.value if table_info.table_type else "TABLE"
        }
        
    except Exception as e:
        logger.error(f"Failed to get schema for '{catalog}.{schema}.{table}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Dashboard Management Endpoints
# ============================================================================

@api_app.get("/dashboards/list")
async def list_dashboards(
    user: UserContext = Depends(get_user_context)
):
    """List all dashboards (stored in Unity Catalog or in-memory for now)"""
    try:
        logger.info(f"Listing dashboards for user: {user.email}")
        
        # TODO: Load from Unity Catalog table: system.dashboards
        # For now, return empty list - dashboards are stored client-side
        return {
            "dashboards": [],
            "note": "Dashboard persistence not yet implemented. Using client-side storage."
        }
        
    except Exception as e:
        logger.error(f"Failed to list dashboards: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@api_app.post("/dashboards/save")
async def save_dashboard(
    dashboard: dict,
    user: UserContext = Depends(get_user_context)
):
    """Save a dashboard (to Unity Catalog)"""
    try:
        logger.info(f"Saving dashboard for user: {user.email}, dashboard_id: {dashboard.get('id')}")
        
        # TODO: Save to Unity Catalog table: system.dashboards
        # For now, acknowledge the save
        return {
            "success": True,
            "dashboard_id": dashboard.get('id'),
            "note": "Dashboard persistence not yet implemented. Using client-side storage."
        }
        
    except Exception as e:
        logger.error(f"Failed to save dashboard: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@api_app.delete("/dashboards/{dashboard_id}")
async def delete_dashboard(
    dashboard_id: str,
    user: UserContext = Depends(get_user_context)
):
    """Delete a dashboard"""
    try:
        logger.info(f"Deleting dashboard {dashboard_id} for user: {user.email}")
        
        # TODO: Delete from Unity Catalog table
        return {
            "success": True,
            "note": "Dashboard persistence not yet implemented. Using client-side storage."
        }
        
    except Exception as e:
        logger.error(f"Failed to delete dashboard: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# AI Endpoints (Fix Authorization)
# ============================================================================

@api_app.post("/ai/suggest-visualization")
async def suggest_visualization_with_ai_fixed(
    request: dict,
    user: UserContext = Depends(get_user_context),
    client: WorkspaceClient = Depends(get_databricks_client)
):
    """
    Suggest the best visualization type for given data and user question.
    (Fixed version with proper auth)
    """
    try:
        logger.info(f"AI visualization suggestion for user: {user.email}")
        
        # TODO: Implement actual AI suggestion using Databricks Foundation Models
        # For now, return a simple suggestion based on data shape
        data_summary = request.get('data_summary', {})
        user_question = request.get('user_question', '')
        
        # Simple heuristic-based suggestion
        suggestion = {
            "chart_type": "bar",
            "reasoning": "Bar chart recommended for categorical comparisons",
            "config": {
                "xAxis": data_summary.get('columns', [''])[0] if data_summary.get('columns') else "",
                "yAxis": data_summary.get('columns', ['', ''])[1] if len(data_summary.get('columns', [])) > 1 else "",
                "aggregation": "SUM"
            }
        }
        
        logger.info(f"AI suggestion generated for user {user.email}")
        return suggestion
        
    except Exception as e:
        logger.error(f"Error suggesting visualization: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Mount API app
app.mount("/api", api_app)

# Serve static files (React build)
# Check for frontend build in multiple possible locations
frontend_dir = None
if os.path.exists("dist") and os.path.exists("dist/index.html"):
    frontend_dir = "dist"
    logger.info("Serving frontend from dist/ directory")
elif os.path.exists("client/build") and os.path.exists("client/build/index.html"):
    frontend_dir = "client/build"
    logger.info("Serving frontend from client/build/ directory")

if frontend_dir:
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="ui")
    logger.info(f"Frontend mounted successfully from {frontend_dir}")
else:
    logger.warning("Frontend not found - no dist/ or client/build/ directory with index.html")
    
    @app.get("/")
    async def root():
        return {
            "message": "Health Insurance Dashboard Backend - Enterprise Edition",
            "note": "Frontend not built yet. Run 'npm run build' to create dist/ directory.",
            "features": [
                "On-behalf-of authentication",
                "Databricks Genie integration",
                "AI-powered reports",
                "Drill-down analytics",
                "CSV streaming export",
                "Admin configuration UI"
            ],
            "expected_paths": ["dist/index.html", "client/build/index.html"]
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

