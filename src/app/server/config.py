import os
import logging

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)
_workspace_client: WorkspaceClient | None = None


def is_databricks_app() -> bool:
    return os.getenv("DATABRICKS_APP_NAME") is not None


def get_workspace_client() -> WorkspaceClient:
    global _workspace_client
    if _workspace_client is None:
        if is_databricks_app():
            _workspace_client = WorkspaceClient()
        else:
            profile = os.getenv("DATABRICKS_PROFILE", "DEFAULT")
            _workspace_client = WorkspaceClient(profile=profile)
    return _workspace_client


def get_catalog() -> str:
    return os.getenv("CATALOG_NAME", "lr_serverless_aws_us_catalog")


def get_schema() -> str:
    return os.getenv("SCHEMA_NAME", "solvency2demo")


def get_warehouse_id() -> str:
    return os.getenv("WAREHOUSE_ID", "c80acfa212bf1166")


def fqn(table: str) -> str:
    return f"{get_catalog()}.{get_schema()}.{table}"


def get_current_user() -> str:
    try:
        me = get_workspace_client().current_user.me()
        return me.user_name or me.display_name or "unknown"
    except Exception:
        return os.getenv("USER", "demo-user")
