import os
import logging

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_workspace_client: WorkspaceClient | None = None


def is_databricks_app() -> bool:
    """Detect if running inside a Databricks App environment."""
    return os.getenv("DATABRICKS_APP_NAME") is not None


def get_workspace_client() -> WorkspaceClient:
    """Return a cached WorkspaceClient instance.

    In a Databricks App the SDK auto-configures from the environment.
    Locally it uses the profile specified by DATABRICKS_PROFILE (default:
    'DEFAULT').
    """
    global _workspace_client
    if _workspace_client is None:
        if is_databricks_app():
            logger.info("Initialising WorkspaceClient (Databricks App mode)")
            _workspace_client = WorkspaceClient()
        else:
            profile = os.getenv("DATABRICKS_PROFILE", "DEFAULT")
            logger.info(
                "Initialising WorkspaceClient (local mode, profile=%s)", profile
            )
            _workspace_client = WorkspaceClient(profile=profile)
    return _workspace_client


def get_catalog() -> str:
    return os.getenv("CATALOG_NAME", "lr_classic_aws_us_catalog")


def get_schema() -> str:
    return os.getenv("SCHEMA_NAME", "solvency2demo")


def get_warehouse_id() -> str:
    return os.getenv("SQL_WAREHOUSE_ID", "c80acfa212bf1166")


def get_current_user() -> str:
    """Get the current user identity."""
    try:
        client = get_workspace_client()
        me = client.current_user.me()
        return me.user_name or me.display_name or "unknown"
    except Exception:
        return os.getenv("USER", "unknown")
