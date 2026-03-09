import asyncio
import logging
from typing import Any

from databricks.sdk.service.sql import StatementState

from server.config import get_workspace_client, get_warehouse_id

logger = logging.getLogger(__name__)


def _execute_sync(sql: str) -> list[dict[str, Any]]:
    """Synchronous SQL execution via the Databricks Statement Execution API."""
    client = get_workspace_client()
    warehouse_id = get_warehouse_id()

    logger.debug("Executing SQL on warehouse %s: %s", warehouse_id, sql[:200])

    response = client.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout="50s",
    )

    if response.status and response.status.state == StatementState.FAILED:
        error_msg = (
            response.status.error.message
            if response.status.error
            else "Unknown SQL error"
        )
        logger.error("SQL execution failed: %s", error_msg)
        raise RuntimeError(f"SQL execution failed: {error_msg}")

    if response.status and response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(
            f"SQL statement in unexpected state: {response.status.state}"
        )

    columns = [col.name for col in response.manifest.schema.columns]
    rows: list[dict[str, Any]] = []

    if response.result and response.result.data_array:
        for row_data in response.result.data_array:
            rows.append(dict(zip(columns, row_data)))

    if response.result and response.result.external_links:
        for link in response.result.external_links:
            chunk = client.statement_execution.get_statement_result_chunk_n(
                statement_id=response.statement_id,
                chunk_index=link.chunk_index,
            )
            if chunk.data_array:
                for row_data in chunk.data_array:
                    rows.append(dict(zip(columns, row_data)))

    logger.debug("Query returned %d rows", len(rows))
    return rows


async def execute_query(sql: str) -> list[dict[str, Any]]:
    """Async wrapper — runs the blocking SDK call in a thread."""
    return await asyncio.to_thread(_execute_sync, sql)
