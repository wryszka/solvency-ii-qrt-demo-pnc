import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.config import get_catalog, get_schema, get_current_user
from server.sql import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/approvals", tags=["approvals"])


class ReviewRequest(BaseModel):
    status: str  # "approved" or "rejected"
    comments: Optional[str] = None


# ── Table bootstrap ──────────────────────────────────────────────────────────

async def ensure_approvals_table() -> None:
    """Create the audit_approvals table if it does not exist."""
    catalog = get_catalog()
    schema = get_schema()
    await execute_query(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.audit_approvals (
            approval_id STRING,
            reporting_period STRING,
            status STRING,
            submitted_by STRING,
            submitted_at TIMESTAMP,
            reviewed_by STRING,
            reviewed_at TIMESTAMP,
            comments STRING
        )
        """
    )


# ── Routes ───────────────────────────────────────────────────────────────────

@router.get("")
async def get_approvals():
    """Return all approval records."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        rows = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.audit_approvals ORDER BY submitted_at DESC"
        )
        return {"data": rows}
    except Exception as exc:
        logger.exception("Failed to fetch approvals")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/current")
async def get_current_approval():
    """Return the latest approval."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        rows = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.audit_approvals ORDER BY submitted_at DESC LIMIT 1"
        )
        return {"data": rows[0] if rows else None}
    except Exception as exc:
        logger.exception("Failed to fetch current approval")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/submit")
async def submit_approval():
    """Create a new pending approval record. Auto-detects user and reporting period."""
    catalog = get_catalog()
    schema = get_schema()
    approval_id = str(uuid.uuid4())
    submitted_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    submitted_by = get_current_user()

    # Get reporting period from the latest lineage run
    try:
        period_rows = await execute_query(
            f"SELECT reporting_period FROM {catalog}.{schema}.audit_pipeline_lineage ORDER BY executed_at DESC LIMIT 1"
        )
        reporting_period = period_rows[0]["reporting_period"] if period_rows else "unknown"
    except Exception:
        reporting_period = "unknown"

    try:
        await execute_query(
            f"""
            INSERT INTO {catalog}.{schema}.audit_approvals
            (approval_id, reporting_period, status, submitted_by, submitted_at, reviewed_by, reviewed_at, comments)
            VALUES (
                '{approval_id}',
                '{reporting_period}',
                'pending',
                '{submitted_by}',
                '{submitted_at}',
                NULL,
                NULL,
                NULL
            )
            """
        )
        return {
            "approval_id": approval_id,
            "reporting_period": reporting_period,
            "status": "pending",
            "submitted_by": submitted_by,
            "submitted_at": submitted_at,
            "reviewed_by": None,
            "reviewed_at": None,
            "comments": None,
        }
    except Exception as exc:
        logger.exception("Failed to submit approval")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/review")
async def review_approval(request: ReviewRequest):
    """Approve or reject the latest pending approval."""
    if request.status not in ("approved", "rejected"):
        raise HTTPException(status_code=400, detail="Status must be 'approved' or 'rejected'")

    catalog = get_catalog()
    schema = get_schema()
    reviewed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    reviewed_by = get_current_user()
    comments_escaped = (request.comments or "").replace("'", "''")

    try:
        # Find the latest pending approval
        pending = await execute_query(
            f"""
            SELECT approval_id FROM {catalog}.{schema}.audit_approvals
            WHERE status = 'pending'
            ORDER BY submitted_at DESC LIMIT 1
            """
        )
        if not pending:
            raise HTTPException(status_code=404, detail="No pending approval found")

        approval_id = pending[0]["approval_id"]

        # Delta tables don't support UPDATE — use MERGE instead
        await execute_query(
            f"""
            MERGE INTO {catalog}.{schema}.audit_approvals t
            USING (SELECT '{approval_id}' AS approval_id) s
            ON t.approval_id = s.approval_id
            WHEN MATCHED THEN UPDATE SET
                status = '{request.status}',
                reviewed_by = '{reviewed_by}',
                reviewed_at = '{reviewed_at}',
                comments = '{comments_escaped}'
            """
        )

        return {
            "approval_id": approval_id,
            "status": request.status,
            "reviewed_by": reviewed_by,
            "reviewed_at": reviewed_at,
            "comments": request.comments,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to review approval")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
