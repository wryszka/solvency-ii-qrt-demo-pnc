import csv
import io
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.config import fqn, get_current_user, get_catalog, get_schema
from server.sql import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/approvals", tags=["approvals"])

VALID_QRTS = {"s0602", "s0501", "s2501"}

QRT_EXPORT_TABLES = {
    "s0602": "s0602_list_of_assets",
    "s0501": "s0501_premiums_claims_expenses",
    "s2501": "s2501_scr_breakdown",
}


class ReviewRequest(BaseModel):
    status: str  # "approved" or "rejected"
    comments: Optional[str] = None


async def ensure_approvals_table() -> None:
    await execute_query(f"""
        CREATE TABLE IF NOT EXISTS {fqn('qrt_approvals')} (
            approval_id STRING,
            qrt_id STRING,
            reporting_period STRING,
            status STRING,
            submitted_by STRING,
            submitted_at TIMESTAMP,
            reviewed_by STRING,
            reviewed_at TIMESTAMP,
            comments STRING,
            export_path STRING
        )
    """)


# ── Get approval for a specific QRT ──────────────────────────────────────────

@router.get("/{qrt_id}")
async def get_approval(qrt_id: str):
    if qrt_id not in VALID_QRTS:
        raise HTTPException(404, "Unknown QRT")
    try:
        await ensure_approvals_table()
        rows = await execute_query(f"""
            SELECT * FROM {fqn('qrt_approvals')}
            WHERE qrt_id = '{qrt_id}'
            ORDER BY submitted_at DESC LIMIT 1
        """)
        return {"data": rows[0] if rows else None}
    except Exception as exc:
        logger.exception("Failed to fetch approval for %s", qrt_id)
        raise HTTPException(500, str(exc)) from exc


# ── Get all approvals (history) ──────────────────────────────────────────────

@router.get("")
async def get_all_approvals():
    try:
        rows = await execute_query(f"""
            SELECT * FROM {fqn('qrt_approvals')}
            ORDER BY submitted_at DESC
        """)
        return {"data": rows}
    except Exception:
        return {"data": []}


# ── Submit for review ────────────────────────────────────────────────────────

@router.post("/{qrt_id}/submit")
async def submit_for_review(qrt_id: str):
    if qrt_id not in VALID_QRTS:
        raise HTTPException(404, "Unknown QRT")

    await ensure_approvals_table()
    approval_id = str(uuid.uuid4())
    submitted_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    submitted_by = get_current_user()

    # Get latest reporting period from the QRT table
    table = QRT_EXPORT_TABLES[qrt_id]
    try:
        period_rows = await execute_query(
            f"SELECT MAX(reporting_period) AS rp FROM {fqn(table)}"
        )
        reporting_period = period_rows[0]["rp"] if period_rows else "unknown"
    except Exception:
        reporting_period = "unknown"

    try:
        await execute_query(f"""
            INSERT INTO {fqn('qrt_approvals')}
            (approval_id, qrt_id, reporting_period, status, submitted_by, submitted_at,
             reviewed_by, reviewed_at, comments, export_path)
            VALUES (
                '{approval_id}', '{qrt_id}', '{reporting_period}', 'pending',
                '{submitted_by}', '{submitted_at}', NULL, NULL, NULL, NULL
            )
        """)
        return {
            "approval_id": approval_id,
            "qrt_id": qrt_id,
            "reporting_period": reporting_period,
            "status": "pending",
            "submitted_by": submitted_by,
        }
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


# ── Approve or reject ────────────────────────────────────────────────────────

@router.post("/{qrt_id}/review")
async def review_qrt(qrt_id: str, request: ReviewRequest):
    if qrt_id not in VALID_QRTS:
        raise HTTPException(404, "Unknown QRT")
    if request.status not in ("approved", "rejected"):
        raise HTTPException(400, "Status must be 'approved' or 'rejected'")

    await ensure_approvals_table()
    reviewed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    reviewed_by = get_current_user()
    comments_escaped = (request.comments or "").replace("'", "''")

    try:
        pending = await execute_query(f"""
            SELECT approval_id, reporting_period
            FROM {fqn('qrt_approvals')}
            WHERE qrt_id = '{qrt_id}' AND status = 'pending'
            ORDER BY submitted_at DESC LIMIT 1
        """)
        if not pending:
            raise HTTPException(404, "No pending approval found")

        approval_id = pending[0]["approval_id"]
        reporting_period = pending[0]["reporting_period"]

        export_path = None
        if request.status == "approved":
            # Export to volume (simulated Tagetik export)
            export_path = await _export_to_volume(qrt_id, reporting_period, reviewed_at)

        export_sql = f"'{export_path}'" if export_path else "NULL"

        await execute_query(f"""
            MERGE INTO {fqn('qrt_approvals')} t
            USING (SELECT '{approval_id}' AS approval_id) s
            ON t.approval_id = s.approval_id
            WHEN MATCHED THEN UPDATE SET
                status = '{request.status}',
                reviewed_by = '{reviewed_by}',
                reviewed_at = '{reviewed_at}',
                comments = '{comments_escaped}',
                export_path = {export_sql}
        """)

        return {
            "approval_id": approval_id,
            "status": request.status,
            "reviewed_by": reviewed_by,
            "export_path": export_path,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


async def _export_to_volume(qrt_id: str, reporting_period: str, timestamp: str) -> str:
    """Export QRT data to the regulatory_exports volume (simulated Tagetik)."""
    catalog = get_catalog()
    schema = get_schema()
    table = QRT_EXPORT_TABLES[qrt_id]
    ts_clean = timestamp.replace(" ", "T").replace(":", "")

    qrt_name = {"s0602": "S0602", "s0501": "S0501", "s2501": "S2501"}[qrt_id]
    period_clean = reporting_period.replace("-", "")
    filename = f"TAGETIK_{qrt_name}_{period_clean}_approved_{ts_clean}.csv"
    volume_path = f"/Volumes/{catalog}/{schema}/regulatory_exports/{filename}"

    try:
        # Get the data
        rows = await execute_query(f"SELECT * FROM {fqn(table)} WHERE reporting_period = '{reporting_period}'")
        if not rows:
            rows = await execute_query(f"SELECT * FROM {fqn(table)}")

        # Build CSV
        if rows:
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
            csv_content = buf.getvalue()

            # Upload to volume via workspace client
            from server.config import get_workspace_client
            w = get_workspace_client()
            w.files.upload(volume_path, io.BytesIO(csv_content.encode("utf-8")), overwrite=True)
            logger.info("Exported %s to %s (%d rows)", qrt_id, volume_path, len(rows))

        return volume_path
    except Exception as e:
        logger.warning("Volume export failed for %s: %s — recording path anyway", qrt_id, e)
        return f"{volume_path} (export pending)"
