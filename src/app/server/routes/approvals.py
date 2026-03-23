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

VALID_QRTS = {"s0602", "s0501", "s2501", "s2606"}

QRT_EXPORT_TABLES = {
    "s0602": "s0602_list_of_assets",
    "s0501": "s0501_premiums_claims_expenses",
    "s2501": "s2501_scr_breakdown",
    "s2606": "s2606_nl_uw_risk",
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


@router.post("/{qrt_id}/certificate")
async def generate_certificate(qrt_id: str):
    """Generate a PDF approval certificate and upload to the regulatory volume."""
    if qrt_id not in VALID_QRTS:
        raise HTTPException(404, "Unknown QRT")

    try:
        await ensure_approvals_table()
        rows = await execute_query(f"""
            SELECT * FROM {fqn('qrt_approvals')}
            WHERE qrt_id = '{qrt_id}' AND status = 'approved'
            ORDER BY submitted_at DESC LIMIT 1
        """)
        if not rows:
            raise HTTPException(404, "No approved submission found for this QRT")

        approval = rows[0]
        cert_path = await _generate_pdf_certificate(qrt_id, approval)
        return {"certificate_path": cert_path, "approval": approval}

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


async def _generate_pdf_certificate(qrt_id: str, approval: dict) -> str:
    """Generate a PDF certificate and upload to the regulatory exports volume."""
    from fpdf import FPDF
    import hashlib

    catalog = get_catalog()
    schema = get_schema()
    qrt_name = {"s0602": "S.06.02", "s0501": "S.05.01", "s2501": "S.25.01"}[qrt_id]
    qrt_title = {"s0602": "List of Assets", "s0501": "Premiums, Claims & Expenses", "s2501": "SCR Standard Formula"}[qrt_id]

    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    # Title
    pdf.set_font("Helvetica", "B", 20)
    pdf.cell(0, 15, "QRT Approval Certificate", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(5)

    # Entity
    pdf.set_font("Helvetica", "", 14)
    pdf.cell(0, 10, "Bricksurance SE", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(10)

    # Details table
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(60, 8, "QRT Reference:", new_x="RIGHT")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, f"{qrt_name} - {qrt_title}", new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(60, 8, "Reporting Period:", new_x="RIGHT")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, str(approval.get("reporting_period", "")), new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(60, 8, "Status:", new_x="RIGHT")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, "APPROVED", new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(60, 8, "Submitted By:", new_x="RIGHT")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, str(approval.get("submitted_by", "")), new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(60, 8, "Submitted At:", new_x="RIGHT")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, str(approval.get("submitted_at", "")), new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(60, 8, "Reviewed By:", new_x="RIGHT")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, str(approval.get("reviewed_by", "")), new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(60, 8, "Reviewed At:", new_x="RIGHT")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, str(approval.get("reviewed_at", "")), new_x="LMARGIN", new_y="NEXT")

    if approval.get("comments"):
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(60, 8, "Comments:", new_x="RIGHT")
        pdf.set_font("Helvetica", "", 11)
        pdf.cell(0, 8, str(approval["comments"]), new_x="LMARGIN", new_y="NEXT")

    pdf.ln(5)

    # Data hash
    export_path = approval.get("export_path", "")
    data_hash = hashlib.sha256(f"{qrt_id}:{approval.get('reporting_period')}:{approval.get('reviewed_at')}".encode()).hexdigest()
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(60, 8, "Data Hash (SHA-256):", new_x="RIGHT")
    pdf.set_font("Courier", "", 9)
    pdf.cell(0, 8, data_hash, new_x="LMARGIN", new_y="NEXT")

    if export_path:
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(60, 8, "Export Path:", new_x="RIGHT")
        pdf.set_font("Courier", "", 9)
        pdf.cell(0, 8, str(export_path), new_x="LMARGIN", new_y="NEXT")

    pdf.ln(10)

    # Footer
    pdf.set_font("Helvetica", "I", 9)
    pdf.cell(0, 8, "This certificate was generated automatically by the Solvency II QRT Reporting System.", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.cell(0, 8, "It confirms that the above QRT has been reviewed and approved for regulatory submission.", new_x="LMARGIN", new_y="NEXT", align="C")

    # Generate PDF bytes
    pdf_bytes = pdf.output()

    # Upload to volume
    ts_clean = str(approval.get("reviewed_at", "")).replace(" ", "T").replace(":", "")
    period_clean = str(approval.get("reporting_period", "")).replace("-", "")
    filename = f"CERT_{qrt_name.replace('.', '')}_{period_clean}_{ts_clean}.pdf"
    volume_path = f"/Volumes/{catalog}/{schema}/regulatory_exports/{filename}"

    try:
        from server.config import get_workspace_client
        w = get_workspace_client()
        w.files.upload(volume_path, io.BytesIO(pdf_bytes), overwrite=True)
        logger.info("Certificate uploaded to %s", volume_path)
    except Exception as e:
        logger.warning("Certificate upload failed: %s", e)
        volume_path = f"{volume_path} (upload pending)"

    return volume_path


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
