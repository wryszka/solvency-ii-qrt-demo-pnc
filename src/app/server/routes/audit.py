import csv
import io
import logging
import zipfile
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from server.config import get_catalog, get_schema
from server.sql import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/audit", tags=["audit"])


def _rows_to_csv_bytes(rows: list[dict]) -> bytes:
    """Convert a list of row dicts to CSV bytes."""
    if not rows:
        return b""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


# ── Lineage ──────────────────────────────────────────────────────────────────

@router.get("/lineage")
async def get_lineage(all: bool = Query(False, description="Return all runs, not just latest")):
    """Return audit pipeline lineage records."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        if all:
            rows = await execute_query(
                f"""
                SELECT *
                FROM {catalog}.{schema}.audit_pipeline_lineage
                ORDER BY executed_at DESC
                """
            )
        else:
            rows = await execute_query(
                f"""
                SELECT *
                FROM {catalog}.{schema}.audit_pipeline_lineage
                WHERE pipeline_run_id = (
                    SELECT pipeline_run_id
                    FROM {catalog}.{schema}.audit_pipeline_lineage
                    ORDER BY executed_at DESC
                    LIMIT 1
                )
                ORDER BY step_sequence
                """
            )
        return {"data": rows}
    except Exception as exc:
        logger.exception("Failed to fetch lineage data")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/lineage/csv")
async def get_lineage_csv():
    """Download full lineage table as CSV."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        rows = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.audit_pipeline_lineage ORDER BY executed_at DESC"
        )
        csv_text = _rows_to_csv_bytes(rows).decode("utf-8")
        return StreamingResponse(
            iter([csv_text]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=audit_pipeline_lineage.csv"},
        )
    except Exception as exc:
        logger.exception("Failed to export lineage CSV")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Data Quality ─────────────────────────────────────────────────────────────

@router.get("/quality")
async def get_quality():
    """Return data quality log for latest pipeline run."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        rows = await execute_query(
            f"""
            SELECT *
            FROM {catalog}.{schema}.audit_data_quality_log
            WHERE pipeline_run_id = (
                SELECT pipeline_run_id
                FROM {catalog}.{schema}.audit_data_quality_log
                ORDER BY executed_at DESC
                LIMIT 1
            )
            ORDER BY executed_at
            """
        )
        return {"data": rows}
    except Exception as exc:
        logger.exception("Failed to fetch DQ data")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/quality/csv")
async def get_quality_csv():
    """Download full DQ log as CSV."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        rows = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.audit_data_quality_log ORDER BY executed_at DESC"
        )
        csv_text = _rows_to_csv_bytes(rows).decode("utf-8")
        return StreamingResponse(
            iter([csv_text]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=audit_data_quality_log.csv"},
        )
    except Exception as exc:
        logger.exception("Failed to export DQ CSV")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Schema tables ────────────────────────────────────────────────────────────

@router.get("/tables")
async def get_tables():
    """Return list of all tables in the schema with row counts."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        table_rows = await execute_query(
            f"SHOW TABLES IN {catalog}.{schema}"
        )
        results = []
        for t in table_rows:
            table_name = t.get("tableName") or t.get("table_name") or t.get("tablename")
            if not table_name:
                continue
            count_rows = await execute_query(
                f"SELECT COUNT(*) AS cnt FROM {catalog}.{schema}.{table_name}"
            )
            results.append({
                "table_name": table_name,
                "row_count": int(count_rows[0]["cnt"] or 0) if count_rows else 0,
            })
        return {"data": results}
    except Exception as exc:
        logger.exception("Failed to list tables")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Regulatory Package (ZIP) ─────────────────────────────────────────────────

@router.get("/regulatory-package")
async def get_regulatory_package():
    """Download a ZIP containing all QRT CSVs, audit CSVs, and approval history."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        s0602 = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.gold_qrt_s0602 ORDER BY c0040_asset_id"
        )
        s0501 = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.gold_qrt_s0501 ORDER BY template_row_id, lob_code"
        )
        s2501 = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.gold_qrt_s2501 ORDER BY template_row_id"
        )
        lineage = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.audit_pipeline_lineage ORDER BY executed_at DESC"
        )
        dq_log = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.audit_data_quality_log ORDER BY executed_at DESC"
        )

        # Approvals table may not exist yet
        try:
            approvals = await execute_query(
                f"SELECT * FROM {catalog}.{schema}.audit_approvals ORDER BY submitted_at DESC"
            )
        except Exception:
            approvals = []

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("s0602_list_of_assets.csv", _rows_to_csv_bytes(s0602))
            zf.writestr("s0501_premiums_claims_expenses.csv", _rows_to_csv_bytes(s0501))
            zf.writestr("s2501_scr_standard_formula.csv", _rows_to_csv_bytes(s2501))
            zf.writestr("audit_pipeline_lineage.csv", _rows_to_csv_bytes(lineage))
            zf.writestr("audit_data_quality_log.csv", _rows_to_csv_bytes(dq_log))
            zf.writestr("audit_approvals.csv", _rows_to_csv_bytes(approvals))

        zip_buffer.seek(0)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"solvency2_regulatory_package_{timestamp}.zip"

        return StreamingResponse(
            iter([zip_buffer.getvalue()]),
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as exc:
        logger.exception("Failed to build regulatory package")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
