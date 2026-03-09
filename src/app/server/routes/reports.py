import csv
import io
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from server.config import get_catalog, get_schema
from server.sql import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/reports", tags=["reports"])


def _rows_to_csv_stream(rows: list[dict]) -> io.StringIO:
    """Convert a list of row dicts to an in-memory CSV StringIO."""
    if not rows:
        return io.StringIO("")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)
    return buf


# ── S.06.02 ──────────────────────────────────────────────────────────────────

@router.get("/s0602")
async def get_s0602(
    cic_filter: Optional[str] = Query(None, description="Filter by CIC code prefix"),
    country_filter: Optional[str] = Query(None, description="Filter by issuer country"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """Return paginated S.06.02 List of Assets."""
    catalog = get_catalog()
    schema = get_schema()

    where_clauses: list[str] = []
    if cic_filter:
        where_clauses.append(f"c0270_cic LIKE '{cic_filter}%'")
    if country_filter:
        where_clauses.append(f"c0250_issuer_country = '{country_filter}'")

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    offset = (page - 1) * page_size

    try:
        count_rows = await execute_query(
            f"SELECT COUNT(*) AS cnt FROM {catalog}.{schema}.gold_qrt_s0602 {where_sql}"
        )
        total = int(count_rows[0]["cnt"] or 0)

        rows = await execute_query(
            f"""
            SELECT *
            FROM {catalog}.{schema}.gold_qrt_s0602
            {where_sql}
            ORDER BY c0040_asset_id
            LIMIT {page_size} OFFSET {offset}
            """
        )

        return {
            "data": rows,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size,
        }
    except Exception as exc:
        logger.exception("Failed to fetch S.06.02 data")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/s0602/csv")
async def get_s0602_csv():
    """Download full S.06.02 as CSV."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        rows = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.gold_qrt_s0602 ORDER BY c0040_asset_id"
        )
        buf = _rows_to_csv_stream(rows)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=s0602_list_of_assets.csv"},
        )
    except Exception as exc:
        logger.exception("Failed to export S.06.02 CSV")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── S.05.01 ──────────────────────────────────────────────────────────────────

@router.get("/s0501")
async def get_s0501():
    """Return S.05.01 Premiums, Claims & Expenses."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        rows = await execute_query(
            f"""
            SELECT *
            FROM {catalog}.{schema}.gold_qrt_s0501
            ORDER BY template_row_id, lob_code
            """
        )
        return {"data": rows}
    except Exception as exc:
        logger.exception("Failed to fetch S.05.01 data")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/s0501/csv")
async def get_s0501_csv():
    """Download full S.05.01 as CSV."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        rows = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.gold_qrt_s0501 ORDER BY template_row_id, lob_code"
        )
        buf = _rows_to_csv_stream(rows)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=s0501_premiums_claims_expenses.csv"},
        )
    except Exception as exc:
        logger.exception("Failed to export S.05.01 CSV")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── S.25.01 ──────────────────────────────────────────────────────────────────

@router.get("/s2501")
async def get_s2501():
    """Return S.25.01 SCR Standard Formula."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        rows = await execute_query(
            f"""
            SELECT *
            FROM {catalog}.{schema}.gold_qrt_s2501
            ORDER BY template_row_id
            """
        )
        return {"data": rows}
    except Exception as exc:
        logger.exception("Failed to fetch S.25.01 data")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/s2501/csv")
async def get_s2501_csv():
    """Download full S.25.01 as CSV."""
    catalog = get_catalog()
    schema = get_schema()
    try:
        rows = await execute_query(
            f"SELECT * FROM {catalog}.{schema}.gold_qrt_s2501 ORDER BY template_row_id"
        )
        buf = _rows_to_csv_stream(rows)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=s2501_scr_standard_formula.csv"},
        )
    except Exception as exc:
        logger.exception("Failed to export S.25.01 CSV")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
