import logging

from fastapi import APIRouter, HTTPException

from server.config import get_catalog, get_schema
from server.sql import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("")
async def get_dashboard():
    """Return key metrics for the executive dashboard."""
    catalog = get_catalog()
    schema = get_schema()

    try:
        # --- S.06.02 asset metrics ---
        asset_rows = await execute_query(
            f"""
            SELECT
                SUM(CAST(c0170_total_sii_amount AS DOUBLE)) AS total_assets_sii,
                COUNT(*) AS asset_count
            FROM {catalog}.{schema}.gold_qrt_s0602
            """
        )
        total_assets_sii = float(asset_rows[0]["total_assets_sii"] or 0)
        asset_count = int(asset_rows[0]["asset_count"] or 0)

        # --- S.05.01 P&L metrics (lob_code = 0 = Total) ---
        pnl_rows = await execute_query(
            f"""
            SELECT
                template_row_id,
                SUM(CAST(amount_eur AS DOUBLE)) AS total_amount
            FROM {catalog}.{schema}.gold_qrt_s0501
            WHERE lob_code = '0'
              AND template_row_id IN ('R0110', 'R0200', 'R0300', 'R0400', 'R0550')
            GROUP BY template_row_id
            """
        )
        pnl_map: dict[str, float] = {}
        for row in pnl_rows:
            pnl_map[row["template_row_id"]] = float(row["total_amount"] or 0)

        gwp = pnl_map.get("R0110", 0.0)
        nwp = pnl_map.get("R0200", 0.0)
        npe = pnl_map.get("R0300", 0.0)
        net_claims = pnl_map.get("R0400", 0.0)
        total_expenses = pnl_map.get("R0550", 0.0)

        combined_ratio = (
            ((net_claims + total_expenses) / npe * 100) if npe != 0 else None
        )

        # --- S.25.01 SCR metrics ---
        scr_rows = await execute_query(
            f"""
            SELECT template_row_id, c0110_gross_scr
            FROM {catalog}.{schema}.gold_qrt_s2501
            WHERE template_row_id IN ('R0100', 'R0200')
            """
        )
        scr_map: dict[str, float] = {}
        for row in scr_rows:
            scr_map[row["template_row_id"]] = float(row["c0110_gross_scr"] or 0)

        scr = scr_map.get("R0200", 0.0)
        bscr = scr_map.get("R0100", 0.0)
        solvency_ratio = (total_assets_sii / scr * 100) if scr > 0 else None

        # --- Audit / pipeline metrics ---
        lineage_rows = await execute_query(
            f"""
            SELECT MAX(executed_at) AS pipeline_last_run
            FROM {catalog}.{schema}.audit_pipeline_lineage
            """
        )
        pipeline_last_run = lineage_rows[0]["pipeline_last_run"] if lineage_rows else None

        # DQ checks for latest pipeline run
        dq_rows = await execute_query(
            f"""
            SELECT
                COUNT(*) AS dq_checks_total,
                SUM(CASE WHEN passed = true THEN 1 ELSE 0 END) AS dq_checks_passed
            FROM {catalog}.{schema}.audit_data_quality_log
            WHERE pipeline_run_id = (
                SELECT pipeline_run_id
                FROM {catalog}.{schema}.audit_data_quality_log
                ORDER BY executed_at DESC
                LIMIT 1
            )
            """
        )
        dq_checks_total = int(dq_rows[0]["dq_checks_total"] or 0) if dq_rows else 0
        dq_checks_passed = int(dq_rows[0]["dq_checks_passed"] or 0) if dq_rows else 0

        return {
            "total_assets_sii": total_assets_sii,
            "asset_count": asset_count,
            "gwp": gwp,
            "nwp": nwp,
            "net_claims": net_claims,
            "total_expenses": total_expenses,
            "combined_ratio": combined_ratio,
            "scr": scr,
            "bscr": bscr,
            "solvency_ratio": solvency_ratio,
            "pipeline_last_run": pipeline_last_run,
            "dq_checks_total": dq_checks_total,
            "dq_checks_passed": dq_checks_passed,
        }

    except Exception as exc:
        logger.exception("Failed to build dashboard metrics")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
