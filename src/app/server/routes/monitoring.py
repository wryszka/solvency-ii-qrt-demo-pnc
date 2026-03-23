import logging

from fastapi import APIRouter, HTTPException, Query

from server.config import fqn
from server.sql import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/monitoring", tags=["monitoring"])


@router.get("/sla-status")
async def get_sla_status(period: str = Query(None)):
    """Feed arrival status vs SLA deadlines."""
    try:
        where = f"WHERE reporting_period = '{period}'" if period else \
            f"WHERE reporting_period = (SELECT MAX(reporting_period) FROM {fqn('pipeline_sla_status')})"
        rows = await execute_query(f"SELECT * FROM {fqn('pipeline_sla_status')} {where} ORDER BY sla_deadline")
        return {"data": rows}
    except Exception as exc:
        logger.exception("Failed to fetch SLA status")
        raise HTTPException(500, str(exc)) from exc


@router.get("/dq-summary")
async def get_dq_summary(period: str = Query(None)):
    """DQ expectation pass/fail rates by pipeline and table."""
    try:
        where = f"WHERE reporting_period = '{period}'" if period else \
            f"WHERE reporting_period = (SELECT MAX(reporting_period) FROM {fqn('dq_expectation_results')})"
        rows = await execute_query(f"""
            SELECT * FROM {fqn('dq_expectation_results')} {where}
            ORDER BY pipeline_name, table_name, expectation_name
        """)

        # Also compute aggregates
        agg = await execute_query(f"""
            SELECT
                SUM(total_records) AS total_records,
                SUM(passing_records) AS total_passing,
                SUM(failing_records) AS total_failing,
                ROUND(SUM(passing_records) * 100.0 / NULLIF(SUM(total_records), 0), 1) AS overall_pass_rate,
                COUNT(*) AS total_expectations,
                COUNT(CASE WHEN failing_records > 0 THEN 1 END) AS failing_expectations
            FROM {fqn('dq_expectation_results')} {where}
        """)

        return {"data": rows, "aggregate": agg[0] if agg else None}
    except Exception as exc:
        logger.exception("Failed to fetch DQ summary")
        raise HTTPException(500, str(exc)) from exc


@router.get("/dq-trends")
async def get_dq_trends():
    """DQ pass rate trend across all quarters."""
    try:
        rows = await execute_query(f"""
            SELECT
                reporting_period,
                SUM(total_records) AS total_records,
                SUM(passing_records) AS total_passing,
                SUM(failing_records) AS total_failing,
                ROUND(SUM(passing_records) * 100.0 / NULLIF(SUM(total_records), 0), 1) AS pass_rate_pct,
                COUNT(CASE WHEN failing_records > 0 THEN 1 END) AS failing_checks
            FROM {fqn('dq_expectation_results')}
            GROUP BY reporting_period
            ORDER BY reporting_period
        """)
        return {"data": rows}
    except Exception as exc:
        logger.exception("Failed to fetch DQ trends")
        raise HTTPException(500, str(exc)) from exc


@router.get("/reconciliation")
async def get_reconciliation(period: str = Query(None)):
    """Cross-QRT reconciliation checks."""
    try:
        where = f"WHERE reporting_period = '{period}'" if period else \
            f"WHERE reporting_period = (SELECT MAX(reporting_period) FROM {fqn('cross_qrt_reconciliation')})"
        rows = await execute_query(f"""
            SELECT * FROM {fqn('cross_qrt_reconciliation')} {where}
            ORDER BY check_name
        """)
        return {"data": rows}
    except Exception as exc:
        logger.exception("Failed to fetch reconciliation")
        raise HTTPException(500, str(exc)) from exc


@router.get("/model-versions")
async def get_model_versions(period: str = Query(None)):
    """Model version comparison — Champion vs Challenger."""
    try:
        where = f"WHERE reporting_period = '{period}'" if period else \
            f"WHERE reporting_period = (SELECT MAX(reporting_period) FROM {fqn('model_registry_log')})"
        rows = await execute_query(f"""
            SELECT * FROM {fqn('model_registry_log')} {where}
            ORDER BY model_version
        """)
        return {"data": rows}
    except Exception as exc:
        logger.exception("Failed to fetch model versions")
        raise HTTPException(500, str(exc)) from exc
