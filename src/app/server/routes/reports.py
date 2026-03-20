import csv
import io
import logging

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from server.config import fqn
from server.sql import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/reports", tags=["reports"])

# ── QRT definitions ──────────────────────────────────────────────────────────

QRT_DEFS = {
    "s0602": {
        "id": "s0602",
        "name": "S.06.02",
        "title": "List of Assets",
        "table": "s0602_list_of_assets",
        "summary_table": "s0602_summary",
        "pipeline": "S.06.02 List of Assets",
        "lineage": [
            {"step": 1, "source": "assets", "target": "assets_enriched", "layer": "Silver",
             "description": "CIC decomposition, SII valuation, credit quality mapping"},
            {"step": 2, "source": "assets_enriched", "target": "s0602_list_of_assets", "layer": "Gold",
             "description": "Map to EIOPA S.06.02 cell references (C0040-C0370)"},
            {"step": 3, "source": "s0602_list_of_assets", "target": "s0602_summary", "layer": "Gold",
             "description": "Totals by CIC category for actuarial sign-off"},
        ],
    },
    "s0501": {
        "id": "s0501",
        "name": "S.05.01",
        "title": "Premiums, Claims & Expenses",
        "table": "s0501_premiums_claims_expenses",
        "summary_table": "s0501_summary",
        "pipeline": "S.05.01 Premiums, Claims & Expenses",
        "lineage": [
            {"step": 1, "source": "premiums", "target": "premiums_by_lob", "layer": "Silver",
             "description": "Aggregate premiums by LoB and quarter"},
            {"step": 2, "source": "claims", "target": "claims_by_lob", "layer": "Silver",
             "description": "Aggregate claims by LoB and quarter"},
            {"step": 3, "source": "expenses", "target": "expenses_by_lob", "layer": "Silver",
             "description": "Expense allocation with component validation"},
            {"step": 4, "source": "premiums_by_lob, claims_by_lob, expenses_by_lob",
             "target": "s0501_premiums_claims_expenses", "layer": "Gold",
             "description": "Map to EIOPA S.05.01 template rows (R0110-R1200)"},
            {"step": 5, "source": "s0501_premiums_claims_expenses",
             "target": "s0501_summary", "layer": "Gold",
             "description": "Loss/expense/combined ratios for actuarial sign-off"},
        ],
    },
    "s2501": {
        "id": "s2501",
        "name": "S.25.01",
        "title": "SCR Standard Formula",
        "table": "s2501_scr_breakdown",
        "summary_table": "s2501_summary",
        "pipeline": "S.25.01 SCR Standard Formula",
        "lineage": [
            {"step": 1, "source": "risk_factors", "target": "scr_results", "layer": "Model",
             "description": "Run Standard Formula model (Champion) from Unity Catalog"},
            {"step": 2, "source": "scr_results", "target": "s2501_scr_breakdown", "layer": "Gold",
             "description": "Map to EIOPA S.25.01 template rows (R0010-R0200)"},
            {"step": 3, "source": "s2501_scr_breakdown, own_funds",
             "target": "s2501_summary", "layer": "Gold",
             "description": "Solvency ratio = Eligible Own Funds / SCR"},
        ],
    },
}


def _rows_to_csv(rows: list[dict]) -> str:
    if not rows:
        return ""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


# ── List all QRTs ────────────────────────────────────────────────────────────

@router.get("")
async def list_reports():
    """List all QRT reports with status and key metrics."""
    results = []

    for qrt_id, defn in QRT_DEFS.items():
        try:
            info = {"id": qrt_id, "name": defn["name"], "title": defn["title"]}

            if qrt_id == "s0602":
                rows = await execute_query(f"""
                    SELECT reporting_period,
                           COUNT(*) AS row_count,
                           ROUND(SUM(CAST(C0170_Total_Solvency_II_Amount AS DOUBLE))/1e6, 1) AS total_sii_meur
                    FROM {fqn('s0602_list_of_assets')}
                    GROUP BY reporting_period ORDER BY reporting_period DESC LIMIT 1
                """)
                if rows:
                    info["period"] = rows[0]["reporting_period"]
                    info["row_count"] = rows[0]["row_count"]
                    info["metric_label"] = "Total SII"
                    info["metric_value"] = f"EUR {rows[0]['total_sii_meur']}M"

            elif qrt_id == "s0501":
                rows = await execute_query(f"""
                    SELECT reporting_period, COUNT(DISTINCT template_row_id) AS row_count,
                           ROUND(SUM(CASE WHEN template_row_id='R0110' AND lob_name='Total' THEN CAST(amount_eur AS DOUBLE) END)/1e6, 1) AS gwp_meur
                    FROM {fqn('s0501_premiums_claims_expenses')}
                    GROUP BY reporting_period ORDER BY reporting_period DESC LIMIT 1
                """)
                if rows:
                    info["period"] = rows[0]["reporting_period"]
                    info["row_count"] = rows[0]["row_count"]
                    info["metric_label"] = "GWP"
                    info["metric_value"] = f"EUR {rows[0]['gwp_meur']}M"

            elif qrt_id == "s2501":
                rows = await execute_query(f"""
                    SELECT reporting_period,
                           ROUND(scr_eur/1e6, 1) AS scr_meur,
                           solvency_ratio_pct
                    FROM {fqn('s2501_summary')}
                    ORDER BY reporting_period DESC LIMIT 1
                """)
                if rows:
                    info["period"] = rows[0]["reporting_period"]
                    info["row_count"] = "9 modules"
                    info["metric_label"] = "Solvency Ratio"
                    info["metric_value"] = f"{rows[0]['solvency_ratio_pct']}%"
                    info["scr"] = f"EUR {rows[0]['scr_meur']}M"

            # Get approval status for this QRT
            try:
                approval_rows = await execute_query(f"""
                    SELECT status, reviewed_at, reviewed_by, reporting_period AS appr_period
                    FROM {fqn('qrt_approvals')}
                    WHERE qrt_id = '{qrt_id}'
                    ORDER BY submitted_at DESC LIMIT 1
                """)
                if approval_rows:
                    info["approval_status"] = approval_rows[0]["status"]
                else:
                    info["approval_status"] = "draft"
            except Exception:
                info["approval_status"] = "draft"

            results.append(info)
        except Exception as e:
            logger.warning("Failed to load %s: %s", qrt_id, e)
            results.append({
                "id": qrt_id, "name": defn["name"], "title": defn["title"],
                "approval_status": "draft", "error": str(e),
            })

    return {"data": results}


# ── Report content ───────────────────────────────────────────────────────────

@router.get("/{qrt_id}/content")
async def get_content(
    qrt_id: str,
    period: str = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
):
    if qrt_id not in QRT_DEFS:
        raise HTTPException(404, "Unknown QRT")
    defn = QRT_DEFS[qrt_id]

    try:
        period_filter = f"WHERE reporting_period = '{period}'" if period else ""
        latest_period = ""
        if not period:
            latest_period = f"WHERE reporting_period = (SELECT MAX(reporting_period) FROM {fqn(defn['table'])})"

        where = period_filter or latest_period

        if qrt_id == "s0602":
            count_rows = await execute_query(
                f"SELECT COUNT(*) AS cnt FROM {fqn(defn['table'])} {where}"
            )
            total = int(count_rows[0]["cnt"] or 0)
            offset = (page - 1) * page_size
            rows = await execute_query(
                f"SELECT * FROM {fqn(defn['table'])} {where} ORDER BY C0040_Asset_ID LIMIT {page_size} OFFSET {offset}"
            )
            return {"data": rows, "total": total, "page": page, "page_size": page_size}

        elif qrt_id == "s0501":
            rows = await execute_query(
                f"SELECT * FROM {fqn(defn['table'])} {where} ORDER BY template_row_id, lob_code"
            )
            return {"data": rows}

        elif qrt_id == "s2501":
            rows = await execute_query(
                f"SELECT * FROM {fqn(defn['table'])} {where} ORDER BY template_row_id"
            )
            return {"data": rows}

    except Exception as exc:
        logger.exception("Failed to fetch %s content", qrt_id)
        raise HTTPException(500, str(exc)) from exc


# ── Data quality checks ──────────────────────────────────────────────────────

@router.get("/{qrt_id}/quality")
async def get_quality(qrt_id: str, period: str = Query(None)):
    if qrt_id not in QRT_DEFS:
        raise HTTPException(404, "Unknown QRT")

    try:
        checks = []

        if qrt_id == "s0602":
            table = fqn("s0602_list_of_assets")
            pw = f"AND reporting_period = '{period}'" if period else ""

            total_r = await execute_query(f"SELECT COUNT(*) AS c FROM {table} WHERE 1=1 {pw}")
            total = int(total_r[0]["c"])

            null_id = await execute_query(f"SELECT COUNT(*) AS c FROM {table} WHERE C0040_Asset_ID IS NULL {pw}")
            checks.append({"check": "Asset ID not null", "constraint": "C0040_Asset_ID IS NOT NULL",
                           "total": total, "failing": int(null_id[0]["c"]), "severity": "DROP ROW"})

            neg_sii = await execute_query(f"SELECT COUNT(*) AS c FROM {table} WHERE CAST(C0170_Total_Solvency_II_Amount AS DOUBLE) <= 0 {pw}")
            checks.append({"check": "SII value positive", "constraint": "C0170 > 0",
                           "total": total, "failing": int(neg_sii[0]["c"]), "severity": "DROP ROW"})

            bad_cic = await execute_query(f"SELECT COUNT(*) AS c FROM {table} WHERE LENGTH(C0270_CIC) != 4 {pw}")
            checks.append({"check": "CIC code valid (4 chars)", "constraint": "LENGTH(C0270_CIC) = 4",
                           "total": total, "failing": int(bad_cic[0]["c"]), "severity": "DROP ROW"})

            null_ccy = await execute_query(f"SELECT COUNT(*) AS c FROM {table} WHERE C0260_Currency IS NULL {pw}")
            checks.append({"check": "Currency not null", "constraint": "C0260_Currency IS NOT NULL",
                           "total": total, "failing": int(null_ccy[0]["c"]), "severity": "DROP ROW"})

        elif qrt_id == "s0501":
            table = fqn("s0501_premiums_claims_expenses")
            pw = f"AND reporting_period = '{period}'" if period else ""

            total_r = await execute_query(f"SELECT COUNT(*) AS c FROM {table} WHERE 1=1 {pw}")
            total = int(total_r[0]["c"])

            null_amt = await execute_query(f"SELECT COUNT(*) AS c FROM {table} WHERE amount_eur IS NULL {pw}")
            checks.append({"check": "Amount not null", "constraint": "amount_eur IS NOT NULL",
                           "total": total, "failing": int(null_amt[0]["c"]), "severity": "DROP ROW"})

            null_row = await execute_query(f"SELECT COUNT(*) AS c FROM {table} WHERE template_row_id IS NULL {pw}")
            checks.append({"check": "Template row ID present", "constraint": "template_row_id IS NOT NULL",
                           "total": total, "failing": int(null_row[0]["c"]), "severity": "DROP ROW"})

            # Check summary ratios
            summary = fqn("s0501_summary")
            bad_ratio = await execute_query(f"SELECT COUNT(*) AS c FROM {summary} WHERE combined_ratio_pct NOT BETWEEN 50 AND 200")
            total_summary = await execute_query(f"SELECT COUNT(*) AS c FROM {summary}")
            checks.append({"check": "Combined ratio realistic (50-200%)", "constraint": "combined_ratio BETWEEN 50 AND 200",
                           "total": int(total_summary[0]["c"]), "failing": int(bad_ratio[0]["c"]), "severity": "DROP ROW"})

        elif qrt_id == "s2501":
            summary = fqn("s2501_summary")

            total_r = await execute_query(f"SELECT COUNT(*) AS c FROM {summary}")
            total = int(total_r[0]["c"])

            neg_scr = await execute_query(f"SELECT COUNT(*) AS c FROM {summary} WHERE scr_eur <= 0")
            checks.append({"check": "SCR positive", "constraint": "scr_eur > 0",
                           "total": total, "failing": int(neg_scr[0]["c"]), "severity": "FAIL UPDATE"})

            low_ratio = await execute_query(f"SELECT COUNT(*) AS c FROM {summary} WHERE solvency_ratio_pct <= 0")
            checks.append({"check": "Solvency ratio positive", "constraint": "solvency_ratio_pct > 0",
                           "total": total, "failing": int(low_ratio[0]["c"]), "severity": "FAIL UPDATE"})

            # Model version check
            model_r = await execute_query(f"SELECT DISTINCT model_version FROM {summary}")
            checks.append({"check": "Model version consistent", "constraint": "Single model version used",
                           "total": total, "failing": 0 if len(model_r) <= 1 else len(model_r) - 1,
                           "severity": "WARNING"})

        for c in checks:
            c["passing"] = c["total"] - c["failing"]
            c["status"] = "PASS" if c["failing"] == 0 else "FAIL"

        return {"data": checks}
    except Exception as exc:
        logger.exception("Failed DQ checks for %s", qrt_id)
        raise HTTPException(500, str(exc)) from exc


# ── Comparison across periods ────────────────────────────────────────────────

@router.get("/{qrt_id}/comparison")
async def get_comparison(qrt_id: str):
    if qrt_id not in QRT_DEFS:
        raise HTTPException(404, "Unknown QRT")
    defn = QRT_DEFS[qrt_id]

    try:
        rows = await execute_query(
            f"SELECT * FROM {fqn(defn['summary_table'])} ORDER BY reporting_period"
        )
        return {"data": rows}
    except Exception as exc:
        logger.exception("Failed comparison for %s", qrt_id)
        raise HTTPException(500, str(exc)) from exc


# ── Lineage ──────────────────────────────────────────────────────────────────

@router.get("/{qrt_id}/lineage")
async def get_lineage(qrt_id: str):
    if qrt_id not in QRT_DEFS:
        raise HTTPException(404, "Unknown QRT")
    return {"data": QRT_DEFS[qrt_id]["lineage"]}


# ── CSV download ─────────────────────────────────────────────────────────────

@router.get("/{qrt_id}/csv")
async def download_csv(qrt_id: str, period: str = Query(None)):
    if qrt_id not in QRT_DEFS:
        raise HTTPException(404, "Unknown QRT")
    defn = QRT_DEFS[qrt_id]

    try:
        where = f"WHERE reporting_period = '{period}'" if period else ""
        rows = await execute_query(f"SELECT * FROM {fqn(defn['table'])} {where}")
        csv_text = _rows_to_csv(rows)
        return StreamingResponse(
            iter([csv_text]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={defn['table']}.csv"},
        )
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


# ── Available periods ────────────────────────────────────────────────────────

@router.get("/{qrt_id}/periods")
async def get_periods(qrt_id: str):
    if qrt_id not in QRT_DEFS:
        raise HTTPException(404, "Unknown QRT")
    defn = QRT_DEFS[qrt_id]
    try:
        rows = await execute_query(
            f"SELECT DISTINCT reporting_period FROM {fqn(defn['table'])} ORDER BY reporting_period DESC"
        )
        return {"data": [r["reporting_period"] for r in rows]}
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc
