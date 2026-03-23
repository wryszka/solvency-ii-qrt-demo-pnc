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


# ── QRT Template Preview (EIOPA format) ──────────────────────────────────────

@router.get("/{qrt_id}/template")
async def get_template(qrt_id: str, period: str = Query(None)):
    """Return QRT data formatted for EIOPA template rendering."""
    if qrt_id not in QRT_DEFS:
        raise HTTPException(404, "Unknown QRT")

    try:
        latest = f"(SELECT MAX(reporting_period) FROM {fqn(QRT_DEFS[qrt_id]['table'])})"

        if qrt_id == "s0501":
            # Cross-tab: rows = template_row_id, columns = LoB
            rp = period or latest
            where = f"WHERE reporting_period = '{period}'" if period else f"WHERE reporting_period = {latest}"
            rows = await execute_query(f"""
                SELECT template_row_id, template_row_label, lob_code, lob_name,
                       CAST(amount_eur AS DOUBLE) AS amount_eur
                FROM {fqn('s0501_premiums_claims_expenses')} {where}
                ORDER BY template_row_id, lob_code
            """)
            # Get period
            p = period
            if not p and rows:
                p = rows[0].get("reporting_period", "")
            return {"qrt": "S.05.01", "title": "Non-Life — Premiums, Claims and Expenses by LoB",
                    "format": "crosstab", "period": p, "data": rows}

        elif qrt_id == "s2501":
            where = f"WHERE reporting_period = '{period}'" if period else f"WHERE reporting_period = {latest}"
            # SCR waterfall + solvency summary
            breakdown = await execute_query(f"""
                SELECT template_row_id, template_row_label,
                       CAST(amount_eur AS DOUBLE) AS amount_eur,
                       model_version
                FROM {fqn('s2501_scr_breakdown')} {where}
                ORDER BY template_row_id
            """)
            summary = await execute_query(f"""
                SELECT * FROM {fqn('s2501_summary')} {where}
            """)
            return {"qrt": "S.25.01", "title": "SCR — Standard Formula",
                    "format": "waterfall", "period": period,
                    "data": breakdown, "summary": summary[0] if summary else None}

        elif qrt_id == "s0602":
            where = f"WHERE reporting_period = '{period}'" if period else f"WHERE reporting_period = {latest}"
            summary = await execute_query(f"""
                SELECT * FROM {fqn('s0602_summary')} {where} ORDER BY cic_category_name
            """)
            count = await execute_query(f"""
                SELECT COUNT(*) AS cnt,
                       ROUND(SUM(CAST(C0170_Total_Solvency_II_Amount AS DOUBLE)), 2) AS total_sii
                FROM {fqn('s0602_list_of_assets')} {where}
            """)
            return {"qrt": "S.06.02", "title": "List of Assets",
                    "format": "summary", "period": period,
                    "data": summary,
                    "totals": count[0] if count else None}

    except Exception as exc:
        logger.exception("Failed to get template for %s", qrt_id)
        raise HTTPException(500, str(exc)) from exc


@router.get("/{qrt_id}/template-pdf")
async def get_template_pdf(qrt_id: str, period: str = Query(None)):
    """Generate a PDF rendering of the QRT in EIOPA template format."""
    if qrt_id not in QRT_DEFS:
        raise HTTPException(404, "Unknown QRT")

    try:
        from fpdf import FPDF

        template = await get_template(qrt_id, period)
        qrt_name = template["qrt"]
        qrt_title = template["title"]

        pdf = FPDF()
        pdf.add_page("L")  # landscape
        pdf.set_auto_page_break(auto=True, margin=15)

        # Header
        pdf.set_font("Helvetica", "B", 16)
        pdf.cell(0, 10, _safe(f"{qrt_name} - {qrt_title}"), new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.set_font("Helvetica", "", 11)
        pdf.cell(0, 8, _safe(f"Bricksurance SE | Period: {template.get('period', 'Latest')}"), new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.ln(5)

        if qrt_id == "s0501" and template.get("data"):
            _render_s0501_pdf(pdf, template["data"])
        elif qrt_id == "s2501":
            _render_s2501_pdf(pdf, template.get("data", []), template.get("summary"))
        elif qrt_id == "s0602":
            _render_s0602_pdf(pdf, template.get("data", []), template.get("totals"))

        # Footer
        pdf.ln(5)
        pdf.set_font("Helvetica", "I", 8)
        pdf.cell(0, 6, "Generated by Databricks Solvency II QRT Reporting System", align="C")

        pdf_bytes = pdf.output()

        return StreamingResponse(
            iter([bytes(pdf_bytes)]),
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={qrt_name.replace('.','')}_template.pdf"},
        )
    except Exception as exc:
        logger.exception("Failed to generate template PDF for %s", qrt_id)
        raise HTTPException(500, str(exc)) from exc


def _safe(text) -> str:
    """Replace non-ASCII characters for PDF Helvetica font."""
    return str(text).replace("\u2014", "-").replace("\u2013", "-").replace("\u2018", "'").replace("\u2019", "'").replace("\u201c", '"').replace("\u201d", '"').replace("\u2192", "->").replace("\u2194", "<->")


def _fmt(val) -> str:
    """Format a numeric value for PDF display."""
    if val is None:
        return ""
    try:
        n = float(val)
        if abs(n) >= 1e9:
            return f"{n/1e9:.2f}B"
        if abs(n) >= 1e6:
            return f"{n/1e6:.1f}M"
        if abs(n) >= 1e3:
            return f"{n/1e3:.0f}K"
        return f"{n:.0f}"
    except (ValueError, TypeError):
        return str(val)


def _render_s0501_pdf(pdf, rows):
    """Render S.05.01 as a cross-tab table."""
    # Build pivot: row_id → {lob_name: amount}
    from collections import OrderedDict
    lob_set = OrderedDict()
    row_map = OrderedDict()
    row_labels = {}

    for r in rows:
        lob = r.get("lob_name", "")
        rid = r.get("template_row_id", "")
        lob_set[lob] = True
        row_labels[rid] = r.get("template_row_label", rid)
        if rid not in row_map:
            row_map[rid] = {}
        row_map[rid][lob] = r.get("amount_eur")

    lobs = list(lob_set.keys())
    col_w = min(28, int((277 - 50) / max(len(lobs), 1)))

    pdf.set_font("Helvetica", "B", 7)
    pdf.cell(10, 6, "Row", border=1)
    pdf.cell(40, 6, "Description", border=1)
    for lob in lobs:
        label = _safe(lob[:12] if len(lob) > 12 else lob)
        pdf.cell(col_w, 6, label, border=1, align="C")
    pdf.ln()

    pdf.set_font("Helvetica", "", 7)
    for rid, values in row_map.items():
        is_total_row = rid in ("R0200", "R0300", "R0400", "R0500")
        if is_total_row:
            pdf.set_font("Helvetica", "B", 7)
        pdf.cell(10, 5, _safe(rid), border=1)
        pdf.cell(40, 5, _safe((row_labels.get(rid, ""))[:25]), border=1)
        for lob in lobs:
            v = values.get(lob)
            pdf.cell(col_w, 5, _fmt(v), border=1, align="R")
        pdf.ln()
        if is_total_row:
            pdf.set_font("Helvetica", "", 7)


def _render_s2501_pdf(pdf, breakdown, summary):
    """Render S.25.01 as a waterfall table with solvency summary."""
    # Main SCR components
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(30, 7, "Row", border=1)
    pdf.cell(100, 7, "Component", border=1)
    pdf.cell(50, 7, "Amount (EUR)", border=1, align="C")
    pdf.ln()

    pdf.set_font("Helvetica", "", 9)
    main_rows = [r for r in breakdown if "." not in r.get("template_row_id", "")]
    for r in main_rows:
        rid = r.get("template_row_id", "")
        is_key = rid in ("R0100", "R0200")
        if is_key:
            pdf.set_font("Helvetica", "B", 9)
        pdf.cell(30, 6, _safe(rid), border=1)
        pdf.cell(100, 6, _safe(r.get("template_row_label", "")), border=1)
        pdf.cell(50, 6, _fmt(r.get("amount_eur")), border=1, align="R")
        pdf.ln()
        if is_key:
            pdf.set_font("Helvetica", "", 9)

    # Solvency summary
    if summary:
        pdf.ln(5)
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 8, "Solvency Position", new_x="LMARGIN", new_y="NEXT")

        pdf.set_font("Helvetica", "", 10)
        pairs = [
            ("SCR", _fmt(summary.get("scr_eur"))),
            ("MCR", _fmt(summary.get("mcr_eur"))),
            ("Eligible Own Funds", _fmt(summary.get("eligible_own_funds_eur"))),
            ("Solvency Ratio", f"{summary.get('solvency_ratio_pct', '')}%"),
            ("Surplus", _fmt(summary.get("surplus_eur"))),
        ]
        for label, val in pairs:
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(60, 7, _safe(label + ":"), border=0)
            pdf.set_font("Helvetica", "", 10)
            pdf.cell(60, 7, _safe(str(val)), new_x="LMARGIN", new_y="NEXT")


def _render_s0602_pdf(pdf, summary_rows, totals):
    """Render S.06.02 as a summary by CIC category."""
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(60, 7, "CIC Category", border=1)
    pdf.cell(30, 7, "Assets", border=1, align="C")
    pdf.cell(50, 7, "Total SII (EUR)", border=1, align="C")
    pdf.cell(30, 7, "% of Total", border=1, align="C")
    pdf.cell(30, 7, "Inv. Grade", border=1, align="C")
    pdf.cell(30, 7, "Avg Duration", border=1, align="C")
    pdf.ln()

    pdf.set_font("Helvetica", "", 9)
    for r in summary_rows:
        pdf.cell(60, 6, _safe(r.get("cic_category_name", "")), border=1)
        pdf.cell(30, 6, str(r.get("asset_count", "")), border=1, align="R")
        pdf.cell(50, 6, _fmt(r.get("total_sii_amount")), border=1, align="R")
        pdf.cell(30, 6, f"{r.get('pct_of_total_sii', '')}%", border=1, align="R")
        pdf.cell(30, 6, str(r.get("investment_grade_count", "")), border=1, align="R")
        dur = r.get("avg_duration")
        pdf.cell(30, 6, f"{float(dur):.1f}" if dur else "", border=1, align="R")
        pdf.ln()

    if totals:
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(60, 6, "TOTAL", border=1)
        pdf.cell(30, 6, str(totals.get("cnt", "")), border=1, align="R")
        pdf.cell(50, 6, _fmt(totals.get("total_sii")), border=1, align="R")
        pdf.cell(30, 6, "100.0%", border=1, align="R")
        pdf.cell(30, 6, "", border=1)
        pdf.cell(30, 6, "", border=1)
        pdf.ln()


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
