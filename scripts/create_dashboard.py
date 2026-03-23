#!/usr/bin/env python3
"""Create the Solvency II QRT Comparison Lakeview Dashboard."""

import json
import subprocess
import uuid
import sys

CATALOG = "lr_serverless_aws_us_catalog"
SCHEMA = "solvency2demo"
WAREHOUSE_ID = "c80acfa212bf1166"
PROFILE = "DEFAULT"
FQN = f"{CATALOG}.{SCHEMA}"

# If updating existing dashboard, pass ID as argument
DASHBOARD_ID = sys.argv[1] if len(sys.argv) > 1 else None


def uid():
    return uuid.uuid4().hex[:8]


# ── Datasets ──────────────────────────────────────────────────────────
# All math (division, rounding) happens here in SQL — widget expressions
# must be simple column references or basic aggregations like SUM(`col`).

datasets = []


def ds(name, display, sql):
    # queryLines must be a single-element array with the full query on one line
    # Multi-line SQL with newlines causes Lakeview to fail silently
    oneline = " ".join(line.strip() for line in sql.strip().splitlines())
    datasets.append({
        "name": name,
        "displayName": display,
        "queryLines": [oneline],
    })
    return name


# Overview
ds_kpi_latest = ds("ds_kpi_latest", "Latest KPIs",
    f"""SELECT reporting_period,
           solvency_ratio_pct,
           ROUND(scr_eur / 1e6, 1) AS scr_m,
           ROUND(eligible_own_funds_eur / 1e6, 1) AS eof_m,
           ROUND(surplus_eur / 1e6, 1) AS surplus_m
    FROM {FQN}.s2501_summary
    WHERE reporting_period = (SELECT MAX(reporting_period) FROM {FQN}.s2501_summary)""")

ds_solvency_trend = ds("ds_solvency_trend", "Solvency Ratio Trend",
    f"""SELECT reporting_period,
           solvency_ratio_pct,
           ROUND(scr_eur / 1e6, 1) AS scr_m,
           ROUND(eligible_own_funds_eur / 1e6, 1) AS eof_m
    FROM {FQN}.s2501_summary ORDER BY reporting_period""")

ds_scr_vs_eof = ds("ds_scr_eof", "SCR vs EOF",
    f"""SELECT reporting_period, 'SCR' AS metric, ROUND(scr_eur / 1e6, 1) AS value_m
    FROM {FQN}.s2501_summary
    UNION ALL
    SELECT reporting_period, 'Eligible Own Funds' AS metric, ROUND(eligible_own_funds_eur / 1e6, 1) AS value_m
    FROM {FQN}.s2501_summary
    ORDER BY reporting_period, metric""")

ds_balance = ds("ds_balance", "Balance Sheet",
    f"""SELECT reporting_period, item, category,
           ROUND(amount_eur / 1e6, 1) AS amount_m
    FROM {FQN}.balance_sheet
    WHERE category = 'assets'
    ORDER BY reporting_period""")

# S.06.02
ds_asset_alloc = ds("ds_asset_alloc", "Asset Allocation by Quarter",
    f"""SELECT reporting_period, cic_category_name,
           asset_count,
           ROUND(total_sii_amount / 1e6, 1) AS sii_m,
           pct_of_total_sii
    FROM {FQN}.s0602_summary ORDER BY reporting_period, cic_category_name""")

ds_asset_quality = ds("ds_asset_quality", "Asset Credit Quality",
    f"""SELECT reporting_period,
           CASE WHEN credit_quality_step <= 2 THEN 'Investment Grade (CQS 0-2)'
                WHEN credit_quality_step <= 4 THEN 'Sub-Investment Grade (CQS 3-4)'
                WHEN credit_quality_step <= 6 THEN 'High Yield / NR (CQS 5-6)'
                ELSE 'Unrated' END AS quality_band,
           COUNT(*) AS asset_count,
           ROUND(SUM(sii_value) / 1e6, 1) AS sii_m
    FROM {FQN}.assets_enriched
    GROUP BY 1, 2 ORDER BY 1""")

ds_asset_duration = ds("ds_asset_duration", "Duration Distribution",
    f"""SELECT reporting_period, asset_class,
           ROUND(AVG(modified_duration), 2) AS avg_duration
    FROM {FQN}.assets_enriched
    WHERE modified_duration IS NOT NULL
    GROUP BY 1, 2 ORDER BY 1""")

ds_asset_country = ds("ds_asset_country", "Assets by Issuer Country",
    f"""SELECT issuer_country,
           COUNT(*) AS asset_count,
           ROUND(SUM(sii_value) / 1e6, 1) AS sii_m
    FROM {FQN}.assets_enriched
    GROUP BY 1 ORDER BY sii_m DESC""")

# S.05.01
ds_combined = ds("ds_combined", "Combined Ratios by LoB",
    f"""SELECT reporting_period, lob_code, lob_name,
           combined_ratio_pct, loss_ratio_pct, expense_ratio_pct,
           ri_cession_rate_pct,
           ROUND(gross_written_premium / 1e6, 1) AS gwp_m,
           ROUND(net_earned_premium / 1e6, 1) AS nep_m
    FROM {FQN}.s0501_summary ORDER BY reporting_period, lob_code""")

ds_pnl_totals = ds("ds_pnl_totals", "P&L Totals by Quarter",
    f"""SELECT reporting_period, template_row_label,
           ROUND(amount_eur / 1e6, 1) AS amount_m
    FROM {FQN}.s0501_premiums_claims_expenses
    WHERE lob_code = 0
      AND template_row_id IN ('R0110', 'R0200', 'R0310', 'R0400', 'R0550')
    ORDER BY reporting_period, template_row_id""")

ds_gwp_by_lob = ds("ds_gwp_by_lob", "GWP by LoB & Quarter",
    f"""SELECT reporting_period, lob_name,
           ROUND(amount_eur / 1e6, 1) AS gwp_m
    FROM {FQN}.s0501_premiums_claims_expenses
    WHERE template_row_id = 'R0110' AND lob_code > 0
    ORDER BY reporting_period, lob_name""")

# S.25.01
ds_scr_modules = ds("ds_scr_modules", "SCR Risk Modules",
    f"""SELECT reporting_period, template_row_id, template_row_label,
           ROUND(amount_eur / 1e6, 1) AS amount_m
    FROM {FQN}.s2501_scr_breakdown
    WHERE template_row_id IN ('R0010','R0020','R0030','R0040','R0050','R0100','R0130','R0150','R0200')
    ORDER BY reporting_period, template_row_id""")

ds_scr_market = ds("ds_scr_market", "Market Risk Sub-modules",
    f"""SELECT reporting_period, template_row_label,
           ROUND(amount_eur / 1e6, 1) AS amount_m
    FROM {FQN}.s2501_scr_breakdown
    WHERE template_row_id LIKE 'R0010.%'
    ORDER BY reporting_period, template_row_id""")

ds_scr_nl = ds("ds_scr_nl", "Non-Life UW Sub-modules",
    f"""SELECT reporting_period, template_row_label,
           ROUND(amount_eur / 1e6, 1) AS amount_m
    FROM {FQN}.s2501_scr_breakdown
    WHERE template_row_id LIKE 'R0050.%'
    ORDER BY reporting_period, template_row_id""")

ds_own_funds = ds("ds_own_funds", "Own Funds by Tier",
    f"""SELECT reporting_period,
           CONCAT('Tier ', tier) AS tier_label,
           ROUND(SUM(amount_eur) / 1e6, 1) AS amount_m
    FROM {FQN}.own_funds
    GROUP BY reporting_period, tier
    ORDER BY reporting_period, tier""")

ds_solvency = ds("ds_solvency", "Solvency Summary Table",
    f"""SELECT reporting_period, model_version,
           solvency_ratio_pct,
           ROUND(scr_eur / 1e6, 1) AS scr_m,
           ROUND(bscr_eur / 1e6, 1) AS bscr_m,
           ROUND(eligible_own_funds_eur / 1e6, 1) AS eof_m,
           ROUND(surplus_eur / 1e6, 1) AS surplus_m
    FROM {FQN}.s2501_summary ORDER BY reporting_period""")


# ── Widget builders ───────────────────────────────────────────────────
# All widget field expressions are simple column references now.

def counter_widget(dataset, field, title):
    wid = uid()
    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": [{"name": field, "expression": f"`{field}`"}],
            "disaggregated": True,
        }}],
        "spec": {
            "version": 2,
            "widgetType": "counter",
            "encodings": {
                "value": {"fieldName": field, "displayName": title},
            },
            "frame": {"showTitle": True, "title": title},
        },
    }


def bar_widget(dataset, x_field, y_field, title, color_field=None, stacked=False, label=False, sort=None):
    wid = uid()
    y_name = f"sum_{y_field}"
    fields = [
        {"name": x_field, "expression": f"`{x_field}`"},
        {"name": y_name, "expression": f"SUM(`{y_field}`)"},
    ]
    if color_field:
        fields.append({"name": color_field, "expression": f"`{color_field}`"})

    enc = {
        "x": {"fieldName": x_field, "scale": {"type": "categorical"}, "displayName": x_field},
        "y": {"fieldName": y_name, "scale": {"type": "quantitative"}, "displayName": y_field},
    }
    if sort:
        enc["x"]["scale"]["sort"] = {"by": sort}
    if color_field:
        enc["color"] = {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": color_field}
    if label:
        enc["label"] = {"show": True}
    if stacked:
        enc["y"]["scale"]["stackMode"] = "stacked"

    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": fields,
            "disaggregated": False,
        }}],
        "spec": {"version": 3, "widgetType": "bar", "encodings": enc,
                 "frame": {"showTitle": True, "title": title}},
    }


def line_widget(dataset, x_field, y_field, title, color_field=None):
    wid = uid()
    y_name = f"sum_{y_field}"
    fields = [
        {"name": x_field, "expression": f"`{x_field}`"},
        {"name": y_name, "expression": f"SUM(`{y_field}`)"},
    ]
    enc = {
        "x": {"fieldName": x_field, "scale": {"type": "categorical"}, "displayName": x_field},
        "y": {"fieldName": y_name, "scale": {"type": "quantitative"}, "displayName": y_field},
    }
    if color_field:
        fields.append({"name": color_field, "expression": f"`{color_field}`"})
        enc["color"] = {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": color_field}

    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": fields,
            "disaggregated": False,
        }}],
        "spec": {"version": 3, "widgetType": "line", "encodings": enc,
                 "frame": {"showTitle": True, "title": title}},
    }


def area_widget(dataset, x_field, y_field, title, color_field=None):
    wid = uid()
    y_name = f"sum_{y_field}"
    fields = [
        {"name": x_field, "expression": f"`{x_field}`"},
        {"name": y_name, "expression": f"SUM(`{y_field}`)"},
    ]
    enc = {
        "x": {"fieldName": x_field, "scale": {"type": "categorical"}, "displayName": x_field},
        "y": {"fieldName": y_name, "scale": {"type": "quantitative"}, "displayName": y_field},
    }
    if color_field:
        fields.append({"name": color_field, "expression": f"`{color_field}`"})
        enc["color"] = {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": color_field}

    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": fields,
            "disaggregated": False,
        }}],
        "spec": {"version": 3, "widgetType": "area", "encodings": enc,
                 "frame": {"showTitle": True, "title": title}},
    }


def pie_widget(dataset, angle_field, color_field, title):
    wid = uid()
    angle_name = f"sum_{angle_field}"
    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": [
                {"name": angle_name, "expression": f"SUM(`{angle_field}`)"},
                {"name": color_field, "expression": f"`{color_field}`"},
            ],
            "disaggregated": False,
        }}],
        "spec": {"version": 3, "widgetType": "pie",
                 "encodings": {
                     "angle": {"fieldName": angle_name, "scale": {"type": "quantitative"}, "displayName": angle_field},
                     "color": {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": color_field},
                 },
                 "frame": {"showTitle": True, "title": title}},
    }


def heatmap_widget(dataset, x_field, y_field, color_field, title):
    wid = uid()
    color_name = f"sum_{color_field}"
    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": [
                {"name": x_field, "expression": f"`{x_field}`"},
                {"name": y_field, "expression": f"`{y_field}`"},
                {"name": color_name, "expression": f"SUM(`{color_field}`)"},
            ],
            "disaggregated": False,
        }}],
        "spec": {"version": 3, "widgetType": "heatmap",
                 "encodings": {
                     "x": {"fieldName": x_field, "scale": {"type": "categorical"}, "displayName": x_field},
                     "y": {"fieldName": y_field, "scale": {"type": "categorical"}, "displayName": y_field},
                     "color": {"fieldName": color_name, "scale": {"type": "quantitative"}, "displayName": color_field},
                 },
                 "frame": {"showTitle": True, "title": title}},
    }


def table_widget(dataset, columns, title):
    """columns: list of (field, title, type, format)"""
    wid = uid()
    fields = [{"name": c[0], "expression": f"`{c[0]}`"} for c in columns]
    col_specs = []
    for c in columns:
        spec = {"fieldName": c[0], "title": c[1], "type": "string", "displayAs": "string"}
        if len(c) > 2 and c[2] == "number":
            spec["type"] = "float"
            spec["displayAs"] = "number"
            spec["alignContent"] = "right"
            if len(c) > 3:
                spec["numberFormat"] = c[3]
        col_specs.append(spec)

    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": fields,
            "disaggregated": True,
        }}],
        "spec": {"version": 1, "widgetType": "table",
                 "encodings": {"columns": col_specs},
                 "frame": {"showTitle": True, "title": title}},
    }


def md_widget(text):
    wid = uid()
    return {"name": wid, "textbox_spec": text}


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def lay(widget, position):
    return {"widget": widget, "position": position}


# ── Page 1: Overview ──────────────────────────────────────────────────

overview_layout = [
    lay(md_widget("# Bricksurance SE — Solvency II QRT Dashboard\nQuarterly comparison of regulatory reporting templates across Q1–Q3 2025."),
        pos(0, 0, 6, 1)),

    # KPI counters
    lay(counter_widget(ds_kpi_latest, "solvency_ratio_pct", "Solvency Ratio %"),
        pos(0, 1, 1, 2)),
    lay(counter_widget(ds_kpi_latest, "scr_m", "SCR (EUR m)"),
        pos(1, 1, 1, 2)),
    lay(counter_widget(ds_kpi_latest, "eof_m", "Eligible Own Funds (EUR m)"),
        pos(2, 1, 1, 2)),
    lay(counter_widget(ds_kpi_latest, "surplus_m", "Surplus (EUR m)"),
        pos(3, 1, 1, 2)),
    lay(counter_widget(ds_kpi_latest, "reporting_period", "Latest Period"),
        pos(4, 1, 2, 2)),

    # Solvency ratio trend
    lay(line_widget(ds_solvency_trend, "reporting_period", "solvency_ratio_pct",
                    "Solvency Ratio Trend (%)"),
        pos(0, 3, 3, 4)),

    # SCR vs Own Funds
    lay(bar_widget(ds_scr_vs_eof, "reporting_period", "value_m",
                   "SCR vs Eligible Own Funds (EUR m)", color_field="metric"),
        pos(3, 3, 3, 4)),

    # Balance sheet
    lay(bar_widget(ds_balance, "reporting_period", "amount_m",
                   "Asset Breakdown (EUR m)", color_field="item", stacked=True),
        pos(0, 7, 6, 4)),
]


# ── Page 2: S.06.02 Assets ───────────────────────────────────────────

s0602_layout = [
    lay(md_widget("# S.06.02 — List of Assets\nInvestment portfolio analysis across reporting periods."),
        pos(0, 0, 6, 1)),

    lay(bar_widget(ds_asset_alloc, "reporting_period", "sii_m",
                   "Asset Allocation by CIC Category (EUR m)", color_field="cic_category_name", stacked=True),
        pos(0, 1, 3, 4)),

    lay(pie_widget(ds_asset_alloc, "pct_of_total_sii", "cic_category_name",
                   "Asset Mix (% of SII)"),
        pos(3, 1, 3, 4)),

    lay(heatmap_widget(ds_asset_quality, "reporting_period", "quality_band", "sii_m",
                       "Credit Quality Distribution (EUR m)"),
        pos(0, 5, 3, 4)),

    lay(bar_widget(ds_asset_duration, "reporting_period", "avg_duration",
                   "Average Modified Duration by Asset Class", color_field="asset_class"),
        pos(3, 5, 3, 4)),

    lay(bar_widget(ds_asset_country, "issuer_country", "sii_m",
                   "Top Issuer Countries (EUR m)", sort="y-reversed", label=True),
        pos(0, 9, 3, 4)),

    lay(line_widget(ds_asset_alloc, "reporting_period", "asset_count",
                    "Number of Assets by Category", color_field="cic_category_name"),
        pos(3, 9, 3, 4)),
]


# ── Page 3: S.05.01 P&L ──────────────────────────────────────────────

s0501_layout = [
    lay(md_widget("# S.05.01 — Premiums, Claims & Expenses\nP&L analysis by line of business and reporting period."),
        pos(0, 0, 6, 1)),

    lay(heatmap_widget(ds_combined, "reporting_period", "lob_name", "combined_ratio_pct",
                       "Combined Ratio by LoB & Quarter (%)"),
        pos(0, 1, 6, 4)),

    lay(area_widget(ds_gwp_by_lob, "reporting_period", "gwp_m",
                    "Gross Written Premium by LoB (EUR m)", color_field="lob_name"),
        pos(0, 5, 3, 4)),

    lay(bar_widget(ds_combined, "lob_name", "loss_ratio_pct",
                   "Loss Ratio by LoB (%)", label=True),
        pos(3, 5, 3, 4)),

    lay(line_widget(ds_pnl_totals, "reporting_period", "amount_m",
                    "Key P&L Items Trend (EUR m)", color_field="template_row_label"),
        pos(0, 9, 3, 4)),

    lay(bar_widget(ds_combined, "lob_name", "ri_cession_rate_pct",
                   "Reinsurance Cession Rate by LoB (%)", color_field="reporting_period"),
        pos(3, 9, 3, 4)),

    lay(table_widget(ds_combined,
                     [("reporting_period", "Quarter"),
                      ("lob_name", "Line of Business"),
                      ("gwp_m", "GWP (EUR m)", "number", "#,##0.0"),
                      ("combined_ratio_pct", "Combined %", "number", "0.0"),
                      ("loss_ratio_pct", "Loss %", "number", "0.0"),
                      ("expense_ratio_pct", "Expense %", "number", "0.0"),
                      ("ri_cession_rate_pct", "RI Cession %", "number", "0.0")],
                     "S.05.01 Detail — All Quarters"),
        pos(0, 13, 6, 5)),
]


# ── Page 4: S.25.01 SCR ──────────────────────────────────────────────

s2501_layout = [
    lay(md_widget("# S.25.01 — SCR Standard Formula\nSolvency Capital Requirement breakdown and solvency position."),
        pos(0, 0, 6, 1)),

    lay(bar_widget(ds_scr_modules, "template_row_label", "amount_m",
                   "SCR Breakdown by Risk Module (EUR m)", color_field="reporting_period"),
        pos(0, 1, 6, 5)),

    lay(bar_widget(ds_scr_market, "template_row_label", "amount_m",
                   "Market Risk Sub-modules (EUR m)", color_field="reporting_period"),
        pos(0, 6, 3, 4)),

    lay(bar_widget(ds_scr_nl, "template_row_label", "amount_m",
                   "Non-Life UW Sub-modules (EUR m)", color_field="reporting_period"),
        pos(3, 6, 3, 4)),

    lay(bar_widget(ds_own_funds, "reporting_period", "amount_m",
                   "Own Funds by Tier (EUR m)", color_field="tier_label", stacked=True),
        pos(0, 10, 3, 4)),

    lay(line_widget(ds_solvency, "reporting_period", "solvency_ratio_pct",
                    "Solvency Ratio Trend (%)"),
        pos(3, 10, 3, 4)),

    lay(table_widget(ds_scr_modules,
                     [("reporting_period", "Quarter"),
                      ("template_row_id", "Row ID"),
                      ("template_row_label", "Risk Module"),
                      ("amount_m", "Amount (EUR m)", "number", "#,##0.0")],
                     "S.25.01 Detail — SCR Components"),
        pos(0, 14, 6, 5)),
]


# ── Page 5: Pipeline & DQ ─────────────────────────────────────────────

ds_sla = ds("ds_sla", "SLA Status",
    f"""SELECT feed_name, source_system, status, dq_pass_rate, row_count, notes FROM {FQN}.pipeline_sla_status WHERE reporting_period = (SELECT MAX(reporting_period) FROM {FQN}.pipeline_sla_status) ORDER BY feed_name""")

ds_dq_trend = ds("ds_dq_trend", "DQ Pass Rate Trend",
    f"""SELECT reporting_period, ROUND(SUM(passing_records) * 100.0 / SUM(total_records), 1) AS pass_rate, SUM(failing_records) AS quarantined FROM {FQN}.dq_expectation_results GROUP BY reporting_period ORDER BY reporting_period""")

ds_dq_by_pipeline = ds("ds_dq_pipeline", "DQ by Pipeline",
    f"""SELECT pipeline_name, SUM(total_records) AS total_records, SUM(failing_records) AS failing, ROUND(SUM(passing_records) * 100.0 / SUM(total_records), 1) AS pass_rate FROM {FQN}.dq_expectation_results WHERE reporting_period = (SELECT MAX(reporting_period) FROM {FQN}.dq_expectation_results) GROUP BY pipeline_name ORDER BY pipeline_name""")

ds_recon = ds("ds_recon", "Reconciliation Checks",
    f"""SELECT check_name, source_qrt, target_qrt, status, ROUND(source_value / 1e6, 1) AS source_m, ROUND(target_value / 1e6, 1) AS target_m, ROUND(difference / 1e6, 1) AS diff_m FROM {FQN}.cross_qrt_reconciliation WHERE reporting_period = (SELECT MAX(reporting_period) FROM {FQN}.cross_qrt_reconciliation) ORDER BY check_name""")

pipeline_dq_layout = [
    lay(md_widget("# Pipeline & Data Quality\nFeed arrival status, DLT expectation results, and cross-QRT reconciliation."),
        pos(0, 0, 6, 1)),

    # SLA status table
    lay(table_widget(ds_sla,
                     [("feed_name", "Feed"),
                      ("source_system", "Source System"),
                      ("status", "SLA Status"),
                      ("row_count", "Rows", "number", "#,##0"),
                      ("dq_pass_rate", "DQ Pass Rate", "number", "0.00%"),
                      ("notes", "Notes")],
                     "Data Feed SLA Status"),
        pos(0, 1, 6, 4)),

    # DQ trend
    lay(bar_widget(ds_dq_trend, "reporting_period", "pass_rate",
                   "DQ Pass Rate Trend (%)"),
        pos(0, 5, 3, 4)),

    # DQ by pipeline
    lay(bar_widget(ds_dq_by_pipeline, "pipeline_name", "pass_rate",
                   "Pass Rate by Pipeline (%)", label=True),
        pos(3, 5, 3, 4)),

    # Reconciliation
    lay(table_widget(ds_recon,
                     [("source_qrt", "Source QRT"),
                      ("target_qrt", "Target"),
                      ("source_m", "Source (EUR m)", "number", "#,##0.0"),
                      ("target_m", "Target (EUR m)", "number", "#,##0.0"),
                      ("diff_m", "Diff (EUR m)", "number", "#,##0.0"),
                      ("status", "Status")],
                     "Cross-QRT Reconciliation"),
        pos(0, 9, 6, 4)),
]


# ── Assemble dashboard ───────────────────────────────────────────────

serialized = {
    "datasets": datasets,
    "pages": [
        {"name": uid(), "displayName": "Overview",
         "pageType": "PAGE_TYPE_CANVAS", "layout": overview_layout},
        {"name": uid(), "displayName": "S.06.02 — Assets",
         "pageType": "PAGE_TYPE_CANVAS", "layout": s0602_layout},
        {"name": uid(), "displayName": "S.05.01 — P&L",
         "pageType": "PAGE_TYPE_CANVAS", "layout": s0501_layout},
        {"name": uid(), "displayName": "S.25.01 — SCR",
         "pageType": "PAGE_TYPE_CANVAS", "layout": s2501_layout},
        {"name": uid(), "displayName": "Pipeline & DQ",
         "pageType": "PAGE_TYPE_CANVAS", "layout": pipeline_dq_layout},
    ],
    "uiSettings": {
        "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
        "applyModeEnabled": False,
    },
}

# ── Deploy ────────────────────────────────────────────────────────────

serialized_json = json.dumps(serialized)

if DASHBOARD_ID:
    print(f"Updating dashboard {DASHBOARD_ID}...")
    payload = {"serialized_dashboard": serialized_json}
    result = subprocess.run(
        ["databricks", "api", "patch", f"/api/2.0/lakeview/dashboards/{DASHBOARD_ID}",
         "--profile", PROFILE, "--json", json.dumps(payload)],
        capture_output=True, text=True,
    )
else:
    print("Creating dashboard...")
    payload = {
        "display_name": "Solvency II QRT — Quarterly Comparison",
        "warehouse_id": WAREHOUSE_ID,
        "parent_path": "/Users/laurence.ryszka@databricks.com",
        "serialized_dashboard": serialized_json,
    }
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/lakeview/dashboards",
         "--profile", PROFILE, "--json", json.dumps(payload)],
        capture_output=True, text=True,
    )

if result.returncode != 0:
    print(f"Error: {result.stderr}")
    sys.exit(1)

resp = json.loads(result.stdout)
dashboard_id = resp.get("dashboard_id", DASHBOARD_ID)
print(f"Dashboard ID: {dashboard_id}")

# Publish
print("Publishing...")
subprocess.run(
    ["databricks", "api", "post",
     f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
     "--profile", PROFILE,
     "--json", json.dumps({"warehouse_id": WAREHOUSE_ID, "embed_credentials": True})],
    capture_output=True, text=True,
)

print(f"Done: https://fevm-lr-serverless-aws-us.cloud.databricks.com/dashboardsv3/{dashboard_id}")
