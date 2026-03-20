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


def uid():
    return uuid.uuid4().hex[:8]


# ── Datasets ──────────────────────────────────────────────────────────

datasets = []


def ds(name, display, sql):
    datasets.append({
        "name": name,
        "displayName": display,
        "queryLines": [sql],
    })
    return name


# Overview
ds_solvency = ds("ds_solvency", "Solvency Summary",
    f"SELECT reporting_period, solvency_ratio_pct, scr_eur, eligible_own_funds_eur, surplus_eur, bscr_eur, mcr_eur, tier1_eur, tier2_eur, tier3_eur, model_version FROM {FQN}.s2501_summary ORDER BY reporting_period")

ds_balance = ds("ds_balance", "Balance Sheet",
    f"SELECT reporting_period, item, category, amount_eur FROM {FQN}.balance_sheet ORDER BY reporting_period, category")

ds_kpi_latest = ds("ds_kpi_latest", "Latest KPIs",
    f"SELECT s.reporting_period, s.solvency_ratio_pct, s.scr_eur, s.eligible_own_funds_eur, s.surplus_eur, ROUND(s.scr_eur / 1e6, 1) as scr_m, ROUND(s.eligible_own_funds_eur / 1e6, 1) as eof_m, ROUND(s.surplus_eur / 1e6, 1) as surplus_m FROM {FQN}.s2501_summary s WHERE s.reporting_period = (SELECT MAX(reporting_period) FROM {FQN}.s2501_summary)")

# S.06.02
ds_asset_alloc = ds("ds_asset_alloc", "Asset Allocation by Quarter",
    f"SELECT reporting_period, cic_category_name, asset_count, total_sii_amount, pct_of_total_sii FROM {FQN}.s0602_summary ORDER BY reporting_period, cic_category_name")

ds_asset_quality = ds("ds_asset_quality", "Asset Credit Quality",
    f"SELECT reporting_period, CASE WHEN credit_quality_step <= 2 THEN 'Investment Grade (CQS 0-2)' WHEN credit_quality_step <= 4 THEN 'Sub-Investment Grade (CQS 3-4)' WHEN credit_quality_step <= 6 THEN 'High Yield / NR (CQS 5-6)' ELSE 'Unrated' END as quality_band, COUNT(*) as asset_count, ROUND(SUM(sii_value), 2) as total_sii FROM {FQN}.assets_enriched GROUP BY reporting_period, CASE WHEN credit_quality_step <= 2 THEN 'Investment Grade (CQS 0-2)' WHEN credit_quality_step <= 4 THEN 'Sub-Investment Grade (CQS 3-4)' WHEN credit_quality_step <= 6 THEN 'High Yield / NR (CQS 5-6)' ELSE 'Unrated' END ORDER BY reporting_period")

ds_asset_duration = ds("ds_asset_duration", "Duration Distribution",
    f"SELECT reporting_period, asset_class, ROUND(AVG(modified_duration), 2) as avg_duration, ROUND(SUM(sii_value), 2) as total_sii FROM {FQN}.assets_enriched WHERE modified_duration IS NOT NULL GROUP BY reporting_period, asset_class ORDER BY reporting_period")

ds_asset_country = ds("ds_asset_country", "Assets by Issuer Country",
    f"SELECT reporting_period, issuer_country, COUNT(*) as asset_count, ROUND(SUM(sii_value), 2) as total_sii FROM {FQN}.assets_enriched GROUP BY reporting_period, issuer_country ORDER BY total_sii DESC")

# S.05.01
ds_combined = ds("ds_combined", "Combined Ratios by LoB",
    f"SELECT reporting_period, lob_code, lob_name, combined_ratio_pct, loss_ratio_pct, expense_ratio_pct, ri_cession_rate_pct, ROUND(gross_written_premium, 2) as gwp, ROUND(net_earned_premium, 2) as nep FROM {FQN}.s0501_summary ORDER BY reporting_period, lob_code")

ds_pnl_totals = ds("ds_pnl_totals", "P&L Totals by Quarter",
    f"SELECT reporting_period, template_row_id, template_row_label, ROUND(SUM(amount_eur), 2) as amount_eur FROM {FQN}.s0501_premiums_claims_expenses WHERE lob_code = 0 AND template_row_id IN ('R0110', 'R0200', 'R0310', 'R0400', 'R0550') GROUP BY reporting_period, template_row_id, template_row_label ORDER BY reporting_period, template_row_id")

ds_gwp_by_lob = ds("ds_gwp_by_lob", "GWP by LoB & Quarter",
    f"SELECT reporting_period, lob_name, amount_eur FROM {FQN}.s0501_premiums_claims_expenses WHERE template_row_id = 'R0110' AND lob_code > 0 ORDER BY reporting_period, lob_name")

# S.25.01
ds_scr_modules = ds("ds_scr_modules", "SCR Risk Modules",
    f"SELECT reporting_period, template_row_id, template_row_label, ROUND(amount_eur, 2) as amount_eur FROM {FQN}.s2501_scr_breakdown WHERE template_row_id IN ('R0010', 'R0020', 'R0030', 'R0040', 'R0050', 'R0100', 'R0130', 'R0150', 'R0200') ORDER BY reporting_period, template_row_id")

ds_scr_market = ds("ds_scr_market", "Market Risk Sub-modules",
    f"SELECT reporting_period, template_row_label, ROUND(amount_eur, 2) as amount_eur FROM {FQN}.s2501_scr_breakdown WHERE template_row_id LIKE 'R0010.%' ORDER BY reporting_period, template_row_id")

ds_scr_nl = ds("ds_scr_nl", "Non-Life UW Sub-modules",
    f"SELECT reporting_period, template_row_label, ROUND(amount_eur, 2) as amount_eur FROM {FQN}.s2501_scr_breakdown WHERE template_row_id LIKE 'R0050.%' ORDER BY reporting_period, template_row_id")

ds_own_funds = ds("ds_own_funds", "Own Funds by Tier",
    f"SELECT reporting_period, component, tier, amount_eur FROM {FQN}.own_funds ORDER BY reporting_period, tier")

ds_solvency_trend = ds("ds_solvency_trend", "Solvency Ratio Trend",
    f"SELECT reporting_period, solvency_ratio_pct, ROUND(scr_eur / 1e6, 1) as scr_m, ROUND(eligible_own_funds_eur / 1e6, 1) as eof_m FROM {FQN}.s2501_summary ORDER BY reporting_period")


# ── Widget builders ───────────────────────────────────────────────────

def counter(dataset, expr, title, fmt="0.0"):
    wid = uid()
    return {
        "name": wid,
        "queries": [{"name": f"q_{wid}", "query": {
            "datasetName": dataset,
            "fields": [{"name": "val", "expression": expr}],
            "disaggregated": True,
        }}],
        "spec": {
            "version": 2,
            "widgetType": "counter",
            "encodings": {
                "value": {"fieldName": "val", "displayName": title},
            },
            "frame": {"showTitle": True, "title": title},
        },
    }


def bar(dataset, fields, x, y, title, color=None, stacked=False, label=False, sort=None):
    wid = uid()
    enc = {
        "x": {"fieldName": x, "scale": {"type": "categorical"}, "displayName": x},
        "y": {"fieldName": y, "scale": {"type": "quantitative"}, "displayName": y},
    }
    if sort:
        enc["x"]["scale"]["sort"] = {"by": sort}
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    if label:
        enc["label"] = {"show": True}
    spec = {"version": 3, "widgetType": "bar", "encodings": enc,
            "frame": {"showTitle": True, "title": title}}
    if stacked:
        spec["encodings"]["y"]["scale"]["stackMode"] = "stacked"
    return {
        "name": wid,
        "queries": [{"name": f"q_{wid}", "query": {
            "datasetName": dataset,
            "fields": [{"name": f["name"], "expression": f["expr"]} for f in fields],
            "disaggregated": False,
        }}],
        "spec": spec,
    }


def line(dataset, fields, x, y, title, color=None, x_type="categorical"):
    wid = uid()
    enc = {
        "x": {"fieldName": x, "scale": {"type": x_type}, "displayName": x},
        "y": {"fieldName": y, "scale": {"type": "quantitative"}, "displayName": y},
    }
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    return {
        "name": wid,
        "queries": [{"name": f"q_{wid}", "query": {
            "datasetName": dataset,
            "fields": [{"name": f["name"], "expression": f["expr"]} for f in fields],
            "disaggregated": False,
        }}],
        "spec": {"version": 3, "widgetType": "line", "encodings": enc,
                 "frame": {"showTitle": True, "title": title}},
    }


def area(dataset, fields, x, y, title, color=None):
    wid = uid()
    enc = {
        "x": {"fieldName": x, "scale": {"type": "categorical"}, "displayName": x},
        "y": {"fieldName": y, "scale": {"type": "quantitative"}, "displayName": y},
    }
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    return {
        "name": wid,
        "queries": [{"name": f"q_{wid}", "query": {
            "datasetName": dataset,
            "fields": [{"name": f["name"], "expression": f["expr"]} for f in fields],
            "disaggregated": False,
        }}],
        "spec": {"version": 3, "widgetType": "area", "encodings": enc,
                 "frame": {"showTitle": True, "title": title}},
    }


def pie(dataset, fields, angle, color, title):
    wid = uid()
    return {
        "name": wid,
        "queries": [{"name": f"q_{wid}", "query": {
            "datasetName": dataset,
            "fields": [{"name": f["name"], "expression": f["expr"]} for f in fields],
            "disaggregated": False,
        }}],
        "spec": {"version": 3, "widgetType": "pie",
                 "encodings": {
                     "angle": {"fieldName": angle, "scale": {"type": "quantitative"}, "displayName": angle},
                     "color": {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color},
                 },
                 "frame": {"showTitle": True, "title": title}},
    }


def table(dataset, fields, columns, title):
    wid = uid()
    return {
        "name": wid,
        "queries": [{"name": f"q_{wid}", "query": {
            "datasetName": dataset,
            "fields": [{"name": f["name"], "expression": f["expr"]} for f in fields],
            "disaggregated": True,
        }}],
        "spec": {"version": 1, "widgetType": "table",
                 "encodings": {"columns": columns},
                 "frame": {"showTitle": True, "title": title}},
    }


def heatmap(dataset, fields, x, y, color_field, title):
    wid = uid()
    return {
        "name": wid,
        "queries": [{"name": f"q_{wid}", "query": {
            "datasetName": dataset,
            "fields": [{"name": f["name"], "expression": f["expr"]} for f in fields],
            "disaggregated": False,
        }}],
        "spec": {"version": 3, "widgetType": "heatmap",
                 "encodings": {
                     "x": {"fieldName": x, "scale": {"type": "categorical"}, "displayName": x},
                     "y": {"fieldName": y, "scale": {"type": "categorical"}, "displayName": y},
                     "color": {"fieldName": color_field, "scale": {"type": "quantitative"}, "displayName": color_field},
                 },
                 "frame": {"showTitle": True, "title": title}},
    }


def md_widget(text):
    wid = uid()
    return {
        "name": wid,
        "textbox_spec": text,
    }


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def lay(widget, position):
    return {"widget": widget, "position": position}


# ── Page 1: Overview ──────────────────────────────────────────────────

overview_layout = [
    # Header
    lay(md_widget("# Bricksurance SE — Solvency II QRT Dashboard\nQuarterly comparison of regulatory reporting templates across Q1–Q3 2025."),
        pos(0, 0, 6, 1)),

    # KPI counters
    lay(counter(ds_kpi_latest, "`solvency_ratio_pct`", "Solvency Ratio %"),
        pos(0, 1, 1, 2)),
    lay(counter(ds_kpi_latest, "`scr_m`", "SCR (EUR m)"),
        pos(1, 1, 1, 2)),
    lay(counter(ds_kpi_latest, "`eof_m`", "Eligible Own Funds (EUR m)"),
        pos(2, 1, 1, 2)),
    lay(counter(ds_kpi_latest, "`surplus_m`", "Surplus (EUR m)"),
        pos(3, 1, 1, 2)),
    lay(counter(ds_kpi_latest, "`reporting_period`", "Latest Period"),
        pos(4, 1, 2, 2)),

    # Solvency ratio trend
    lay(line(ds_solvency_trend,
            [{"name": "qtr", "expr": "`reporting_period`"},
             {"name": "ratio", "expr": "`solvency_ratio_pct`"}],
            "qtr", "ratio", "Solvency Ratio Trend (%)"),
        pos(0, 3, 3, 4)),

    # SCR vs Own Funds
    lay(bar(ds_solvency_trend,
            [{"name": "qtr", "expr": "`reporting_period`"},
             {"name": "metric", "expr": "'SCR'"},
             {"name": "value", "expr": "`scr_m`"}],
            "qtr", "value", "SCR vs Eligible Own Funds (EUR m)", color="metric"),
        pos(3, 3, 3, 4)),

    # Balance sheet
    lay(bar(ds_balance,
            [{"name": "qtr", "expr": "`reporting_period`"},
             {"name": "item", "expr": "`item`"},
             {"name": "amt", "expr": "SUM(`amount_eur`) / 1e6"}],
            "qtr", "amt", "Balance Sheet Items (EUR m)", color="item", stacked=True),
        pos(0, 7, 6, 4)),
]

# Fix: SCR vs EOF needs both series — use UNION via a separate dataset
ds_scr_vs_eof = ds("ds_scr_eof", "SCR vs EOF",
    f"SELECT reporting_period, 'SCR' as metric, ROUND(scr_eur / 1e6, 1) as value FROM {FQN}.s2501_summary UNION ALL SELECT reporting_period, 'Eligible Own Funds' as metric, ROUND(eligible_own_funds_eur / 1e6, 1) as value FROM {FQN}.s2501_summary ORDER BY reporting_period, metric")

# Replace the SCR vs EOF widget
overview_layout[7] = lay(
    bar(ds_scr_vs_eof,
        [{"name": "qtr", "expr": "`reporting_period`"},
         {"name": "metric", "expr": "`metric`"},
         {"name": "value", "expr": "SUM(`value`)"}],
        "qtr", "value", "SCR vs Eligible Own Funds (EUR m)", color="metric"),
    pos(3, 3, 3, 4))


# ── Page 2: S.06.02 Assets ───────────────────────────────────────────

s0602_layout = [
    lay(md_widget("# S.06.02 — List of Assets\nInvestment portfolio analysis across reporting periods."),
        pos(0, 0, 6, 1)),

    # Asset allocation stacked bar
    lay(bar(ds_asset_alloc,
            [{"name": "qtr", "expr": "`reporting_period`"},
             {"name": "cat", "expr": "`cic_category_name`"},
             {"name": "sii", "expr": "SUM(`total_sii_amount`) / 1e6"}],
            "qtr", "sii", "Asset Allocation by CIC Category (EUR m)", color="cat", stacked=True),
        pos(0, 1, 3, 4)),

    # Asset allocation pie (latest quarter)
    lay(pie(ds_asset_alloc,
            [{"name": "cat", "expr": "`cic_category_name`"},
             {"name": "pct", "expr": "SUM(`pct_of_total_sii`)"}],
            "pct", "cat", "Asset Mix — Latest Quarter (% of SII)"),
        pos(3, 1, 3, 4)),

    # Credit quality heatmap
    lay(heatmap(ds_asset_quality,
                [{"name": "qtr", "expr": "`reporting_period`"},
                 {"name": "band", "expr": "`quality_band`"},
                 {"name": "sii", "expr": "SUM(`total_sii`) / 1e6"}],
                "qtr", "band", "sii",
                "Credit Quality Distribution (EUR m)"),
        pos(0, 5, 3, 4)),

    # Duration by asset class
    lay(bar(ds_asset_duration,
            [{"name": "qtr", "expr": "`reporting_period`"},
             {"name": "cls", "expr": "`asset_class`"},
             {"name": "dur", "expr": "SUM(`avg_duration`)"}],
            "qtr", "dur", "Average Modified Duration by Asset Class", color="cls"),
        pos(3, 5, 3, 4)),

    # Top issuer countries
    lay(bar(ds_asset_country,
            [{"name": "country", "expr": "`issuer_country`"},
             {"name": "sii", "expr": "SUM(`total_sii`) / 1e6"}],
            "country", "sii", "Top Issuer Countries (EUR m)", sort="y-reversed", label=True),
        pos(0, 9, 3, 4)),

    # Asset count by category over time
    lay(line(ds_asset_alloc,
             [{"name": "qtr", "expr": "`reporting_period`"},
              {"name": "cat", "expr": "`cic_category_name`"},
              {"name": "cnt", "expr": "SUM(`asset_count`)"}],
             "qtr", "cnt", "Number of Assets by Category", color="cat"),
        pos(3, 9, 3, 4)),
]


# ── Page 3: S.05.01 P&L ──────────────────────────────────────────────

s0501_layout = [
    lay(md_widget("# S.05.01 — Premiums, Claims & Expenses\nP&L analysis by line of business and reporting period."),
        pos(0, 0, 6, 1)),

    # Combined ratio heatmap
    lay(heatmap(ds_combined,
                [{"name": "qtr", "expr": "`reporting_period`"},
                 {"name": "lob", "expr": "`lob_name`"},
                 {"name": "cr", "expr": "SUM(`combined_ratio_pct`)"}],
                "qtr", "lob", "cr",
                "Combined Ratio by LoB & Quarter (%)"),
        pos(0, 1, 6, 4)),

    # GWP by LoB stacked area
    lay(area(ds_gwp_by_lob,
             [{"name": "qtr", "expr": "`reporting_period`"},
              {"name": "lob", "expr": "`lob_name`"},
              {"name": "gwp", "expr": "SUM(`amount_eur`) / 1e6"}],
             "qtr", "gwp", "Gross Written Premium by LoB (EUR m)", color="lob"),
        pos(0, 5, 3, 4)),

    # Loss ratio vs expense ratio by LoB (latest)
    lay(bar(ds_combined,
            [{"name": "lob", "expr": "`lob_name`"},
             {"name": "lr", "expr": "SUM(`loss_ratio_pct`)"}],
            "lob", "lr", "Loss Ratio by LoB — Latest Quarter (%)", label=True),
        pos(3, 5, 3, 4)),

    # P&L totals trend
    lay(line(ds_pnl_totals,
             [{"name": "qtr", "expr": "`reporting_period`"},
              {"name": "item", "expr": "`template_row_label`"},
              {"name": "amt", "expr": "SUM(`amount_eur`) / 1e6"}],
             "qtr", "amt", "Key P&L Items Trend (EUR m)", color="item"),
        pos(0, 9, 3, 4)),

    # RI cession rates
    lay(bar(ds_combined,
            [{"name": "lob", "expr": "`lob_name`"},
             {"name": "qtr", "expr": "`reporting_period`"},
             {"name": "ri", "expr": "SUM(`ri_cession_rate_pct`)"}],
            "lob", "ri", "Reinsurance Cession Rate by LoB (%)", color="qtr"),
        pos(3, 9, 3, 4)),

    # Detailed table
    lay(table(ds_combined,
              [{"name": "qtr", "expr": "`reporting_period`"},
               {"name": "lob", "expr": "`lob_name`"},
               {"name": "gwp", "expr": "`gwp`"},
               {"name": "cr", "expr": "`combined_ratio_pct`"},
               {"name": "lr", "expr": "`loss_ratio_pct`"},
               {"name": "er", "expr": "`expense_ratio_pct`"},
               {"name": "ri", "expr": "`ri_cession_rate_pct`"}],
              [
                  {"fieldName": "qtr", "type": "string", "displayAs": "string", "title": "Quarter"},
                  {"fieldName": "lob", "type": "string", "displayAs": "string", "title": "Line of Business"},
                  {"fieldName": "gwp", "type": "float", "displayAs": "number", "numberFormat": "#,##0", "title": "GWP (EUR)", "alignContent": "right"},
                  {"fieldName": "cr", "type": "float", "displayAs": "number", "numberFormat": "0.0", "title": "Combined %", "alignContent": "right"},
                  {"fieldName": "lr", "type": "float", "displayAs": "number", "numberFormat": "0.0", "title": "Loss %", "alignContent": "right"},
                  {"fieldName": "er", "type": "float", "displayAs": "number", "numberFormat": "0.0", "title": "Expense %", "alignContent": "right"},
                  {"fieldName": "ri", "type": "float", "displayAs": "number", "numberFormat": "0.0", "title": "RI Cession %", "alignContent": "right"},
              ],
              "S.05.01 Detail — All Quarters"),
        pos(0, 13, 6, 5)),
]


# ── Page 4: S.25.01 SCR ──────────────────────────────────────────────

s2501_layout = [
    lay(md_widget("# S.25.01 — SCR Standard Formula\nSolvency Capital Requirement breakdown and solvency position."),
        pos(0, 0, 6, 1)),

    # SCR waterfall by module (latest)
    lay(bar(ds_scr_modules,
            [{"name": "module", "expr": "`template_row_label`"},
             {"name": "qtr", "expr": "`reporting_period`"},
             {"name": "amt", "expr": "SUM(`amount_eur`) / 1e6"}],
            "module", "amt", "SCR Breakdown by Risk Module (EUR m)", color="qtr"),
        pos(0, 1, 6, 5)),

    # Market risk sub-modules
    lay(bar(ds_scr_market,
            [{"name": "sub", "expr": "`template_row_label`"},
             {"name": "qtr", "expr": "`reporting_period`"},
             {"name": "amt", "expr": "SUM(`amount_eur`) / 1e6"}],
            "sub", "amt", "Market Risk Sub-modules (EUR m)", color="qtr"),
        pos(0, 6, 3, 4)),

    # Non-life UW sub-modules
    lay(bar(ds_scr_nl,
            [{"name": "sub", "expr": "`template_row_label`"},
             {"name": "qtr", "expr": "`reporting_period`"},
             {"name": "amt", "expr": "SUM(`amount_eur`) / 1e6"}],
            "sub", "amt", "Non-Life UW Sub-modules (EUR m)", color="qtr"),
        pos(3, 6, 3, 4)),

    # Own funds by tier
    lay(bar(ds_own_funds,
            [{"name": "qtr", "expr": "`reporting_period`"},
             {"name": "tier", "expr": "CONCAT('Tier ', `tier`)"},
             {"name": "amt", "expr": "SUM(`amount_eur`) / 1e6"}],
            "qtr", "amt", "Own Funds by Tier (EUR m)", color="tier", stacked=True),
        pos(0, 10, 3, 4)),

    # Solvency ratio with 100% reference
    lay(line(ds_solvency,
             [{"name": "qtr", "expr": "`reporting_period`"},
              {"name": "ratio", "expr": "`solvency_ratio_pct`"}],
             "qtr", "ratio", "Solvency Ratio Trend (%)"),
        pos(3, 10, 3, 4)),

    # SCR detail table
    lay(table(ds_scr_modules,
              [{"name": "qtr", "expr": "`reporting_period`"},
               {"name": "module", "expr": "`template_row_label`"},
               {"name": "id", "expr": "`template_row_id`"},
               {"name": "amt", "expr": "`amount_eur`"}],
              [
                  {"fieldName": "qtr", "type": "string", "displayAs": "string", "title": "Quarter"},
                  {"fieldName": "id", "type": "string", "displayAs": "string", "title": "Row ID"},
                  {"fieldName": "module", "type": "string", "displayAs": "string", "title": "Risk Module"},
                  {"fieldName": "amt", "type": "float", "displayAs": "number", "numberFormat": "#,##0", "title": "Amount (EUR)", "alignContent": "right"},
              ],
              "S.25.01 Detail — SCR Components"),
        pos(0, 14, 6, 5)),
]


# ── Assemble dashboard ───────────────────────────────────────────────

serialized = {
    "datasets": datasets,
    "pages": [
        {
            "name": uid(),
            "displayName": "Overview",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": overview_layout,
        },
        {
            "name": uid(),
            "displayName": "S.06.02 — Assets",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": s0602_layout,
        },
        {
            "name": uid(),
            "displayName": "S.05.01 — P&L",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": s0501_layout,
        },
        {
            "name": uid(),
            "displayName": "S.25.01 — SCR",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": s2501_layout,
        },
    ],
    "uiSettings": {
        "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
        "applyModeEnabled": False,
    },
}

# ── Deploy ────────────────────────────────────────────────────────────

payload = {
    "display_name": "Solvency II QRT — Quarterly Comparison",
    "warehouse_id": WAREHOUSE_ID,
    "parent_path": "/Users/laurence.ryszka@databricks.com",
    "serialized_dashboard": json.dumps(serialized),
}

print("Creating dashboard...")
result = subprocess.run(
    ["databricks", "api", "post", "/api/2.0/lakeview/dashboards",
     "--profile", PROFILE, "--json", json.dumps(payload)],
    capture_output=True, text=True,
)

if result.returncode != 0:
    print(f"Error: {result.stderr}")
    sys.exit(1)

resp = json.loads(result.stdout)
dashboard_id = resp.get("dashboard_id", "")
path = resp.get("path", "")

print(f"Dashboard created: {dashboard_id}")
print(f"Path: {path}")

# Publish
print("Publishing...")
pub_result = subprocess.run(
    ["databricks", "api", "post",
     f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
     "--profile", PROFILE,
     "--json", json.dumps({"warehouse_id": WAREHOUSE_ID, "embed_credentials": True})],
    capture_output=True, text=True,
)

if pub_result.returncode == 0:
    print("Dashboard published successfully.")
else:
    print(f"Publish warning: {pub_result.stderr}")

print(f"\nDashboard URL: https://fevm-lr-serverless-aws-us.cloud.databricks.com/dashboardsv3/{dashboard_id}")
