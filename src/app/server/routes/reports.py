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
            {"step": 1, "phase": "Ingestion", "source": "Investment Platform (Simcorp)", "target": "assets",
             "layer": "Bronze", "description": "5,000 investment positions ingested from custodian feeds",
             "row_count_hint": "~5,000 positions per quarter",
             "sql_snippet": None, "expectations": []},
            {"step": 2, "phase": "Transformation", "source": "assets", "target": "assets_enriched",
             "layer": "Silver",
             "description": "Enrich raw investment register with CIC code decomposition (country + category), Solvency II valuation method (mark-to-market vs mark-to-model), and credit quality step mapping from external ratings",
             "row_count_hint": "5,000 -> ~4,996 (after DQ drops)",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW assets_enriched AS\nSELECT\n  asset_id, asset_name, asset_class,\n  cic_code,\n  SUBSTRING(cic_code, 1, 2) AS cic_country,\n  SUBSTRING(cic_code, 3, 1) AS cic_category,\n  CASE SUBSTRING(cic_code, 3, 1)\n    WHEN '1' THEN 'Government bonds'\n    WHEN '2' THEN 'Corporate bonds'\n    WHEN '3' THEN 'Equity'\n    WHEN '4' THEN 'Collective investment undertakings'\n    WHEN '9' THEN 'Property'\n    ELSE 'Other'\n  END AS cic_category_name,\n  -- SII valuation\n  sii_value,\n  CASE WHEN is_listed THEN 'Mark-to-market'\n       ELSE 'Mark-to-model' END AS valuation_method,\n  credit_rating, credit_quality_step,\n  modified_duration\nFROM LIVE.assets",
             "expectations": [
                 {"name": "asset_id_not_null", "rule": "asset_id IS NOT NULL", "action": "DROP ROW"},
                 {"name": "sii_value_positive", "rule": "sii_value > 0", "action": "FAIL UPDATE"},
                 {"name": "cic_code_valid", "rule": "LENGTH(cic_code) = 4", "action": "DROP ROW"},
                 {"name": "currency_not_null", "rule": "currency IS NOT NULL", "action": "DROP ROW"},
             ]},
            {"step": 3, "phase": "Confirmation", "source": "assets_enriched", "target": "s0602_list_of_assets",
             "layer": "Gold",
             "description": "Map enriched assets to EIOPA S.06.02 template. Each column corresponds to an EIOPA cell reference (C0040-C0370). This is a column rename -- no business logic, just regulatory format mapping",
             "row_count_hint": "1:1 mapping (one row per asset)",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW s0602_list_of_assets AS\nSELECT\n  asset_id           AS C0040_Asset_ID,\n  portfolio_type     AS C0060_Portfolio,\n  custodian_name     AS C0120_Custodian,\n  par_value          AS C0130_Quantity,\n  sii_value          AS C0170_Total_Solvency_II_Amount,\n  asset_name         AS C0190_Item_Title,\n  issuer_name        AS C0200_Issuer_Name,\n  issuer_lei         AS C0210_Issuer_Code,\n  cic_code           AS C0270_CIC,\n  credit_rating      AS C0290_External_Rating,\n  credit_quality_step AS C0310_Credit_Quality_Step,\n  modified_duration  AS C0340_Duration,\n  maturity_date      AS C0370_Maturity_Date\nFROM LIVE.assets_enriched",
             "expectations": [
                 {"name": "c0040_asset_id_present", "rule": "C0040_Asset_ID IS NOT NULL", "action": "DROP ROW"},
                 {"name": "c0170_sii_positive", "rule": "C0170_Total_Solvency_II_Amount > 0", "action": "FAIL UPDATE"},
                 {"name": "c0270_cic_present", "rule": "C0270_CIC IS NOT NULL", "action": "DROP ROW"},
             ]},
            {"step": 4, "phase": "Confirmation", "source": "s0602_list_of_assets", "target": "s0602_summary",
             "layer": "Gold",
             "description": "Aggregate S.06.02 output by CIC category for actuarial review. Shows totals, percentages, and quality indicators that the actuary checks before sign-off",
             "row_count_hint": "~5 categories (Gov bonds, Corp bonds, Equity, CIU, Property)",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW s0602_summary AS\nSELECT\n  cic_category_name,\n  COUNT(*) AS asset_count,\n  SUM(C0170_Total_Solvency_II_Amount) AS total_sii_amount,\n  ROUND(SUM(C0170) * 100.0 /\n    SUM(SUM(C0170)) OVER (), 2) AS pct_of_total_sii,\n  COUNT(CASE WHEN C0310 <= 2 THEN 1 END)\n    AS investment_grade_count,\n  AVG(C0340_Duration) AS avg_duration\nFROM LIVE.s0602_list_of_assets\nGROUP BY cic_category_name",
             "expectations": [
                 {"name": "total_sii_positive", "rule": "total_sii_amount > 0", "action": "FAIL UPDATE"},
             ]},
            {"step": 5, "phase": "Export", "source": "s0602_summary", "target": "EIOPA S.06.02 Template",
             "layer": "Export",
             "description": "Final QRT ready for actuarial sign-off and regulatory submission. Exported as CSV (Tagetik format) and PDF (EIOPA template layout)",
             "row_count_hint": "5,000 assets + summary",
             "sql_snippet": None, "expectations": []},
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
            # --- Ingestion phase ---
            {"step": 1, "phase": "Ingestion", "source": "Policy Admin (Guidewire)", "target": "premiums",
             "layer": "Bronze", "description": "~20,000 premium transactions per quarter from the policy administration system. Each transaction has gross written, gross earned, reinsurers' share, and net amounts by line of business",
             "row_count_hint": "~20K transactions/quarter",
             "sql_snippet": None, "expectations": []},
            {"step": 2, "phase": "Ingestion", "source": "Claims Management System", "target": "claims",
             "layer": "Bronze", "description": "~15,000 claim events per quarter. Each claim has loss date, cause, gross/net paid, incurred, and reserved amounts with open/settled status",
             "row_count_hint": "~15K events/quarter",
             "sql_snippet": None, "expectations": []},
            {"step": 3, "phase": "Ingestion", "source": "Finance / ERP (SAP)", "target": "expenses",
             "layer": "Bronze", "description": "Expense allocations by line of business from the finance system. 6 expense categories: acquisition, administrative, claims management, overhead, investment management, other",
             "row_count_hint": "7 LoB rows/quarter",
             "sql_snippet": None, "expectations": []},
            # --- Transformation phase ---
            {"step": 4, "phase": "Transformation", "source": "premiums", "target": "premiums_by_lob",
             "layer": "Silver",
             "description": "Aggregate raw premium transactions to quarterly totals per line of business. Reconcile: net written = gross written - reinsurers' share (tolerance EUR 1.00)",
             "row_count_hint": "~20K txns -> 7 LoB rows",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW premiums_by_lob AS\nSELECT\n  reporting_period, lob_code, lob_name,\n  SUM(gross_written_premium)    AS gross_written_premium,\n  SUM(gross_earned_premium)     AS gross_earned_premium,\n  SUM(reinsurers_share_written) AS reinsurers_share_written,\n  SUM(reinsurers_share_earned)  AS reinsurers_share_earned,\n  SUM(net_written_premium)      AS net_written_premium,\n  SUM(net_earned_premium)       AS net_earned_premium,\n  COUNT(*)                      AS transaction_count\nFROM LIVE.premiums\nGROUP BY reporting_period, lob_code, lob_name",
             "expectations": [
                 {"name": "gross_written_positive", "rule": "gross_written_premium > 0", "action": "DROP ROW"},
                 {"name": "net_equals_gross_minus_ri", "rule": "ABS(net_written - (gross_written - ri_share)) < 1.0", "action": "WARN"},
             ]},
            {"step": 5, "phase": "Transformation", "source": "claims", "target": "claims_by_lob",
             "layer": "Silver",
             "description": "Aggregate raw claim events to quarterly totals per LoB. Validate that net incurred never exceeds gross incurred (after rounding tolerance)",
             "row_count_hint": "~15K events -> 7 LoB rows",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW claims_by_lob AS\nSELECT\n  reporting_period, lob_code, lob_name,\n  SUM(gross_incurred)            AS gross_incurred,\n  SUM(gross_paid)                AS gross_paid,\n  SUM(gross_reserved)            AS gross_reserved,\n  SUM(reinsurers_share_incurred) AS reinsurers_share_incurred,\n  SUM(net_incurred)              AS net_incurred,\n  SUM(net_paid)                  AS net_paid,\n  COUNT(*)                       AS claim_count,\n  COUNT(CASE WHEN status = 'open' THEN 1 END) AS open_claims\nFROM LIVE.claims\nGROUP BY reporting_period, lob_code, lob_name",
             "expectations": [
                 {"name": "gross_incurred_positive", "rule": "gross_incurred > 0", "action": "DROP ROW"},
                 {"name": "net_leq_gross", "rule": "net_incurred <= gross_incurred + 1.0", "action": "WARN"},
             ]},
            {"step": 6, "phase": "Transformation", "source": "expenses", "target": "expenses_by_lob",
             "layer": "Silver",
             "description": "Validate expense allocations: the sum of 6 expense components must equal the total (tolerance EUR 1.00). This catches allocation errors from the finance system",
             "row_count_hint": "7 LoB rows (pass-through with validation)",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW expenses_by_lob AS\nSELECT\n  reporting_period, lob_code, lob_name,\n  acquisition_expenses,\n  administrative_expenses,\n  claims_management_expenses,\n  overhead_expenses,\n  investment_management_expenses,\n  other_expenses,\n  total_expenses\nFROM LIVE.expenses",
             "expectations": [
                 {"name": "total_expenses_positive", "rule": "total_expenses > 0", "action": "DROP ROW"},
                 {"name": "components_sum_to_total", "rule": "ABS(total - (acq + admin + claims_mgmt + overhead + inv_mgmt + other)) < 1.0", "action": "WARN"},
             ]},
            # --- Confirmation phase ---
            {"step": 7, "phase": "Confirmation", "source": "premiums_by_lob + claims_by_lob + expenses_by_lob",
             "target": "s0501_premiums_claims_expenses", "layer": "Gold",
             "description": "Merge 3 silver tables into EIOPA S.05.01 template format. 26 UNION ALL statements map to template rows R0110-R1200: premiums written/earned (gross/RI/net), claims incurred/paid, and 6 expense categories. Each row has a template_row_id matching the EIOPA log",
             "row_count_hint": "3 x 7 LoB -> 144 template rows (18 row types x 8 LoB incl. Total)",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW\n  s0501_premiums_claims_expenses AS\n\n-- R0110: Premiums written - Gross\nSELECT reporting_period, 'R0110' AS template_row_id,\n  'Premiums written - Gross' AS template_row_label,\n  lob_code, lob_name,\n  gross_written_premium AS amount_eur\nFROM LIVE.premiums_by_lob\n\nUNION ALL\n-- R0200: Premiums written - Net\nSELECT ..., 'R0200', 'Premiums written - Net',\n  net_written_premium\nFROM LIVE.premiums_by_lob\n\nUNION ALL\n-- R0310: Claims incurred - Gross\nSELECT ..., 'R0310', 'Claims incurred - Gross',\n  gross_incurred\nFROM LIVE.claims_by_lob\n\nUNION ALL\n-- R0550: Expenses incurred\nSELECT ..., 'R0550', 'Expenses incurred',\n  total_expenses\nFROM LIVE.expenses_by_lob\n-- ... 26 UNION ALL total",
             "expectations": [
                 {"name": "row_id_present", "rule": "template_row_id IS NOT NULL", "action": "DROP ROW"},
                 {"name": "amount_not_null", "rule": "amount_eur IS NOT NULL", "action": "DROP ROW"},
             ]},
            # --- Export phase ---
            {"step": 8, "phase": "Export", "source": "s0501_premiums_claims_expenses",
             "target": "s0501_summary", "layer": "Gold",
             "description": "Compute key P&L ratios for actuarial sign-off: loss ratio (net claims / net earned premium), expense ratio (expenses / net earned premium), combined ratio (loss + expense). Combined ratio below 100% means underwriting profit. Also computes RI cession rate per LoB",
             "row_count_hint": "7 LoB rows with ratios",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW s0501_summary AS\nWITH pivoted AS (\n  SELECT reporting_period, lob_code, lob_name,\n    MAX(CASE WHEN template_row_id = 'R0110'\n         THEN amount_eur END) AS gross_written_premium,\n    MAX(CASE WHEN template_row_id = 'R0300'\n         THEN amount_eur END) AS net_earned_premium,\n    MAX(CASE WHEN template_row_id = 'R0400'\n         THEN amount_eur END) AS net_incurred,\n    MAX(CASE WHEN template_row_id = 'R0550'\n         THEN amount_eur END) AS total_expenses\n  FROM LIVE.s0501_premiums_claims_expenses\n  WHERE lob_code != 0\n  GROUP BY reporting_period, lob_code, lob_name\n)\nSELECT *,\n  ROUND(net_incurred * 100.0 /\n    NULLIF(net_earned_premium, 0), 1)\n    AS loss_ratio_pct,\n  ROUND(total_expenses * 100.0 /\n    NULLIF(net_earned_premium, 0), 1)\n    AS expense_ratio_pct,\n  ROUND((net_incurred + total_expenses) * 100.0 /\n    NULLIF(net_earned_premium, 0), 1)\n    AS combined_ratio_pct\nFROM pivoted",
             "expectations": [
                 {"name": "combined_ratio_realistic", "rule": "combined_ratio_pct BETWEEN 50 AND 200", "action": "DROP ROW"},
             ]},
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
            # --- Ingestion phase ---
            {"step": 1, "phase": "Ingestion", "source": "Risk Engine (Igloo/RAFM)", "target": "risk_factors",
             "layer": "Bronze", "description": "17 SCR sub-module risk charges from the risk engine: market risk (interest rate, equity, property, spread, currency, concentration), counterparty default (type 1 & 2), non-life UW (premium/reserve, lapse, catastrophe), health, and life",
             "row_count_hint": "17 sub-module charges/quarter",
             "sql_snippet": None, "expectations": []},
            {"step": 2, "phase": "Ingestion", "source": "Finance / Treasury", "target": "own_funds",
             "layer": "Bronze", "description": "Own funds components by tier: Tier 1 (ordinary share capital, share premium, reconciliation reserve, subordinated liabilities), Tier 2, Tier 3. Used for solvency ratio calculation",
             "row_count_hint": "6 components/quarter",
             "sql_snippet": None, "expectations": []},
            # --- Transformation phase ---
            {"step": 3, "phase": "Transformation", "source": "risk_factors", "target": "scr_results",
             "layer": "Model",
             "description": "Load the Standard Formula model (Champion, v1, 2025 calibration) from Unity Catalog and run it against the risk factor charges. The model aggregates sub-modules using EIOPA correlation matrices:\n- Market risk: 7 sub-modules via 7x7 correlation matrix\n- Non-life UW: 3 sub-modules via 3x3 matrix\n- BSCR: 5 modules via 5x5 matrix\nThen adds operational risk (3% of BSCR) and subtracts loss-absorbing capacity of deferred taxes",
             "row_count_hint": "17 inputs -> 19 output components (9 main + 10 sub-modules)",
             "sql_snippet": "# Python (MLflow PythonModel)\nchampion = mlflow.pyfunc.load_model(\n    f'models:/{model_name}@Champion'\n)\n\n# Aggregate correlated:\n# sqrt(sum_i sum_j rho_ij * C_i * C_j)\nscr_market = aggregate(mkt_charges, mkt_corr)\nscr_non_life = aggregate(nl_charges, nl_corr)\nbscr = aggregate(all_modules, bscr_corr)\n\nop_risk = bscr * 0.03  # 3% of BSCR\nlac_dt = min(bscr * 0.10, bscr * 0.15)\nSCR = bscr + op_risk - lac_dt",
             "expectations": []},
            # --- Confirmation phase ---
            {"step": 4, "phase": "Confirmation", "source": "scr_results", "target": "s2501_scr_breakdown",
             "layer": "Gold",
             "description": "Map SCR model output to EIOPA S.25.01 template rows. Each component gets an EIOPA row reference: R0010 (Market), R0020 (Default), R0050 (Non-life), R0100 (BSCR), R0130 (Op risk), R0150 (LAC_DT), R0200 (SCR). Sub-modules get dotted references (R0010.01-R0010.07)",
             "row_count_hint": "19 components -> 17 template rows",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW\n  s2501_scr_breakdown AS\n\nSELECT reporting_period,\n  'R0010' AS template_row_id,\n  'Market risk' AS template_row_label,\n  amount_eur, model_version\nFROM LIVE.scr_results\nWHERE component = 'SCR_market'\n\nUNION ALL\nSELECT ..., 'R0100',\n  'Basic Solvency Capital Requirement',\n  amount_eur\nWHERE component = 'BSCR'\n\nUNION ALL\nSELECT ..., 'R0200',\n  'Solvency Capital Requirement',\n  amount_eur\nWHERE component = 'SCR'\n-- ... 15 UNION ALL total",
             "expectations": [
                 {"name": "row_id_present", "rule": "template_row_id IS NOT NULL", "action": "DROP ROW"},
                 {"name": "amount_not_null", "rule": "amount_eur IS NOT NULL", "action": "DROP ROW"},
             ]},
            {"step": 5, "phase": "Confirmation", "source": "s2501_scr_breakdown + own_funds",
             "target": "s2501_summary", "layer": "Gold",
             "description": "Combine SCR breakdown with own funds to compute the solvency position. Applies EIOPA tiering limits (Tier 2 capped at 50% SCR, Tier 3 at 15% SCR). MCR = max(25% SCR, EUR 3.7M floor). Solvency ratio = Eligible Own Funds / SCR (must exceed 100%)",
             "row_count_hint": "1 summary row per quarter",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW\n  s2501_summary AS\nWITH scr AS (\n  SELECT reporting_period,\n    MAX(CASE WHEN template_row_id = 'R0200'\n         THEN amount_eur END) AS scr_eur\n  FROM LIVE.s2501_scr_breakdown\n  GROUP BY reporting_period\n),\nfunds AS (\n  SELECT reporting_period,\n    SUM(CASE WHEN tier=1 THEN amount_eur END) AS tier1,\n    SUM(CASE WHEN tier=2 THEN amount_eur END) AS tier2,\n    SUM(CASE WHEN tier=3 THEN amount_eur END) AS tier3\n  FROM LIVE.own_funds GROUP BY reporting_period\n)\nSELECT\n  -- Eligible own funds (tiering limits)\n  tier1 + LEAST(tier2, scr*0.50)\n       + LEAST(tier3, scr*0.15)\n    AS eligible_own_funds_eur,\n  -- Solvency ratio\n  ROUND(eligible / scr * 100, 1)\n    AS solvency_ratio_pct",
             "expectations": [
                 {"name": "solvency_ratio_positive", "rule": "solvency_ratio_pct > 0", "action": "FAIL UPDATE"},
                 {"name": "scr_positive", "rule": "scr_eur > 0", "action": "FAIL UPDATE"},
             ]},
            # --- Export phase ---
            {"step": 6, "phase": "Export", "source": "s2501_summary", "target": "EIOPA S.25.01 Template + Solvency Position",
             "layer": "Export",
             "description": "Final solvency position ready for board review and regulatory filing. Includes SCR breakdown, eligible own funds, solvency ratio, MCR ratio, and surplus. Exported as CSV and PDF",
             "row_count_hint": "17 template rows + solvency summary",
             "sql_snippet": None, "expectations": []},
        ],
    },
    "s2606": {
        "id": "s2606",
        "name": "S.26.06",
        "title": "NL Underwriting Risk",
        "table": "s2606_nl_uw_risk",
        "summary_table": "s2606_summary",
        "pipeline": "S.26.06 NL UW Risk Template",
        "lineage": [
            {"step": 1, "phase": "Ingestion", "source": "Exposure Management System", "target": "exposures",
             "layer": "Bronze", "description": "Exposure sets by peril and line of business -- total sum insured, deductibles, and limits. ~35 peril x LoB combinations per quarter",
             "row_count_hint": "~35 exposure sets/quarter",
             "sql_snippet": None, "expectations": []},
            {"step": 2, "phase": "Ingestion", "source": "Actuarial Reserving", "target": "volume_measures",
             "layer": "Bronze", "description": "Premium and reserve volume measures by LoB from the actuarial reserving process. Earned premium, written premium (next year), and best estimate claims/premium provisions",
             "row_count_hint": "7 LoB rows/quarter",
             "sql_snippet": None, "expectations": []},
            {"step": 3, "phase": "Preparation", "source": "exposures", "target": "igloo_exchange/input_exposures.csv",
             "layer": "Export", "description": "Export exposure data as CSV to Unity Catalog Volume. In production this file would be sent to the Igloo stochastic server via SFTP or API. The file contains: exposure_id, peril, LoB, TSI, deductible, limit",
             "row_count_hint": "~35 rows exported to /Volumes/.../igloo_exchange/",
             "sql_snippet": "# Export to Volume (Igloo input)\nexposures_df = spark.sql(\n  \"SELECT * FROM exposures\"\n  \" WHERE reporting_period = '2025-Q3'\"\n)\nexposures_df.toPandas().to_csv(\n  '/Volumes/.../igloo_exchange/'\n  'input_exposures_2025Q3.csv'\n)",
             "expectations": []},
            {"step": 4, "phase": "Stochastic", "source": "Igloo 5.2.1 (10K simulations)", "target": "igloo_run_results",
             "layer": "Model", "description": "Igloo stochastic catastrophe model runs 10,000 Monte Carlo simulations across 7 perils. Produces VaR and TVaR at 6 return periods (1-in-10 to 1-in-500), gross and net of reinsurance. Results are imported from the exchange Volume back into Delta",
             "row_count_hint": "~210 result rows (7 perils x 5 LoB x 6 return periods)",
             "sql_snippet": "# Igloo stochastic engine (mock)\nprint('Running 10,000 simulations...')\ntime.sleep(5)  # Simulated run time\n\n# Import results from Volume\nresults = spark.read.csv(\n  '/Volumes/.../igloo_exchange/'\n  'output_results_2025Q3.csv'\n)\nresults.write.saveAsTable(\n  'igloo_run_results'\n)",
             "expectations": []},
            {"step": 5, "phase": "Transformation", "source": "igloo_run_results", "target": "cat_risk_by_lob",
             "layer": "Silver", "description": "Filter Igloo output to the 1-in-200 return period (VaR 99.5%, the Solvency II regulatory standard). Aggregate net-of-reinsurance VaR across all perils per LoB. Validate that TVaR >= VaR (tail is heavier)",
             "row_count_hint": "~210 results -> 5 LoB cat risk charges",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW cat_risk_by_lob AS\nSELECT\n  lob_code, lob_name,\n  SUM(var_net_eur)  AS var_net_eur,\n  SUM(tvar_net_eur) AS tvar_net_eur,\n  COUNT(DISTINCT peril) AS perils_modelled\nFROM LIVE.igloo_run_results\nWHERE return_period = 200  -- 1-in-200 VaR 99.5%\nGROUP BY lob_code, lob_name",
             "expectations": [
                 {"name": "var_net_positive", "rule": "var_net_eur > 0", "action": "DROP ROW"},
                 {"name": "tvar_gte_var", "rule": "tvar_net_eur >= var_net_eur", "action": "DROP ROW"},
             ]},
            {"step": 6, "phase": "Transformation", "source": "volume_measures", "target": "premium_reserve_risk",
             "layer": "Silver", "description": "Apply EIOPA Standard Formula sigma factors to compute premium and reserve risk charges. Premium risk sigma ranges from 6.5% (Medical) to 14% (General liability). Reserve risk sigma ranges from 9% to 19%. Risk charge = 3 x sigma x volume (VaR 99.5% approximation)",
             "row_count_hint": "7 LoB -> 7 premium + 7 reserve risk charges",
             "sql_snippet": "CREATE OR REFRESH MATERIALIZED VIEW premium_reserve_risk AS\nSELECT\n  lob_code, lob_name,\n  -- Volume measure\n  GREATEST(earned_premium_net,\n           written_premium_net_next_year)\n    + best_estimate_claims_provision\n    AS volume_measure_eur,\n  -- Premium risk = 3 * sigma_prem * volume\n  3.0 * sigma_premium * volume AS premium_risk_eur,\n  -- Reserve risk = 3 * sigma_res * BE_claims\n  3.0 * sigma_reserve * BE_claims AS reserve_risk_eur\nFROM LIVE.volume_measures",
             "expectations": [
                 {"name": "volume_positive", "rule": "volume_measure_eur > 0", "action": "DROP ROW"},
                 {"name": "premium_risk_positive", "rule": "premium_risk_eur >= 0", "action": "DROP ROW"},
                 {"name": "reserve_risk_positive", "rule": "reserve_risk_eur >= 0", "action": "DROP ROW"},
             ]},
            {"step": 7, "phase": "Confirmation", "source": "cat_risk_by_lob + premium_reserve_risk", "target": "s2606_nl_uw_risk",
             "layer": "Gold", "description": "Merge catastrophe risk and premium/reserve risk into EIOPA S.26.06 template. Aggregate via correlation matrix (premium/reserve <-> cat correlation = 0.25). Template rows: R0010 (premium), R0020 (reserve), R0040 (cat), R0100 (diversified total), R0110 (diversification benefit)",
             "row_count_hint": "7 template rows",
             "sql_snippet": "-- Diversified NL UW SCR\nSQRT(\n  POWER(combined_prem_res_risk, 2) +\n  POWER(total_cat_risk, 2) +\n  2 * 0.25 * combined_prem_res_risk\n           * total_cat_risk\n) AS diversified_nl_uw_scr",
             "expectations": [
                 {"name": "row_id_present", "rule": "template_row_id IS NOT NULL", "action": "DROP ROW"},
                 {"name": "amount_not_null", "rule": "amount_eur IS NOT NULL", "action": "DROP ROW"},
             ]},
            {"step": 8, "phase": "Confirmation", "source": "s2606_nl_uw_risk", "target": "s2606_summary",
             "layer": "Gold", "description": "Summary view for actuarial sign-off showing premium risk, reserve risk, cat risk (VaR and TVaR), diversification benefit, and total NL UW SCR. Includes cat risk as percentage of total",
             "row_count_hint": "1 summary row per quarter",
             "sql_snippet": None,
             "expectations": [
                 {"name": "total_nl_uw_positive", "rule": "total_nl_uw_scr > 0", "action": "FAIL UPDATE"},
             ]},
            {"step": 9, "phase": "Export", "source": "s2606_summary", "target": "EIOPA S.26.06 Template",
             "layer": "Export", "description": "Final NL underwriting risk template ready for actuarial sign-off and regulatory submission. Includes stochastic model audit trail (Igloo run ID, simulation count, file paths)",
             "row_count_hint": "7 template rows + summary",
             "sql_snippet": None, "expectations": []},
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

            elif qrt_id == "s2606":
                rows = await execute_query(f"""
                    SELECT reporting_period,
                           ROUND(total_nl_uw_scr/1e6, 1) AS nl_uw_meur,
                           cat_pct_of_total
                    FROM {fqn('s2606_summary')}
                    ORDER BY reporting_period DESC LIMIT 1
                """)
                if rows:
                    info["period"] = rows[0]["reporting_period"]
                    info["row_count"] = "7 components"
                    info["metric_label"] = "NL UW SCR"
                    info["metric_value"] = f"EUR {rows[0]['nl_uw_meur']}M"

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

        elif qrt_id == "s2606":
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

        elif qrt_id == "s2606":
            summary = fqn("s2606_summary")
            total_r = await execute_query(f"SELECT COUNT(*) AS c FROM {summary}")
            total = int(total_r[0]["c"]) if total_r else 0

            neg_scr = await execute_query(f"SELECT COUNT(*) AS c FROM {summary} WHERE total_nl_uw_scr <= 0")
            checks.append({"check": "NL UW SCR positive", "constraint": "total_nl_uw_scr > 0",
                           "total": total, "failing": int(neg_scr[0]["c"]) if neg_scr else 0, "severity": "FAIL UPDATE"})

            # Cat risk should be less than total (diversification)
            bad_cat = await execute_query(f"SELECT COUNT(*) AS c FROM {summary} WHERE cat_risk_var_eur > total_nl_uw_scr")
            checks.append({"check": "Cat risk < total (diversification)", "constraint": "cat_risk_var < total_nl_uw_scr",
                           "total": total, "failing": int(bad_cat[0]["c"]) if bad_cat else 0, "severity": "WARNING"})

            # Premium risk positive
            neg_prem = await execute_query(f"SELECT COUNT(*) AS c FROM {summary} WHERE premium_risk_eur <= 0")
            checks.append({"check": "Premium risk positive", "constraint": "premium_risk_eur > 0",
                           "total": total, "failing": int(neg_prem[0]["c"]) if neg_prem else 0, "severity": "FAIL UPDATE"})

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

        elif qrt_id == "s2606":
            where = f"WHERE reporting_period = '{period}'" if period else f"WHERE reporting_period = {latest}"
            breakdown = await execute_query(f"""
                SELECT template_row_id, template_row_label,
                       CAST(amount_eur AS DOUBLE) AS amount_eur
                FROM {fqn('s2606_nl_uw_risk')} {where}
                ORDER BY template_row_id
            """)
            summary = await execute_query(f"""
                SELECT * FROM {fqn('s2606_summary')} {where}
            """)
            return {"qrt": "S.26.06", "title": "Non-Life Underwriting Risk",
                    "format": "waterfall", "period": period,
                    "data": breakdown, "summary": summary[0] if summary else None}

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
