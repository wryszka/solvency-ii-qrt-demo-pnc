#!/usr/bin/env python3
"""Add table and column descriptions to all demo tables for Genie discoverability."""

import subprocess
import json
import sys

CATALOG = "lr_serverless_aws_us_catalog"
SCHEMA = "solvency2demo"
WAREHOUSE_ID = "ab79eced8207d29b"
PROFILE = "DEFAULT"
FQN = f"{CATALOG}.{SCHEMA}"


def sql(statement):
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "wait_timeout": "30s",
    }
    r = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--profile", PROFILE, "--json", json.dumps(payload)],
        capture_output=True, text=True,
    )
    resp = json.loads(r.stdout) if r.returncode == 0 else {}
    state = resp.get("status", {}).get("state", "ERROR")
    if state == "FAILED":
        err = resp.get("status", {}).get("error", {}).get("message", "")
        print(f"  FAILED: {err[:120]}")
    return state


# ── Table descriptions ────────────────────────────────────────────────

TABLE_COMMENTS = {
    "counterparties": "Master register of counterparties — issuers, reinsurers, and banks with credit ratings and LEI codes",
    "assets": "Investment portfolio snapshot at quarter-end — bonds, equities, CIUs, property with SII valuations",
    "policies": "Policy register with inception/expiry dates, GWP, and line of business classification",
    "premiums": "Premium transactions by quarter — gross written, gross earned, reinsurers share, and net amounts by LoB",
    "claims": "Claims transactions — loss events with gross/net paid, incurred, and reserved amounts by LoB and quarter",
    "expenses": "Expense allocation by line of business — acquisition, administrative, claims management, overhead costs per quarter",
    "reinsurance": "Reinsurance programme — quota share and excess-of-loss treaties by line of business",
    "claims_triangles": "Claims development triangles — cumulative paid and incurred by accident year, development period, and LoB",
    "risk_factors": "SCR sub-module risk charges by risk module (market, default, non-life, health, life) per quarter",
    "scr_parameters": "EIOPA Standard Formula parameters — BSCR correlation matrix, market risk correlations, op risk factor",
    "volume_measures": "Premium and reserve volume measures by LoB — feeds S.26.06 NL UW risk calculation",
    "exposures": "Exposure sets by peril and LoB — total sum insured, deductibles, limits for catastrophe modelling",
    "igloo_results": "Simulated stochastic engine output — VaR and TVaR by peril, LoB, and return period (gross/ceded/net)",
    "own_funds": "Own funds components by tier (1, 2, 3) — share capital, reserves, subordinated liabilities per quarter",
    "balance_sheet": "Solvency II balance sheet — total assets, technical provisions, other liabilities, excess per quarter",
    "scr_results": "SCR calculation results from Standard Formula model — risk module charges, BSCR, op risk, LAC_DT, final SCR",
    "assets_enriched": "Enriched investment register — CIC decomposition, SII valuation method, credit quality mapping from raw assets",
    "s0602_list_of_assets": "EIOPA S.06.02 QRT — one row per asset with columns mapped to EIOPA cell references C0040-C0370",
    "s0602_summary": "S.06.02 validation summary — totals by CIC category with asset count, SII amount, and quality indicators",
    "premiums_by_lob": "Premium aggregation by LoB and quarter — gross/RI/net written and earned premiums with reconciliation",
    "claims_by_lob": "Claims aggregation by LoB and quarter — gross/RI/net incurred, paid, and reserved with open claim count",
    "expenses_by_lob": "Expense allocation by LoB and quarter — acquisition, admin, claims mgmt, overhead, investment mgmt components",
    "s0501_premiums_claims_expenses": "EIOPA S.05.01 QRT — long format with template row IDs (R0110-R1200) for premiums, claims, and expenses by LoB",
    "s0501_summary": "S.05.01 validation summary — loss ratio, expense ratio, combined ratio, and RI cession rate by LoB per quarter",
    "s2501_scr_breakdown": "EIOPA S.25.01 QRT — SCR breakdown with template row IDs (R0010-R0200) including market and NL sub-modules",
    "s2501_summary": "S.25.01 solvency summary — SCR, eligible own funds, solvency ratio, MCR ratio, and surplus per quarter",
    "qrt_approvals": "QRT approval workflow — submission and review status, reviewer comments, and Tagetik export paths",
    "pipeline_sla_status": "Pipeline SLA tracking — feed arrival times vs deadlines for Control Tower monitoring",
    "dq_expectation_results": "DQ expectation results — pass/fail rates from DLT pipeline expectations per table and quarter",
    "cross_qrt_reconciliation": "Cross-QRT reconciliation checks — consistency validation between QRTs and source data",
    "model_registry_log": "Model version usage log — Champion vs Challenger Standard Formula SCR results per quarter",
}

# ── Column descriptions ───────────────────────────────────────────────

COLUMN_COMMENTS = {
    "assets": {
        "asset_id": "Unique asset identifier (A000001 format)",
        "asset_name": "Descriptive name of the asset (e.g. issuer name + coupon + maturity year)",
        "asset_class": "Asset class: government_bonds, corporate_bonds, equity, ciu, property",
        "cic_code": "Complementary Identification Code (4 chars: 2-char country + 2-char category)",
        "sii_value": "Solvency II valuation amount in EUR",
        "market_value_eur": "Market value in EUR",
        "acquisition_cost": "Original acquisition cost in EUR",
        "par_value": "Par/nominal value for bonds",
        "coupon_rate": "Annual coupon rate for bonds (decimal, e.g. 0.025 = 2.5%)",
        "credit_rating": "External credit rating (S&P scale: AAA to CCC, NR for not rated)",
        "credit_quality_step": "EIOPA Credit Quality Step (0=AAA-AA, 1=A, 2=BBB, 3=BB, 4=B, 5=CCC, 6=NR)",
        "modified_duration": "Modified duration in years (bonds only) — measures interest rate sensitivity",
        "issuer_name": "Name of the bond issuer or equity company",
        "issuer_lei": "Legal Entity Identifier of the issuer (20-char alphanumeric)",
        "issuer_country": "ISO 2-letter country code of the issuer",
        "reporting_period": "Reporting quarter (format: YYYY-QN, e.g. 2025-Q3)",
    },
    "premiums": {
        "transaction_id": "Unique premium transaction identifier",
        "policy_id": "Link to policies table",
        "lob_code": "Solvency II line of business code (1=Medical, 4=Motor liability, 7=Property, etc.)",
        "lob_name": "Full name of the line of business",
        "reporting_period": "Reporting quarter (format: YYYY-QN)",
        "gross_written_premium": "Gross written premium in EUR before reinsurance",
        "gross_earned_premium": "Gross earned premium in EUR",
        "reinsurers_share_written": "Reinsurers share of written premium in EUR",
        "net_written_premium": "Net written premium in EUR (gross minus RI share)",
        "net_earned_premium": "Net earned premium in EUR",
    },
    "claims": {
        "claim_id": "Unique claim identifier",
        "lob_code": "Solvency II line of business code",
        "lob_name": "Full name of the line of business",
        "reporting_period": "Reporting quarter (format: YYYY-QN)",
        "loss_date": "Date the loss event occurred",
        "cause": "Cause of loss (e.g. collision, fire, theft, flood)",
        "gross_paid": "Gross claims paid in EUR",
        "gross_incurred": "Gross claims incurred in EUR (paid + reserved)",
        "gross_reserved": "Gross outstanding reserve in EUR",
        "net_incurred": "Net claims incurred in EUR (after reinsurance)",
        "status": "Claim status: open, settled, reopened",
    },
    "risk_factors": {
        "risk_module": "SCR risk module: market, default, non_life, health, life",
        "risk_sub_module": "Sub-module within the risk module (e.g. equity, spread_bonds, premium_reserve)",
        "charge_eur": "Capital charge in EUR for this sub-module",
        "reporting_period": "Reporting quarter (format: YYYY-QN)",
        "description": "Human-readable description of the risk charge",
    },
    "own_funds": {
        "component": "Own funds component name (e.g. ordinary_share_capital, reconciliation_reserve)",
        "tier": "Tiering classification: 1 (unrestricted), 2, or 3",
        "amount_eur": "Amount in EUR",
        "reporting_period": "Reporting quarter (format: YYYY-QN)",
    },
    "balance_sheet": {
        "item": "Balance sheet line item (e.g. total_assets, technical_provisions_gross)",
        "category": "Classification: assets, liabilities, or equity",
        "amount_eur": "Amount in EUR",
        "reporting_period": "Reporting quarter (format: YYYY-QN)",
    },
    "scr_results": {
        "component": "SCR component name (e.g. SCR_market, BSCR, Op_risk, LAC_DT, SCR)",
        "amount_eur": "Amount in EUR",
        "description": "Human-readable description of the SCR component",
        "reporting_period": "Reporting quarter (format: YYYY-QN)",
        "model_version": "Model version used (e.g. Champion (v2025))",
        "calibration_year": "Calibration year of the Standard Formula parameters",
    },
    "s0501_summary": {
        "reporting_period": "Reporting quarter (format: YYYY-QN)",
        "lob_code": "Solvency II line of business code",
        "lob_name": "Full name of the line of business",
        "gross_written_premium": "Total gross written premium in EUR for this LoB and quarter",
        "net_earned_premium": "Total net earned premium in EUR",
        "net_incurred": "Total net claims incurred in EUR",
        "total_expenses": "Total expenses incurred in EUR",
        "loss_ratio_pct": "Loss ratio = net incurred / net earned premium × 100",
        "expense_ratio_pct": "Expense ratio = total expenses / net earned premium × 100",
        "combined_ratio_pct": "Combined ratio = loss ratio + expense ratio (below 100% = underwriting profit)",
        "ri_cession_rate_pct": "Reinsurance cession rate = (gross - net) / gross written premium × 100",
    },
    "s2501_summary": {
        "reporting_period": "Reporting quarter (format: YYYY-QN)",
        "model_version": "Standard Formula model version used",
        "scr_eur": "Solvency Capital Requirement in EUR",
        "bscr_eur": "Basic SCR (diversified aggregation of risk modules) in EUR",
        "eligible_own_funds_eur": "Eligible own funds in EUR (after tiering limits)",
        "solvency_ratio_pct": "Solvency ratio = eligible own funds / SCR × 100 (must be above 100%)",
        "surplus_eur": "Surplus = eligible own funds minus SCR in EUR",
        "mcr_eur": "Minimum Capital Requirement in EUR",
        "market_risk_eur": "Market risk SCR module charge in EUR",
        "non_life_uw_risk_eur": "Non-life underwriting risk SCR module charge in EUR",
    },
    "s2501_scr_breakdown": {
        "reporting_period": "Reporting quarter (format: YYYY-QN)",
        "template_row_id": "EIOPA S.25.01 template row reference (R0010=Market, R0100=BSCR, R0200=SCR)",
        "template_row_label": "Human-readable label for the template row",
        "amount_eur": "Amount in EUR for this SCR component",
        "model_version": "Standard Formula model version used",
    },
    "s0602_summary": {
        "reporting_period": "Reporting quarter",
        "cic_category_name": "CIC asset category (Government bonds, Corporate bonds, Equity, CIU, Property)",
        "asset_count": "Number of assets in this category",
        "total_sii_amount": "Total Solvency II value in EUR for this category",
        "pct_of_total_sii": "Percentage of total portfolio SII value",
        "avg_duration": "Average modified duration (bonds only)",
    },
}

# ── Execute ───────────────────────────────────────────────────────────

print("Adding table descriptions...")
for table, comment in TABLE_COMMENTS.items():
    escaped = comment.replace("'", "\\'")
    state = sql(f"COMMENT ON TABLE {FQN}.{table} IS '{escaped}'")
    status = "ok" if state == "SUCCEEDED" else state
    print(f"  {table}: {status}")

print("\nAdding column descriptions...")
for table, cols in COLUMN_COMMENTS.items():
    for col, comment in cols.items():
        escaped = comment.replace("'", "\\'")
        state = sql(f"ALTER TABLE {FQN}.{table} ALTER COLUMN {col} COMMENT '{escaped}'")
        status = "ok" if state == "SUCCEEDED" else state
        if status != "ok":
            print(f"  {table}.{col}: {status}")
    print(f"  {table}: {len(cols)} columns described")

print("\nDone.")
