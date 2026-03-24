# Databricks notebook source
# MAGIC %md
# MAGIC # Solvency II QRT Demo — Interactive Walkthrough
# MAGIC
# MAGIC **Bricksurance SE** — A mid-size European P&C insurer producing quarterly regulatory reports on Databricks.
# MAGIC
# MAGIC This notebook walks through the entire demo interactively. Run each cell to explore the data,
# MAGIC pipelines, models, and QRT outputs. Use this alongside the app and dashboard for the full experience.
# MAGIC
# MAGIC ## Quick Links (update after your deploy)
# MAGIC
# MAGIC | Asset | URL |
# MAGIC |-------|-----|
# MAGIC | **App** | [solvency2-qrt](https://solvency2-qrt-7474659673789953.aws.databricksapps.com) |
# MAGIC | **Dashboard** | [Lakeview Dashboard](https://fevm-lr-serverless-aws-us.cloud.databricks.com/dashboardsv3/01f1270282cd14fe8c155d26361eec82) |
# MAGIC | **Genie** | [Ask Genie](https://fevm-lr-serverless-aws-us.cloud.databricks.com/genie/rooms/01f12703e70110e5b4aeec0e5f7ee98c) |
# MAGIC | **Model Registry** | [standard_formula](https://fevm-lr-serverless-aws-us.cloud.databricks.com/explore/data/models/lr_serverless_aws_us_catalog/solvency2demo/standard_formula) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## End-to-End Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                           BRICKSURANCE SE — Solvency II on Databricks                          │
# MAGIC └──────────────────────────────────────────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC  SOURCE SYSTEMS                    DATABRICKS PLATFORM                           CONSUMERS
# MAGIC ┌──────────────┐     ┌──────────────────────────────────────────────────────┐  ┌──────────────┐
# MAGIC │              │     │                                                      │  │              │
# MAGIC │  Simcorp     │────>│  BRONZE (15 tables)                                  │  │  Databricks  │
# MAGIC │  (Investments)│     │  assets, premiums, claims, expenses,                 │  │  App         │
# MAGIC │              │     │  risk_factors, own_funds, exposures,                  │  │  ┌────────┐  │
# MAGIC ├──────────────┤     │  igloo_results, counterparties, ...                   │  │  │Monitor │  │
# MAGIC │              │     │                                                      │  │  │Reports │  │
# MAGIC │  Guidewire   │────>│         │              │              │               │  │  │DQ      │  │
# MAGIC │  (Policies)  │     │         ▼              ▼              ▼               │  │  │Approve │  │
# MAGIC │              │     │                                                      │  │  └────────┘  │
# MAGIC ├──────────────┤     │  SILVER (DLT Pipelines + Expectations)               │  │              │
# MAGIC │              │     │  ┌─────────────┐ ┌─────────────┐ ┌───────────────┐   │  ├──────────────┤
# MAGIC │  SAP / ERP   │────>│  │ S.06.02     │ │ S.05.01     │ │ S.25.01       │   │  │              │
# MAGIC │  (Finance)   │     │  │ assets_     │ │ premiums_   │ │ Standard      │   │  │  Lakeview    │
# MAGIC │              │     │  │ enriched    │ │ by_lob      │ │ Formula Model │   │  │  Dashboard   │
# MAGIC ├──────────────┤     │  │ (CIC, SII)  │ │ claims_     │ │ (MLflow UC)   │   │  │  (5 tabs)    │
# MAGIC │              │     │  │             │ │ by_lob      │ │ Champion v1   │   │  │              │
# MAGIC │  Igloo 5.2.1 │<──>│  │             │ │ expenses_   │ │ Challenger v2 │   │  ├──────────────┤
# MAGIC │  (Cat Model) │     │  │             │ │ by_lob      │ │               │   │  │              │
# MAGIC │  10K sims    │     │  └──────┬──────┘ └──────┬──────┘ └───────┬───────┘   │  │  Genie       │
# MAGIC │              │     │         │              │              │               │  │  AI Q&A      │
# MAGIC ├──────────────┤     │  ┌──────┴──────┐       │    ┌─────────┴─────────┐    │  │  (30 tables) │
# MAGIC │              │     │  │ S.26.06     │       │    │                   │    │  │              │
# MAGIC │  Risk Engine │────>│  │ cat_risk +  │       │    │                   │    │  ├──────────────┤
# MAGIC │  (Igloo/RAFM)│     │  │ prem_res    │       │    │                   │    │  │              │
# MAGIC │              │     │  │ risk        │       │    │                   │    │  │  BaFin       │
# MAGIC └──────────────┘     │  └──────┬──────┘       │    │                   │    │  │  (Regulator) │
# MAGIC                      │         │              │    │                   │    │  │              │
# MAGIC                      │         ▼              ▼    ▼                   │    │  │  CSV + PDF   │
# MAGIC                      │                                                │    │  │  XBRL        │
# MAGIC                      │  GOLD (EIOPA Template Mapping)                 │    │  │              │
# MAGIC                      │  ┌─────────────┐ ┌─────────────┐ ┌───────────┐│    │  └──────────────┘
# MAGIC                      │  │ s0602_list  │ │ s0501_prem  │ │ s2501_scr ││    │
# MAGIC                      │  │ _of_assets  │ │ _claims_exp │ │ _breakdown││    │
# MAGIC                      │  │ (C0040-370) │ │ (R0110-1200)│ │(R0010-200)││    │
# MAGIC                      │  └──────┬──────┘ └──────┬──────┘ └─────┬─────┘│    │
# MAGIC                      │         │              │            │         │    │
# MAGIC                      │  ┌──────┴──────┐ ┌─────┴───────┐ ┌─┴───────┐ │    │
# MAGIC                      │  │ s2606_nl_uw │ │             │ │         │ │    │
# MAGIC                      │  │ _risk       │ │             │ │         │ │    │
# MAGIC                      │  │ (R0010-110) │ │             │ │         │ │    │
# MAGIC                      │  └──────┬──────┘ │             │ │         │ │    │
# MAGIC                      │         │        │             │ │         │ │    │
# MAGIC                      │         ▼        ▼             ▼ ▼         │    │
# MAGIC                      │                                                │    │
# MAGIC                      │  SUMMARY (Actuarial Sign-off Views)            │    │
# MAGIC                      │  s0602_summary  s0501_summary  s2501_summary   │    │
# MAGIC                      │  s2606_summary  (ratios, totals, solvency)     │    │
# MAGIC                      │                                                │    │
# MAGIC                      │         │                                      │    │
# MAGIC                      │         ▼                                      │    │
# MAGIC                      │                                                │    │
# MAGIC                      │  MONITORING & GOVERNANCE                       │    │
# MAGIC                      │  ┌──────────────┐ ┌──────────────┐             │    │
# MAGIC                      │  │ SLA Status   │ │ DQ Results   │             │    │
# MAGIC                      │  │ (feed times) │ │ (expectations│             │    │
# MAGIC                      │  ├──────────────┤ ├──────────────┤             │    │
# MAGIC                      │  │ Cross-QRT    │ │ Model        │             │    │
# MAGIC                      │  │ Reconciliatn │ │ Registry Log │             │    │
# MAGIC                      │  └──────────────┘ └──────────────┘             │    │
# MAGIC                      │                                                │    │
# MAGIC                      │         │                                      │    │
# MAGIC                      │         ▼                                      │    │
# MAGIC                      │                                                │    │
# MAGIC                      │  APPROVAL & EXPORT                             │    │
# MAGIC                      │  ┌─────────────────────────────────────────┐   │    │
# MAGIC                      │  │ Submit → Review → Approve → Export      │   │    │
# MAGIC                      │  │                                         │   │    │
# MAGIC                      │  │ qrt_approvals table (audit trail)       │   │    │
# MAGIC                      │  │ regulatory_exports/ Volume (CSV + PDF)  │   │    │
# MAGIC                      │  │ Approval certificate (SHA-256 hash)     │   │    │
# MAGIC                      │  └─────────────────────────────────────────┘   │    │
# MAGIC                      │                                                │    │
# MAGIC                      └────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Four QRT Pipelines
# MAGIC
# MAGIC | QRT | Pipeline | Key Feature | Tables |
# MAGIC |-----|----------|-------------|--------|
# MAGIC | **S.06.02** | assets → enriched → EIOPA template | CIC decomposition, SII valuation | 5K assets |
# MAGIC | **S.05.01** | 3 parallel streams → merge → ratios | Fan-in from premiums + claims + expenses | 144 template rows |
# MAGIC | **S.25.01** | risk_factors → MLflow model → template | Standard Formula from Unity Catalog | 17 SCR components |
# MAGIC | **S.26.06** | exposures → Igloo → cat + prem/res risk | Stochastic model with Volume CSV exchange | 10K simulations |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

CATALOG = "lr_serverless_aws_us_catalog"
SCHEMA = "solvency2demo"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Show all tables
tables = [r.tableName for r in spark.sql("SHOW TABLES").collect()]
print(f"Schema: {CATALOG}.{SCHEMA}")
print(f"Tables: {len(tables)}")
for t in sorted(tables):
    cnt = spark.table(t).count()
    print(f"  {t:40s} {cnt:>10,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Scene 1: Control Tower — Has My Data Arrived?
# MAGIC
# MAGIC > "First thing every morning during reporting season — check if all data feeds are in."

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   feed_name,
# MAGIC   source_system,
# MAGIC   status,
# MAGIC   ROUND(dq_pass_rate * 100, 1) AS dq_pass_pct,
# MAGIC   row_count,
# MAGIC   notes
# MAGIC FROM pipeline_sla_status
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM pipeline_sla_status)
# MAGIC ORDER BY feed_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross-QRT Reconciliation
# MAGIC > "Do the numbers add up across all QRTs?"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   source_qrt,
# MAGIC   target_qrt,
# MAGIC   check_description,
# MAGIC   ROUND(source_value / 1e6, 1) AS source_eur_m,
# MAGIC   ROUND(target_value / 1e6, 1) AS target_eur_m,
# MAGIC   ROUND(difference / 1e6, 1) AS diff_eur_m,
# MAGIC   status
# MAGIC FROM cross_qrt_reconciliation
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM cross_qrt_reconciliation)
# MAGIC ORDER BY check_name

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Scene 2: Data Quality — DLT Expectations
# MAGIC
# MAGIC > "How clean is the data? What got quarantined?"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   pipeline_name,
# MAGIC   table_name,
# MAGIC   expectation_name,
# MAGIC   total_records,
# MAGIC   passing_records,
# MAGIC   failing_records,
# MAGIC   ROUND(pass_rate * 100, 1) AS pass_rate_pct,
# MAGIC   action
# MAGIC FROM dq_expectation_results
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM dq_expectation_results)
# MAGIC ORDER BY pipeline_name, table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ Trend: Is quality improving over time?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   reporting_period,
# MAGIC   SUM(total_records) AS total_records,
# MAGIC   SUM(failing_records) AS quarantined_rows,
# MAGIC   ROUND(SUM(passing_records) * 100.0 / SUM(total_records), 2) AS pass_rate_pct
# MAGIC FROM dq_expectation_results
# MAGIC GROUP BY reporting_period
# MAGIC ORDER BY reporting_period

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Scene 3: S.06.02 — List of Assets
# MAGIC
# MAGIC > "5,000 investment positions enriched with CIC codes, SII valuations, and credit quality."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver: CIC Decomposition + SII Valuation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   cic_category_name,
# MAGIC   COUNT(*) AS asset_count,
# MAGIC   ROUND(SUM(sii_value) / 1e6, 1) AS total_sii_eur_m,
# MAGIC   ROUND(SUM(sii_value) * 100.0 / SUM(SUM(sii_value)) OVER (), 1) AS pct_of_total,
# MAGIC   ROUND(AVG(modified_duration), 1) AS avg_duration
# MAGIC FROM assets_enriched
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM assets_enriched)
# MAGIC GROUP BY cic_category_name
# MAGIC ORDER BY total_sii_eur_m DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold: EIOPA S.06.02 Template (first 10 rows)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   C0040_Asset_ID,
# MAGIC   C0190_Item_Title,
# MAGIC   C0270_CIC,
# MAGIC   C0170_Total_Solvency_II_Amount,
# MAGIC   C0290_External_Rating,
# MAGIC   C0310_Credit_Quality_Step,
# MAGIC   C0340_Duration
# MAGIC FROM s0602_list_of_assets
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM s0602_list_of_assets)
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Scene 4: S.05.01 — Premiums, Claims & Expenses
# MAGIC
# MAGIC > "Three data streams merge into one regulatory template. The richest pipeline."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver: Premium, Claims, Expenses aggregation by LoB

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   p.lob_name,
# MAGIC   ROUND(p.gross_written_premium / 1e6, 1) AS gwp_m,
# MAGIC   ROUND(c.gross_incurred / 1e6, 1) AS claims_incurred_m,
# MAGIC   ROUND(e.total_expenses / 1e6, 1) AS expenses_m
# MAGIC FROM premiums_by_lob p
# MAGIC JOIN claims_by_lob c ON p.reporting_period = c.reporting_period AND p.lob_code = c.lob_code
# MAGIC JOIN expenses_by_lob e ON p.reporting_period = e.reporting_period AND p.lob_code = e.lob_code
# MAGIC WHERE p.reporting_period = (SELECT MAX(reporting_period) FROM premiums_by_lob)
# MAGIC ORDER BY p.lob_code

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold: Key P&L Ratios
# MAGIC > "Combined ratio below 100% = underwriting profit."

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   lob_name,
# MAGIC   ROUND(gross_written_premium / 1e6, 1) AS gwp_m,
# MAGIC   loss_ratio_pct,
# MAGIC   expense_ratio_pct,
# MAGIC   combined_ratio_pct,
# MAGIC   ri_cession_rate_pct
# MAGIC FROM s0501_summary
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM s0501_summary)
# MAGIC ORDER BY lob_code

# COMMAND ----------

# MAGIC %md
# MAGIC ### Period Comparison: How did ratios change?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   reporting_period,
# MAGIC   ROUND(SUM(gross_written_premium) / 1e6, 1) AS total_gwp_m,
# MAGIC   ROUND(AVG(combined_ratio_pct), 1) AS avg_combined_ratio,
# MAGIC   ROUND(AVG(loss_ratio_pct), 1) AS avg_loss_ratio
# MAGIC FROM s0501_summary
# MAGIC GROUP BY reporting_period
# MAGIC ORDER BY reporting_period

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Scene 5: S.25.01 — SCR Standard Formula
# MAGIC
# MAGIC > "The model is in Unity Catalog. We know exactly which version produced these numbers."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Governance: Champion vs Challenger

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   reporting_period,
# MAGIC   model_version,
# MAGIC   alias,
# MAGIC   calibration_year,
# MAGIC   ROUND(scr_result_eur / 1e6, 1) AS scr_eur_m,
# MAGIC   description
# MAGIC FROM model_registry_log
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM model_registry_log)
# MAGIC ORDER BY model_version

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold: SCR Waterfall

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   template_row_id,
# MAGIC   template_row_label,
# MAGIC   ROUND(amount_eur / 1e6, 1) AS amount_eur_m
# MAGIC FROM s2501_scr_breakdown
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM s2501_scr_breakdown)
# MAGIC   AND template_row_id NOT LIKE '%.%'  -- main components only
# MAGIC ORDER BY template_row_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solvency Position

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   reporting_period,
# MAGIC   ROUND(scr_eur / 1e6, 1) AS scr_m,
# MAGIC   ROUND(eligible_own_funds_eur / 1e6, 1) AS eligible_own_funds_m,
# MAGIC   solvency_ratio_pct,
# MAGIC   ROUND(surplus_eur / 1e6, 1) AS surplus_m,
# MAGIC   model_version
# MAGIC FROM s2501_summary
# MAGIC ORDER BY reporting_period

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Scene 6: S.26.06 — NL Underwriting Risk (Stochastic)
# MAGIC
# MAGIC > "This QRT has a stochastic modelling step. Exposures go to Igloo, results come back."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Exposures sent to Igloo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   lob_name,
# MAGIC   peril,
# MAGIC   number_of_risks,
# MAGIC   ROUND(total_sum_insured_eur / 1e6, 1) AS tsi_eur_m,
# MAGIC   ROUND(aggregate_limit_eur / 1e6, 1) AS limit_eur_m
# MAGIC FROM exposures
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM exposures)
# MAGIC ORDER BY lob_name, peril
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Igloo Run Log
# MAGIC > "Full audit trail of every stochastic run."

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   run_id,
# MAGIC   reporting_period,
# MAGIC   model_version,
# MAGIC   num_simulations,
# MAGIC   exposure_count,
# MAGIC   result_count,
# MAGIC   status,
# MAGIC   input_path,
# MAGIC   output_path
# MAGIC FROM igloo_run_log
# MAGIC ORDER BY started_at DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Stochastic Results — VaR by LoB at 1-in-200

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   lob_name,
# MAGIC   ROUND(var_net_eur / 1e6, 1) AS var_net_eur_m,
# MAGIC   ROUND(tvar_net_eur / 1e6, 1) AS tvar_net_eur_m,
# MAGIC   perils_modelled
# MAGIC FROM cat_risk_by_lob
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM cat_risk_by_lob)
# MAGIC ORDER BY var_net_eur DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Premium & Reserve Risk (EIOPA Standard Formula factors)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   lob_name,
# MAGIC   ROUND(volume_measure_eur / 1e6, 1) AS volume_m,
# MAGIC   sigma_premium,
# MAGIC   sigma_reserve,
# MAGIC   ROUND(premium_risk_eur / 1e6, 1) AS prem_risk_m,
# MAGIC   ROUND(reserve_risk_eur / 1e6, 1) AS res_risk_m
# MAGIC FROM premium_reserve_risk
# MAGIC ORDER BY lob_code

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold: S.26.06 NL UW Risk — Diversified Total

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   template_row_id,
# MAGIC   template_row_label,
# MAGIC   ROUND(amount_eur / 1e6, 1) AS amount_eur_m
# MAGIC FROM s2606_nl_uw_risk
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM s2606_nl_uw_risk)
# MAGIC ORDER BY template_row_id

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Scene 7: The Big Picture — Cross-QRT Consistency
# MAGIC
# MAGIC > "One query proves all four QRTs share the same data foundation."

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'S.06.02 Total Assets' AS metric,
# MAGIC   ROUND(SUM(CAST(C0170_Total_Solvency_II_Amount AS DOUBLE)) / 1e6, 1) AS value_eur_m,
# MAGIC   reporting_period
# MAGIC FROM s0602_list_of_assets
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM s0602_list_of_assets)
# MAGIC GROUP BY reporting_period
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'S.05.01 Total GWP',
# MAGIC   ROUND(SUM(CAST(amount_eur AS DOUBLE)) / 1e6, 1),
# MAGIC   reporting_period
# MAGIC FROM s0501_premiums_claims_expenses
# MAGIC WHERE template_row_id = 'R0110' AND lob_code = 0
# MAGIC   AND reporting_period = (SELECT MAX(reporting_period) FROM s0501_premiums_claims_expenses)
# MAGIC GROUP BY reporting_period
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'S.25.01 SCR',
# MAGIC   ROUND(scr_eur / 1e6, 1),
# MAGIC   reporting_period
# MAGIC FROM s2501_summary
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM s2501_summary)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'S.25.01 Solvency Ratio %',
# MAGIC   solvency_ratio_pct,
# MAGIC   reporting_period
# MAGIC FROM s2501_summary
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM s2501_summary)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'S.26.06 NL UW SCR',
# MAGIC   ROUND(total_nl_uw_scr / 1e6, 1),
# MAGIC   reporting_period
# MAGIC FROM s2606_summary
# MAGIC WHERE reporting_period = (SELECT MAX(reporting_period) FROM s2606_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Scene 8: Approval Workflow
# MAGIC
# MAGIC > "Open the app, click Approve / Export on any QRT. Submit for review, approve, generate certificate."
# MAGIC
# MAGIC **App URL:** [solvency2-qrt](https://solvency2-qrt-7474659673789953.aws.databricksapps.com)
# MAGIC
# MAGIC The app handles:
# MAGIC - Submit for review (creates pending approval record)
# MAGIC - Approve with comments (exports CSV to Volume, generates PDF certificate)
# MAGIC - Full audit trail in `qrt_approvals` table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if any approvals exist
# MAGIC SELECT * FROM qrt_approvals ORDER BY submitted_at DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check regulatory exports volume

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/lr_serverless_aws_us_catalog/solvency2demo/regulatory_exports/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Igloo exchange volume

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/lr_serverless_aws_us_catalog/solvency2demo/igloo_exchange/'

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC | What | Databricks Feature | Demo Point |
# MAGIC |------|-------------------|------------|
# MAGIC | Data arrives late | **Control Tower** — pipeline_sla_status table | SLA monitoring |
# MAGIC | Manual Excel transforms | **DLT Pipelines** — declarative SQL with expectations | Automated quality gates |
# MAGIC | "Which model version?" | **MLflow + Unity Catalog** — Champion/Challenger aliases | Model governance |
# MAGIC | Cross-QRT mismatch | **Single SQL query** — join across gold tables | Cross-QRT consistency |
# MAGIC | Regulator asks "why?" | **Period comparison + lineage** — drill to transformation | Full audit trail |
# MAGIC | "Who approved this?" | **Approval workflow** — timestamps, PDF certificate | Regulatory compliance |
# MAGIC | Validation rules fail | **DLT Expectations** — DROP ROW / FAIL UPDATE / WARN | Automated DQ |
# MAGIC | Stochastic model run | **Volume exchange** — CSV export/import with Igloo | External model orchestration |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Open the [App](https://solvency2-qrt-7474659673789953.aws.databricksapps.com) and walk through the demo narrative
# MAGIC - Open the [Dashboard](https://fevm-lr-serverless-aws-us.cloud.databricks.com/dashboardsv3/01f1270282cd14fe8c155d26361eec82) for the visual overview
# MAGIC - Ask [Genie](https://fevm-lr-serverless-aws-us.cloud.databricks.com/genie/rooms/01f12703e70110e5b4aeec0e5f7ee98c) a question
