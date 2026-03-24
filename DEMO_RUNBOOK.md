# Solvency II QRT Demo — Runbook

> **Bricksurance SE** — A mid-size European P&C insurer producing quarterly regulatory reports on Databricks.

This is your single reference for running the demo. It covers setup, the demo narrative, and links to every asset.

---

## Quick Links (update after deploy)

| Asset | URL |
|-------|-----|
| **App** | https://solvency2-qrt-7474659673789953.aws.databricksapps.com |
| **Dashboard** | https://fevm-lr-serverless-aws-us.cloud.databricks.com/dashboardsv3/01f1270282cd14fe8c155d26361eec82 |
| **Genie** | https://fevm-lr-serverless-aws-us.cloud.databricks.com/genie/rooms/01f12703e70110e5b4aeec0e5f7ee98c |
| **Workspace Notebooks** | https://fevm-lr-serverless-aws-us.cloud.databricks.com/#workspace/Workspace/Users/laurence.ryszka@databricks.com/Solvency%20II%20QRT%20Demo |
| **S.06.02 Pipeline** | https://fevm-lr-serverless-aws-us.cloud.databricks.com/pipelines/f197effe-b00d-4e25-a107-c526c3f7b81b |
| **S.05.01 Pipeline** | https://fevm-lr-serverless-aws-us.cloud.databricks.com/pipelines/be783357-612d-49ae-878b-3ddd2c9091e1 |
| **S.25.01 Pipeline** | https://fevm-lr-serverless-aws-us.cloud.databricks.com/pipelines/0284efb1-ea5c-4f00-bcca-ec167a1f5b87 |
| **S.26.06 Pipeline** | https://fevm-lr-serverless-aws-us.cloud.databricks.com/pipelines/249d6207-55d0-4e86-af47-391c1f9f1c10 |
| **S.26.06 Job** | https://fevm-lr-serverless-aws-us.cloud.databricks.com/#job/877567542678417 |
| **Model Registry** | https://fevm-lr-serverless-aws-us.cloud.databricks.com/explore/data/models/lr_serverless_aws_us_catalog/solvency2demo/standard_formula |
| **Schema** | `lr_serverless_aws_us_catalog.solvency2demo` (36 tables) |

---

## First-Time Setup (5 min)

```bash
# 1. Clone the repo
git clone https://github.com/wryszka/solvency-ii-qrt-demo-pnc.git
cd solvency-ii-qrt-demo-pnc

# 2. Authenticate the Databricks CLI
databricks auth login --profile DEFAULT

# 3. Run the deploy script (creates everything)
bash deploy_demo.sh

# The script will:
#   - Create workspace folders and upload notebooks
#   - Bootstrap Q1-Q3 synthetic data (~6 min)
#   - Deploy DAB bundle (4 DLT pipelines + 4 workflow jobs)
#   - Register the Standard Formula MLflow model
#   - Trigger all 4 QRT pipelines
#   - Add table/column descriptions
#   - Create the Lakeview dashboard (5 tabs)
#   - Create the Genie space (30 tables)
#   - Deploy the Databricks App with permissions

# 4. Note the asset URLs printed at the end — update the Quick Links above
```

**Override defaults:**
```bash
bash deploy_demo.sh --catalog my_catalog --schema my_schema --profile STAGING
```

---

## Teardown (2 min)

To completely remove everything:

1. Open `Solvency II QRT Demo / 00_Generate_Data / full_teardown` in the workspace
2. Set `confirm` = `yes`
3. Run all cells

Or from the CLI:
```bash
# Upload and run the teardown notebook
databricks workspace import "/Workspace/Users/$USER/teardown_tmp/full_teardown" \
    --file src/00_Generate_Data/full_teardown.py --format SOURCE --language PYTHON --overwrite
databricks jobs submit --json '{...}'  # see deploy_demo.sh for pattern
```

---

## Demo Narrative: "Maria's Morning" (~10 min)

### Setting

> It's mid-January. Q4 2025 just closed. **Maria** is the reporting actuary at Bricksurance SE. She needs to produce four QRTs and submit them to BaFin by end of month.

---

### Scene 1: Control Tower (1.5 min)

**Open the app** → click **Monitor**

> "First thing every morning during reporting season — have my data feeds arrived?"

**Show:**
- **KPI strip** at the top: feeds received, DQ pass rate, reconciliation checks, quarantined rows
- **Feed status cards**: each data feed with source system name, row count, SLA status
  - Reinsurance arrived 11 days early — "the RI team is always first"
  - Expenses arrived just in time — "finance usually cuts it close"
- **Cross-QRT Reconciliation** at the bottom: total assets match balance sheet, GWP matches premiums, own funds exceed SCR — all MATCH

> "In the old world I'd be sending emails asking 'has your data landed?' Now I open one page."

---

### Scene 2: Data Quality (1 min)

**Click** → **Data Quality** in the nav

> "Before I look at the numbers: how clean is the data?"

**Show:**
- **Overall pass rate**: 99.9% across 14 DLT expectations
- **Trend chart**: quality improving Q1→Q3 (quarantined rows declining)
- **Per-pipeline breakdown**: expand S.06.02 — 4 assets dropped for null IDs
- **Action badges**: `DROP ROW` (quarantined), `FAIL UPDATE` (blocks pipeline), `WARN` (logged)

> "Every expectation is defined in SQL right next to the transformation. An auditor can read it. Bad data doesn't propagate."

---

### Scene 3: DLT Pipeline (1 min)

**Open the S.05.01 DLT pipeline** (link from Quick Links above, or from the workspace)

> "Here's the pipeline graph. Three data streams — premiums, claims, expenses — flow through silver aggregation, merge into the EIOPA template, then produce the summary ratios."

**Click a node** (e.g., `premiums_by_lob`) → show:
- The SQL transformation
- `CONSTRAINT gross_written_positive EXPECT (gross_written_premium > 0) ON VIOLATION DROP ROW`

> "The DQ rules are embedded in the pipeline. If a premium is negative, it's dropped before reaching the QRT."

---

### Scene 4: QRT Reports (2 min)

**Back in the app** → **Reports** → click **S.25.01 — SCR Standard Formula**

**Content tab**: SCR waterfall — market risk, default, non-life, BSCR, op risk, final SCR.

**EIOPA Template tab**: Click it — shows the actual S.25.01 regulatory form with EIOPA row references (R0010-R0200). Click **Download PDF**.

> "This is exactly what BaFin will see."

**Period Comparison tab**: Side-by-side Q1, Q2, Q3.

> "If the regulator asks 'why did SCR change?' — I show them this."

**Reconciliation tab**: Cross-QRT checks — SCR vs own funds, asset counts.

> "Consistency across all four QRTs, checked automatically."

---

### Scene 5: Pipeline Lineage (1 min)

**Still on S.25.01** → click **Lineage** tab

> "The full audit trail. Every step from ingestion to export, with the actual SQL and DQ expectations."

**Show:**
- 4 phases: Ingestion → Transformation → Confirmation → Export
- The **Model** step (step 3) shows: "Standard Formula Champion from Unity Catalog"
- Click **Show SQL** on any step

**Switch to S.05.01 → Lineage** to show the richer DAG:
- 3 parallel ingestion streams (premiums, claims, expenses)
- 3 parallel silver transformations
- Merge into gold EIOPA template

---

### Scene 6: Stochastic Modelling — S.26.06 (1.5 min)

**Back to Reports** → click **S.26.06 — NL Underwriting Risk**

> "This one is different. It has a stochastic modelling step."

**Click Lineage tab** → show the unique pipeline:
- **Preparation phase**: Exposures exported as CSV to a Unity Catalog Volume
- **Stochastic phase**: "Igloo 5.2.1 — 10,000 Monte Carlo simulations, 7 perils"
- Click **Show SQL** on the Igloo step — shows the export/import code

> "In production, that CSV goes to the Igloo server. The results come back. Databricks orchestrates the handoff and imports the stochastic output."

**Content tab**: NL UW risk breakdown — premium risk, reserve risk, cat risk, diversification.

---

### Scene 7: Model Governance (1 min)

**On S.25.01** → click **Model Governance** tab

> "Which version of the model did we use? The question every actuary dreads."

**Show:**
- **Champion** (v1, 2025 calibration): SCR = EUR 549M — this is production
- **Challenger** (v2, 2026 calibration): SCR = EUR 560M — pending for next year
- **Impact analysis**: +2% SCR driven by tighter correlations and higher op risk

> "Both versions are in Unity Catalog with full audit trail. Promoting the Challenger is one alias change."

**Open the Model Registry** (Quick Links) → show v1/v2, Champion alias

---

### Scene 8: Approval + Certificate (1 min)

**On S.25.01** → click **Approve / Export** tab

1. Click **Submit for Review**
2. Add comment: *"Validated against actuarial function report. Property combined ratio spike explained by December fire claim."*
3. Click **Approve & Export to Tagetik**
4. Click **Generate Certificate**

> "A PDF certificate with the approval timestamp, reviewer, and SHA-256 hash of the data. The CSV is exported to the regulatory volume. Ready for XBRL."

---

### Scene 9: Dashboard (30s)

**Click Dashboard** in the nav → **Open Dashboard**

> "The CFO wants a single view. Four tabs: Overview with solvency ratio trend, then drill into each QRT."

Show the 5-tab Lakeview dashboard. Point out the Pipeline & DQ tab.

---

### Scene 10: Ask Genie (30s)

**Click Ask Genie** → **Open Genie**

Type: *"What is the solvency ratio for Q3 2025?"*

> "Two weeks later, the CFO has a question. No SQL skills needed."

---

### Closing

> "What you just saw: from data arrival monitoring, through automated pipelines, stochastic modelling, quality gates, model governance, EIOPA template production, to approval and export — all on one platform.
>
> No Excel. No email chains. No 'which version did we use.'
>
> The actuary's day goes from chasing data to actually reviewing it."

---

## What's Under the Hood

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ Bronze (15 tables)        │ Generated by generate_data.py          │
│ assets, premiums, claims, │ Synthetic but realistic P&C data       │
│ expenses, risk_factors,   │ for Bricksurance SE                    │
│ own_funds, exposures ...  │                                        │
├───────────────────────────┼────────────────────────────────────────┤
│ Silver (DLT)              │ Aggregation + validation               │
│ assets_enriched,          │ DLT expectations = quality gates       │
│ premiums_by_lob,          │ Bad data quarantined, not propagated   │
│ cat_risk_by_lob ...       │                                        │
├───────────────────────────┼────────────────────────────────────────┤
│ Gold (DLT)                │ EIOPA template mapping                 │
│ s0602_list_of_assets,     │ Cell references (C0040, R0110 etc.)    │
│ s0501_premiums_claims_... │ Directly maps to regulatory forms      │
│ s2501_scr_breakdown,      │                                        │
│ s2606_nl_uw_risk          │                                        │
├───────────────────────────┼────────────────────────────────────────┤
│ Summary (DLT)             │ Sign-off views for actuaries           │
│ s0602_summary,            │ Ratios, totals, reconciliation checks  │
│ s0501_summary,            │                                        │
│ s2501_summary,            │                                        │
│ s2606_summary             │                                        │
├───────────────────────────┼────────────────────────────────────────┤
│ Monitoring (4 tables)     │ pipeline_sla_status,                   │
│                           │ dq_expectation_results,                │
│                           │ cross_qrt_reconciliation,              │
│                           │ model_registry_log                     │
└───────────────────────────┴────────────────────────────────────────┘
```

### QRT Pipelines

| QRT | Pipeline | Steps | Unique Feature |
|-----|----------|-------|----------------|
| S.06.02 | assets → enriched → EIOPA template → summary | 5 | CIC decomposition |
| S.05.01 | 3 parallel streams (premiums/claims/expenses) → merge → EIOPA → ratios | 8 | Fan-in from 3 sources |
| S.25.01 | risk_factors → MLflow model → EIOPA template → solvency ratio | 6 | Unity Catalog model |
| S.26.06 | exposures → Igloo (Volume CSV) → cat risk + prem/res risk → EIOPA | 9 | Stochastic model handoff |

### App Pages

| Page | URL Path | Purpose |
|------|----------|---------|
| Monitor | `/monitor` | Control Tower — feed SLA, DQ overview, reconciliation |
| Reports | `/` | QRT list with status and key metrics |
| Report Detail | `/report/{id}` | 8 tabs: Content, Quality, Comparison, Reconciliation, Template, Lineage, Model Governance*, Approval |
| Data Quality | `/data-quality` | DQ expectations by pipeline, trend chart |
| Dashboard | `/dashboard` | Link to Lakeview (5 tabs) |
| Ask Genie | `/genie` | Link to Genie space (30 tables) |

*Model Governance tab only appears on S.25.01

### Key Files

```
deploy_demo.sh                    — One-click deploy (creates everything)
src/00_Generate_Data/
  generate_data.py                — Synthetic data (19 table types)
  bootstrap_archive.py            — Runs Q1-Q3 generation
  full_teardown.py                — Removes everything
  teardown.py                     — Schema-only teardown
src/01_QRT_S0602_Assets/          — 3 DLT SQL notebooks
src/02_QRT_S0501_PnL/             — 5 DLT SQL notebooks
src/03_QRT_S2501_SCR/             — Model + 2 DLT SQL notebooks
src/04_QRT_S2606_NL_Risk/         — Igloo mock + 4 DLT SQL notebooks
resources/qrt_s*.yml              — Pipeline + job definitions (DAB)
src/app/                          — Databricks App (FastAPI + React)
scripts/create_dashboard.py       — Lakeview dashboard generator
scripts/add_descriptions.py       — Table/column descriptions
databricks.yml                    — DAB bundle config
```

---

## Troubleshooting

**App shows empty data / "?" for metrics:**
The app's service principal needs `USE CATALOG`, `ALL PRIVILEGES ON SCHEMA`, and `CAN_USE` on the SQL warehouse. The deploy script grants these automatically, but if you recreated the app, re-run the grant commands.

**Dashboard shows "select fields to visualize":**
The dashboard was created but not published, or `queryLines` have issues. Re-run: `python3 scripts/create_dashboard.py <dashboard_id>`

**Genie says "no tables available":**
Tables must be in `serialized_space.data_sources.tables[].identifier` (sorted alphabetically). The deploy script handles this. Max 30 tables per space.

**S.26.06 pipeline fails:**
The `run_igloo` task needs the `exposures` and `igloo_results` tables to exist (created by `generate_data.py`). Run `bootstrap_archive.py` first.

**Pipeline shows "NOTEBOOK_NOT_FOUND":**
Run `databricks bundle deploy` to sync notebooks to the `.bundle` workspace path. Clear local sync state first: `rm -rf .databricks/bundle/dev/sync-snapshots .databricks/bundle/dev/fileset-snapshots`
