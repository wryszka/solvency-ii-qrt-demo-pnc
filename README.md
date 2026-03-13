# Solvency II QRT Demo (P&C)

Databricks-based demo generating synthetic P&C insurance data and producing EIOPA-aligned Quantitative Reporting Templates (QRT):

- **S.06.02** — List of Assets
- **S.05.01** — Premiums, Claims and Expenses by Line of Business
- **S.25.01** — Solvency Capital Requirement (Standard Formula)

## Architecture

```
Bronze (synthetic source data) → Silver (cleansed/aggregated) → Gold (EIOPA QRT format)
```

Output tables are designed for consumption by external reporting tools (e.g., Tagetik) via JDBC/ODBC or CSV export.

## Setup

### 1. Catalog Configuration

The default catalog is `lr_classic_aws_us_catalog`. To use a different catalog:

1. Edit `databricks.yml` — change `catalog_name` default (line 7)
2. That's it — all notebooks and jobs inherit from the bundle variable

The setup notebook (`00_setup.py`) will attempt `CREATE CATALOG IF NOT EXISTS`. If you don't have
permission to create catalogs, it falls back to using the catalog as-is. The schema (`solvency2demo`)
is created automatically.

### 2. Workspace Configuration

Edit `databricks.yml` — set `workspace.host` and `workspace.profile` to match your environment.

### 3. Deploy & Run

Requires [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) v0.200+ with Asset Bundles support.
All jobs use **serverless compute** — no cluster configuration needed.

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run qrt_pipeline -t dev
```

### Cleanup

```bash
databricks bundle destroy -t dev --auto-approve
```

## Repository Structure

```
├── databricks.yml              # DAB bundle configuration
├── resources/
│   └── qrt_pipeline_job.yml    # Workflow job definition
├── src/
│   ├── notebooks/              # Pipeline notebooks (00-08)
│   ├── config/                 # Reference data (CIC codes, LoB mapping, correlations)
│   └── app/                    # Databricks App (Phase 2)
└── tests/
```

## Parameters

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog_name` | `lr_classic_aws_us_catalog` | Unity Catalog (configurable — change to any catalog you have access to, e.g. `solvency2demo`) |
| `reporting_date` | `2025-12-31` | Solvency II reporting date |
| `entity_lei` | `5493001KJTIIGC8Y1R12` | Synthetic undertaking LEI |
| `entity_name` | `Bricksurance SE` | Synthetic undertaking name |
