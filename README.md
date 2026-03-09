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

## Deployment

Requires [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) with Asset Bundles support.

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run qrt_pipeline -t dev
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
| `catalog_name` | `lr_serverless_aws_us_catalog` | Unity Catalog (configurable — change to any catalog you have access to, e.g. `solvency2demo`) |
| `reporting_date` | `2025-12-31` | Solvency II reporting date |
| `entity_lei` | `5493001KJTIIGC8Y1R12` | Synthetic undertaking LEI |
| `entity_name` | `Europa Re Insurance SE` | Synthetic undertaking name |
