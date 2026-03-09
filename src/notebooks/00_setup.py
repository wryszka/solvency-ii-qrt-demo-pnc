# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: Schemas, Volumes, and Lineage Audit Infrastructure
# MAGIC Creates the medallion layer schemas, export volume, and pipeline lineage audit table
# MAGIC for the Solvency II QRT demo. The lineage table provides a complete, exportable audit
# MAGIC trail of every data transformation — what/where/when/who — for regulatory compliance.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
catalog = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# Try to create the catalog (works if user has CREATE CATALOG permission).
# If not, assume the catalog already exists (e.g. workspace default catalog).
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT 'Solvency II QRT reporting demo — synthetic P&C insurer data, EIOPA-aligned reports, and audit lineage.'")
    print(f"Catalog '{catalog}' created or already exists.")
except Exception as e:
    if "PERMISSION_DENIED" in str(e) or "does not exist" in str(e):
        print(f"Cannot create catalog '{catalog}' (permission denied) — assuming it already exists.")
    else:
        raise
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas

# COMMAND ----------

# Bronze - raw synthetic source data
spark.sql("CREATE SCHEMA IF NOT EXISTS qrt_demo_bronze COMMENT 'Raw synthetic source data simulating insurer operational systems (policy admin, claims, investments, actuarial). No transformations applied.'")

# Silver - cleansed and aggregated
spark.sql("CREATE SCHEMA IF NOT EXISTS qrt_demo_silver COMMENT 'Cleansed, validated, and aggregated insurance data. Business logic applied (CIC decomposition, LoB aggregation, SCR correlation). Referential integrity enforced.'")

# Gold - EIOPA QRT-aligned output tables
spark.sql("CREATE SCHEMA IF NOT EXISTS qrt_demo_gold COMMENT 'EIOPA-aligned Quantitative Reporting Templates (QRT) ready for export to Tagetik or XBRL filing. Column naming follows EIOPA taxonomy cell references.'")

# Audit - pipeline lineage and governance
spark.sql("CREATE SCHEMA IF NOT EXISTS qrt_demo_audit COMMENT 'Pipeline lineage audit trail and data quality logs. Provides exportable what/where/when/who traceability for every transformation step in the QRT pipeline.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Export Volume

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS qrt_demo_gold.exports COMMENT 'CSV/XBRL export files for Tagetik and external reporting tool ingestion.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Pipeline Lineage Audit Table
# MAGIC This table records every pipeline execution step with full traceability:
# MAGIC - **What**: source tables, target table, transformation logic
# MAGIC - **Where**: notebook path, workspace, catalog/schema
# MAGIC - **When**: execution timestamp, reporting period
# MAGIC - **Who**: executed by (user), pipeline run ID

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.qrt_demo_audit.pipeline_lineage (
    lineage_id          STRING      COMMENT 'Unique lineage record ID (UUID). Primary key.',
    pipeline_run_id     STRING      COMMENT 'Pipeline execution run ID. Groups all steps from a single pipeline execution. Shared across notebooks in the same workflow run.',
    step_name           STRING      COMMENT 'Pipeline step identifier matching notebook name (e.g. 01_generate_bronze_data, 02_silver_assets).',
    step_sequence       INT         COMMENT 'Execution order within the pipeline (0=setup, 1=bronze generation, 2-4=silver, 5-7=gold, 8=validation).',
    source_tables       STRING      COMMENT 'Comma-separated fully-qualified source table names consumed by this step. Empty for data generation steps.',
    target_table        STRING      COMMENT 'Fully-qualified target table name produced by this step.',
    transformation_type STRING      COMMENT 'Category of transformation: generation, cleansing, aggregation, enrichment, formatting, validation.',
    transformation_desc STRING      COMMENT 'Human-readable description of the transformation logic applied.',
    row_count_in        LONG        COMMENT 'Total input row count across all source tables. Null for generation steps.',
    row_count_out       LONG        COMMENT 'Output row count written to the target table.',
    columns_in          STRING      COMMENT 'JSON array of input column names consumed from source tables.',
    columns_out         STRING      COMMENT 'JSON array of output column names written to the target table.',
    parameters          STRING      COMMENT 'JSON object of pipeline parameters used (catalog, reporting_date, seed, scale_factor).',
    notebook_path       STRING      COMMENT 'Full workspace path of the notebook that executed this step.',
    workspace_url       STRING      COMMENT 'Databricks workspace URL where the step was executed.',
    catalog_name        STRING      COMMENT 'Unity Catalog name used for this execution.',
    schema_name         STRING      COMMENT 'Target schema name (qrt_demo_bronze, qrt_demo_silver, qrt_demo_gold).',
    reporting_period    STRING      COMMENT 'Solvency II reporting period in YYYY-QN format.',
    executed_by         STRING      COMMENT 'Username or service principal that executed this step.',
    executed_at         TIMESTAMP   COMMENT 'UTC timestamp when this step completed.',
    duration_seconds    DOUBLE      COMMENT 'Wall-clock execution duration in seconds for this step.',
    status              STRING      COMMENT 'Execution status: success, failed, or skipped.',
    error_message       STRING      COMMENT 'Error message if status is failed. Null on success.',
    data_quality_checks STRING      COMMENT 'JSON array of data quality check results (check_name, passed, details). Populated by validation steps.',
    checksum            STRING      COMMENT 'SHA-256 hash of the output table content for tamper detection and reproducibility verification.'
) USING DELTA
COMMENT 'Complete pipeline lineage audit trail for Solvency II QRT reporting. Each row represents one transformation step with full what/where/when/who traceability. Designed for regulatory audit, data governance, and exportable lineage documentation. Append-only — historical runs are preserved.'
TBLPROPERTIES (
    'delta.appendOnly' = 'true',
    'quality' = 'audit',
    'pipeline' = 'solvency-ii-qrt-pnc'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Data Quality Log Table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.qrt_demo_audit.data_quality_log (
    check_id            STRING      COMMENT 'Unique quality check ID (UUID). Primary key.',
    pipeline_run_id     STRING      COMMENT 'Pipeline run ID linking to pipeline_lineage.pipeline_run_id.',
    step_name           STRING      COMMENT 'Pipeline step that performed the check.',
    table_name          STRING      COMMENT 'Fully-qualified table name being checked.',
    check_name          STRING      COMMENT 'Name of the quality check (e.g. row_count, null_check, referential_integrity, sum_reconciliation).',
    check_category      STRING      COMMENT 'Category: completeness, accuracy, consistency, timeliness, uniqueness, validity.',
    check_expression    STRING      COMMENT 'SQL expression or description of the check logic.',
    expected_value      STRING      COMMENT 'Expected value or threshold for the check.',
    actual_value        STRING      COMMENT 'Actual observed value.',
    passed              BOOLEAN     COMMENT 'Whether the check passed (true) or failed (false).',
    severity            STRING      COMMENT 'Impact level if check fails: critical (blocks pipeline), warning (logged but continues), info (informational).',
    details             STRING      COMMENT 'Additional context or diagnostic information.',
    reporting_period    STRING      COMMENT 'Solvency II reporting period.',
    executed_by         STRING      COMMENT 'User who ran the check.',
    executed_at         TIMESTAMP   COMMENT 'UTC timestamp of check execution.'
) USING DELTA
COMMENT 'Data quality check results for every pipeline step. Supports DQ monitoring dashboards and regulatory evidence that QRT data meets quality standards. Append-only.'
TBLPROPERTIES (
    'delta.appendOnly' = 'true',
    'quality' = 'audit',
    'pipeline' = 'solvency-ii-qrt-pnc'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

schemas = [row.databaseName for row in spark.sql("SHOW SCHEMAS").collect()]
expected = ["qrt_demo_bronze", "qrt_demo_silver", "qrt_demo_gold", "qrt_demo_audit"]
for s in expected:
    status = "OK" if s in schemas else "MISSING"
    print(f"  Schema {s}: {status}")

# Check lineage table exists
lineage_count = spark.table(f"{catalog}.qrt_demo_audit.pipeline_lineage").count()
dq_count = spark.table(f"{catalog}.qrt_demo_audit.data_quality_log").count()
print(f"  pipeline_lineage rows: {lineage_count}")
print(f"  data_quality_log rows: {dq_count}")
print(f"\nSetup complete for catalog: {catalog}")
