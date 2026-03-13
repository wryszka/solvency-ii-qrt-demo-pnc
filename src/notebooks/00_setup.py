# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: Schema and Lineage Audit Infrastructure
# MAGIC Creates a single `solvency2demo` schema containing all demo tables (bronze/silver/gold/audit),
# MAGIC plus the pipeline lineage and data quality audit tables.
# MAGIC
# MAGIC All tables use name prefixes to distinguish layers:
# MAGIC - `bronze_*` — raw synthetic source data
# MAGIC - `silver_*` — cleansed and aggregated
# MAGIC - `gold_*` — EIOPA QRT-aligned output
# MAGIC - `audit_*` — pipeline lineage and data quality logs
# MAGIC
# MAGIC **Designed for minimal permissions** — works with any catalog the user has access to,
# MAGIC including `hive_metastore` on Databricks Free/Community Edition.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_classic_aws_us_catalog")  # <-- Change to your catalog name
dbutils.widgets.text("schema_name", "solvency2demo")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

# COMMAND ----------

# Try to create the catalog (works if user has CREATE CATALOG permission).
# On Free edition or shared metastores, fall back to the existing catalog.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    print(f"Catalog '{catalog}' created or already exists.")
except Exception as e:
    if "PERMISSION_DENIED" in str(e) or "does not exist" in str(e) or "not supported" in str(e).lower():
        print(f"Cannot create catalog '{catalog}' — assuming it already exists.")
    else:
        raise

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {schema}
COMMENT 'Solvency II QRT reporting demo (P&C). Contains synthetic insurer data (bronze_*), cleansed aggregations (silver_*), EIOPA-aligned QRT outputs (gold_*), and pipeline audit trail (audit_*). Single-schema design for easy deployment and minimal permissions.'
""")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Export Volume (Unity Catalog only)

# COMMAND ----------

try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.exports COMMENT 'CSV/XBRL export files for Tagetik and external reporting tool ingestion.'")
    print("Export volume created.")
except Exception as e:
    print(f"Volume creation skipped (not supported on this edition): {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Pipeline Lineage Audit Table
# MAGIC Records every pipeline execution step with full traceability:
# MAGIC - **What**: source tables, target table, transformation logic
# MAGIC - **Where**: notebook path, workspace, catalog/schema
# MAGIC - **When**: execution timestamp, reporting period
# MAGIC - **Who**: executed by (user), pipeline run ID

# COMMAND ----------

# Use DELTA if available, fall back to default format for Community Edition
try:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.audit_pipeline_lineage (
        lineage_id          STRING      COMMENT 'Unique lineage record ID (UUID). Primary key.',
        pipeline_run_id     STRING      COMMENT 'Pipeline execution run ID. Groups all steps from a single pipeline execution.',
        step_name           STRING      COMMENT 'Pipeline step identifier matching notebook name (e.g. 01_generate_bronze_data).',
        step_sequence       INT         COMMENT 'Execution order within the pipeline (0=setup, 1=bronze, 2-4=silver, 5-7=gold, 8=validation).',
        source_tables       STRING      COMMENT 'Comma-separated fully-qualified source table names consumed by this step.',
        target_table        STRING      COMMENT 'Fully-qualified target table name produced by this step.',
        transformation_type STRING      COMMENT 'Category: generation, cleansing, aggregation, enrichment, formatting, validation.',
        transformation_desc STRING      COMMENT 'Human-readable description of the transformation logic applied.',
        row_count_in        LONG        COMMENT 'Total input row count across all source tables. Null for generation steps.',
        row_count_out       LONG        COMMENT 'Output row count written to the target table.',
        columns_in          STRING      COMMENT 'JSON array of input column names consumed from source tables.',
        columns_out         STRING      COMMENT 'JSON array of output column names written to the target table.',
        parameters          STRING      COMMENT 'JSON object of pipeline parameters used (catalog, reporting_date, seed, scale_factor).',
        notebook_path       STRING      COMMENT 'Full workspace path of the notebook that executed this step.',
        workspace_url       STRING      COMMENT 'Databricks workspace URL where the step was executed.',
        catalog_name        STRING      COMMENT 'Unity Catalog name used for this execution.',
        schema_name         STRING      COMMENT 'Schema name for this execution.',
        reporting_period    STRING      COMMENT 'Solvency II reporting period in YYYY-QN format.',
        executed_by         STRING      COMMENT 'Username or service principal that executed this step.',
        executed_at         TIMESTAMP   COMMENT 'UTC timestamp when this step completed.',
        duration_seconds    DOUBLE      COMMENT 'Wall-clock execution duration in seconds for this step.',
        status              STRING      COMMENT 'Execution status: success, failed, or skipped.',
        error_message       STRING      COMMENT 'Error message if status is failed. Null on success.',
        data_quality_checks STRING      COMMENT 'JSON array of data quality check results.',
        checksum            STRING      COMMENT 'SHA-256 hash of the output table content for tamper detection.'
    ) USING DELTA
    COMMENT 'Complete pipeline lineage audit trail for Solvency II QRT reporting. Append-only — historical runs preserved.'
    TBLPROPERTIES (
        'delta.appendOnly' = 'true',
        'quality' = 'audit',
        'pipeline' = 'solvency-ii-qrt-pnc'
    )
    """)
except Exception:
    # Fallback for environments without Delta TBLPROPERTIES support
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.audit_pipeline_lineage (
        lineage_id STRING, pipeline_run_id STRING, step_name STRING, step_sequence INT,
        source_tables STRING, target_table STRING, transformation_type STRING,
        transformation_desc STRING, row_count_in LONG, row_count_out LONG,
        columns_in STRING, columns_out STRING, parameters STRING, notebook_path STRING,
        workspace_url STRING, catalog_name STRING, schema_name STRING,
        reporting_period STRING, executed_by STRING, executed_at TIMESTAMP,
        duration_seconds DOUBLE, status STRING, error_message STRING,
        data_quality_checks STRING, checksum STRING
    ) USING DELTA
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Data Quality Log Table

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.audit_data_quality_log (
        check_id            STRING      COMMENT 'Unique quality check ID (UUID). Primary key.',
        pipeline_run_id     STRING      COMMENT 'Pipeline run ID linking to audit_pipeline_lineage.pipeline_run_id.',
        step_name           STRING      COMMENT 'Pipeline step that performed the check.',
        table_name          STRING      COMMENT 'Fully-qualified table name being checked.',
        check_name          STRING      COMMENT 'Name of the quality check (e.g. row_count, null_check, referential_integrity).',
        check_category      STRING      COMMENT 'Category: completeness, accuracy, consistency, timeliness, uniqueness, validity.',
        check_expression    STRING      COMMENT 'SQL expression or description of the check logic.',
        expected_value      STRING      COMMENT 'Expected value or threshold for the check.',
        actual_value        STRING      COMMENT 'Actual observed value.',
        passed              BOOLEAN     COMMENT 'Whether the check passed (true) or failed (false).',
        severity            STRING      COMMENT 'Impact level: critical (blocks pipeline), warning (continues), info.',
        details             STRING      COMMENT 'Additional context or diagnostic information.',
        reporting_period    STRING      COMMENT 'Solvency II reporting period.',
        executed_by         STRING      COMMENT 'User who ran the check.',
        executed_at         TIMESTAMP   COMMENT 'UTC timestamp of check execution.'
    ) USING DELTA
    COMMENT 'Data quality check results for every pipeline step. Append-only.'
    TBLPROPERTIES (
        'delta.appendOnly' = 'true',
        'quality' = 'audit',
        'pipeline' = 'solvency-ii-qrt-pnc'
    )
    """)
except Exception:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.audit_data_quality_log (
        check_id STRING, pipeline_run_id STRING, step_name STRING, table_name STRING,
        check_name STRING, check_category STRING, check_expression STRING,
        expected_value STRING, actual_value STRING, passed BOOLEAN, severity STRING,
        details STRING, reporting_period STRING, executed_by STRING, executed_at TIMESTAMP
    ) USING DELTA
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# Check schema exists
tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()]
print(f"Schema: {catalog}.{schema}")
print(f"Tables found: {len(tables)}")
for t in sorted(tables):
    print(f"  {t}")

# Check audit tables
lineage_count = spark.table(f"{catalog}.{schema}.audit_pipeline_lineage").count()
dq_count = spark.table(f"{catalog}.{schema}.audit_data_quality_log").count()
print(f"\naudit_pipeline_lineage rows: {lineage_count}")
print(f"audit_data_quality_log rows: {dq_count}")
print(f"\nSetup complete: {catalog}.{schema}")
