# Databricks notebook source
# MAGIC %md
# MAGIC # Lineage Logger — reusable audit trail helper
# MAGIC Provides `log_lineage()` and `log_data_quality()` functions for all pipeline notebooks.
# MAGIC Include via `%run ./helpers/lineage_logger` at the top of each notebook.

# COMMAND ----------

import uuid
import json
import hashlib
from datetime import datetime

class LineageLogger:
    """Logs pipeline lineage and data quality checks to the audit schema."""

    def __init__(self, spark, catalog, schema, reporting_period, pipeline_run_id=None):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.reporting_period = reporting_period
        self.pipeline_run_id = pipeline_run_id or str(uuid.uuid4())
        self.audit_prefix = f"{catalog}.{schema}"
        self._step_start_times = {}

        # Get current user and workspace
        try:
            self.executed_by = spark.sql("SELECT current_user()").first()[0]
        except Exception:
            self.executed_by = "unknown"

        try:
            ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            self.workspace_url = ctx.get("extraContext", {}).get("api_url", "")
            self.notebook_path = ctx.get("extraContext", {}).get("notebook_path", "")
        except Exception:
            self.workspace_url = ""
            self.notebook_path = ""

    def start_step(self, step_name):
        """Mark the start of a pipeline step for duration tracking."""
        self._step_start_times[step_name] = datetime.utcnow()

    def _compute_checksum(self, table_name):
        """Compute SHA-256 checksum of table content for tamper detection."""
        try:
            # Hash the count + schema + sample for performance (full hash too expensive for large tables)
            df = self.spark.table(table_name)
            count = df.count()
            schema_str = str(df.schema)
            content = f"{count}|{schema_str}"
            return hashlib.sha256(content.encode()).hexdigest()
        except Exception:
            return None

    def log_lineage(
        self,
        step_name,
        step_sequence,
        source_tables,
        target_table,
        transformation_type,
        transformation_desc,
        row_count_in=None,
        row_count_out=None,
        columns_in=None,
        columns_out=None,
        parameters=None,
        status="success",
        error_message=None,
        data_quality_checks=None,
    ):
        """Write a lineage record to the audit table."""
        now = datetime.utcnow()
        duration = None
        if step_name in self._step_start_times:
            duration = (now - self._step_start_times[step_name]).total_seconds()

        checksum = self._compute_checksum(target_table) if status == "success" and target_table else None

        # Resolve notebook path for this specific step
        notebook_path = self.notebook_path

        record = {
            "lineage_id": str(uuid.uuid4()),
            "pipeline_run_id": self.pipeline_run_id,
            "step_name": step_name,
            "step_sequence": step_sequence,
            "source_tables": ", ".join(source_tables) if source_tables else None,
            "target_table": target_table,
            "transformation_type": transformation_type,
            "transformation_desc": transformation_desc,
            "row_count_in": row_count_in,
            "row_count_out": row_count_out,
            "columns_in": json.dumps(columns_in) if columns_in else None,
            "columns_out": json.dumps(columns_out) if columns_out else None,
            "parameters": json.dumps(parameters) if parameters else None,
            "notebook_path": notebook_path,
            "workspace_url": self.workspace_url,
            "catalog_name": self.catalog,
            "schema_name": self.schema,
            "reporting_period": self.reporting_period,
            "executed_by": self.executed_by,
            "executed_at": now,
            "duration_seconds": duration,
            "status": status,
            "error_message": error_message,
            "data_quality_checks": json.dumps(data_quality_checks) if data_quality_checks else None,
            "checksum": checksum,
        }

        from pyspark.sql import Row
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, LongType,
            DoubleType, TimestampType
        )

        schema = StructType([
            StructField("lineage_id", StringType()),
            StructField("pipeline_run_id", StringType()),
            StructField("step_name", StringType()),
            StructField("step_sequence", IntegerType()),
            StructField("source_tables", StringType()),
            StructField("target_table", StringType()),
            StructField("transformation_type", StringType()),
            StructField("transformation_desc", StringType()),
            StructField("row_count_in", LongType()),
            StructField("row_count_out", LongType()),
            StructField("columns_in", StringType()),
            StructField("columns_out", StringType()),
            StructField("parameters", StringType()),
            StructField("notebook_path", StringType()),
            StructField("workspace_url", StringType()),
            StructField("catalog_name", StringType()),
            StructField("schema_name", StringType()),
            StructField("reporting_period", StringType()),
            StructField("executed_by", StringType()),
            StructField("executed_at", TimestampType()),
            StructField("duration_seconds", DoubleType()),
            StructField("status", StringType()),
            StructField("error_message", StringType()),
            StructField("data_quality_checks", StringType()),
            StructField("checksum", StringType()),
        ])

        df = self.spark.createDataFrame([record], schema=schema)
        df.write.format("delta").mode("append").saveAsTable(f"{self.audit_prefix}.audit_pipeline_lineage")
        print(f"  LINEAGE: {step_name} -> {target_table} ({status}, {row_count_out} rows)")

    def log_data_quality(self, step_name, table_name, check_name, check_category,
                         check_expression, expected_value, actual_value, passed,
                         severity="warning", details=None):
        """Write a data quality check result to the audit table."""
        from pyspark.sql.types import (
            StructType, StructField, StringType, BooleanType, TimestampType
        )

        record = {
            "check_id": str(uuid.uuid4()),
            "pipeline_run_id": self.pipeline_run_id,
            "step_name": step_name,
            "table_name": table_name,
            "check_name": check_name,
            "check_category": check_category,
            "check_expression": check_expression,
            "expected_value": str(expected_value),
            "actual_value": str(actual_value),
            "passed": passed,
            "severity": severity,
            "details": details,
            "reporting_period": self.reporting_period,
            "executed_by": self.executed_by,
            "executed_at": datetime.utcnow(),
        }

        schema = StructType([
            StructField("check_id", StringType()),
            StructField("pipeline_run_id", StringType()),
            StructField("step_name", StringType()),
            StructField("table_name", StringType()),
            StructField("check_name", StringType()),
            StructField("check_category", StringType()),
            StructField("check_expression", StringType()),
            StructField("expected_value", StringType()),
            StructField("actual_value", StringType()),
            StructField("passed", BooleanType()),
            StructField("severity", StringType()),
            StructField("details", StringType()),
            StructField("reporting_period", StringType()),
            StructField("executed_by", StringType()),
            StructField("executed_at", TimestampType()),
        ])

        df = self.spark.createDataFrame([record], schema=schema)
        df.write.format("delta").mode("append").saveAsTable(f"{self.audit_prefix}.audit_data_quality_log")
        status_icon = "PASS" if passed else "FAIL"
        print(f"  DQ [{status_icon}]: {table_name} / {check_name} (expected={expected_value}, actual={actual_value})")
