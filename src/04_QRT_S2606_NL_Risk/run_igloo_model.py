# Databricks notebook source
# MAGIC %md
# MAGIC # Igloo Stochastic Model — Export, Run, Import
# MAGIC
# MAGIC Simulates the integration with an external catastrophe modelling engine (Igloo / RAFM / ReMetrica).
# MAGIC
# MAGIC **Workflow:**
# MAGIC 1. Export exposure data to a UC Volume (CSV file sent to Igloo)
# MAGIC 2. Run stochastic simulation (10,000 scenarios, 6 return periods)
# MAGIC 3. Import results from Volume (VaR/TVaR by peril, LoB, return period)
# MAGIC 4. Write to `igloo_run_results` table for downstream DLT consumption
# MAGIC 5. Log the run to `igloo_run_log` for audit trail
# MAGIC
# MAGIC In production, steps 2-3 would be an API call to the Igloo server or a file watch on SFTP.
# MAGIC Here we mock it using pre-generated stochastic output.

# COMMAND ----------

import uuid
import time
from datetime import datetime, timezone

# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog_name")
except Exception:
    catalog = "lr_serverless_aws_us_catalog"
try:
    schema = dbutils.widgets.get("schema_name")
except Exception:
    schema = "solvency2demo"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Get the latest reporting period from exposures
rp = spark.sql("SELECT MAX(reporting_period) FROM exposures").first()[0]

run_id = str(uuid.uuid4())[:8]
print(f"Catalog:           {catalog}")
print(f"Schema:            {schema}")
print(f"Reporting period:  {rp}")
print(f"Igloo run ID:      {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Export Exposures to Volume (Igloo Input)

# COMMAND ----------

# Ensure the exchange volume exists
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.igloo_exchange")

volume_base = f"/Volumes/{catalog}/{schema}/igloo_exchange"
input_path = f"{volume_base}/input_exposures_{rp.replace('-', '')}.csv"

# Read exposures for this period
exposures_df = spark.sql(f"""
    SELECT exposure_id, lob_code, lob_name, peril,
           number_of_risks, total_sum_insured_eur,
           aggregate_deductible_eur, aggregate_limit_eur,
           currency, reporting_period
    FROM exposures
    WHERE reporting_period = '{rp}'
""")

exposure_count = exposures_df.count()

# Write to Volume as CSV
exposures_df.toPandas().to_csv(input_path, index=False)

print(f"Exported {exposure_count} exposure sets to:")
print(f"  {input_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Run Igloo Stochastic Simulation (Mock)
# MAGIC
# MAGIC In production this would be:
# MAGIC ```
# MAGIC POST https://igloo-server.internal/api/v1/runs
# MAGIC {
# MAGIC   "input_file": "input_exposures_2025Q3.csv",
# MAGIC   "simulations": 10000,
# MAGIC   "return_periods": [10, 25, 50, 100, 200, 500],
# MAGIC   "model_version": "5.2.1"
# MAGIC }
# MAGIC ```

# COMMAND ----------

print("=" * 60)
print("  IGLOO STOCHASTIC ENGINE v5.2.1")
print("=" * 60)
print(f"  Input:        {exposure_count} exposure sets")
print(f"  Simulations:  10,000")
print(f"  Return periods: 10, 25, 50, 100, 200, 500")
print(f"  Perils:       windstorm, flood, earthquake, hail,")
print(f"                subsidence, freeze, wildfire")
print("=" * 60)
print()
print("  Running stochastic simulation...")

# Simulate processing time
time.sleep(5)

print("  Simulation complete.")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Import Results from Volume (Igloo Output)

# COMMAND ----------

# Read the pre-generated igloo_results (our mock output)
results_df = spark.sql(f"""
    SELECT lob_code, lob_name, peril, return_period,
           var_gross_eur, tvar_gross_eur,
           var_ceded_eur, tvar_ceded_eur,
           var_net_eur, tvar_net_eur,
           num_simulations, model_version,
           reporting_period
    FROM igloo_results
    WHERE reporting_period = '{rp}'
""")

result_count = results_df.count()
output_path = f"{volume_base}/output_results_{rp.replace('-', '')}.csv"

# Write to Volume (simulating Igloo writing its output)
results_df.toPandas().to_csv(output_path, index=False)

print(f"Igloo output received: {result_count} result rows")
print(f"  Output file: {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write Results to Delta Table

# COMMAND ----------

# Re-import from CSV to Delta (proving the round-trip through Volume)
reimported = spark.read.csv(output_path, header=True, inferSchema=True)
reimported = reimported.withColumn("igloo_run_id", spark.sql(f"SELECT '{run_id}'").first()[0])

# Write to igloo_run_results
reimported.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.igloo_run_results")
spark.sql(f"COMMENT ON TABLE {catalog}.{schema}.igloo_run_results IS 'Igloo stochastic output imported from Volume — VaR/TVaR by peril, LoB, return period'")

cnt = spark.table(f"{catalog}.{schema}.igloo_run_results").count()
print(f"Wrote {cnt} rows to igloo_run_results (run_id: {run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Log the Run

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

run_log = [{
    "run_id": run_id,
    "reporting_period": rp,
    "model_name": "Igloo",
    "model_version": "5.2.1",
    "num_simulations": 10000,
    "num_return_periods": 6,
    "exposure_count": exposure_count,
    "result_count": result_count,
    "input_path": input_path,
    "output_path": output_path,
    "status": "SUCCESS",
    "started_at": datetime.now(timezone.utc),
    "completed_at": datetime.now(timezone.utc),
}]

schema_def = StructType([
    StructField("run_id", StringType()),
    StructField("reporting_period", StringType()),
    StructField("model_name", StringType()),
    StructField("model_version", StringType()),
    StructField("num_simulations", IntegerType()),
    StructField("num_return_periods", IntegerType()),
    StructField("exposure_count", IntegerType()),
    StructField("result_count", IntegerType()),
    StructField("input_path", StringType()),
    StructField("output_path", StringType()),
    StructField("status", StringType()),
    StructField("started_at", TimestampType()),
    StructField("completed_at", TimestampType()),
])

log_df = spark.createDataFrame(run_log, schema=schema_def)
log_df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.igloo_run_log")
spark.sql(f"COMMENT ON TABLE {catalog}.{schema}.igloo_run_log IS 'Igloo stochastic run audit log — timestamps, simulation parameters, file paths'")

# COMMAND ----------

print("=" * 60)
print("  IGLOO RUN COMPLETE")
print("=" * 60)
print(f"  Run ID:       {run_id}")
print(f"  Period:       {rp}")
print(f"  Exposures:    {exposure_count} sets exported")
print(f"  Results:      {result_count} rows imported")
print(f"  Simulations:  10,000")
print(f"  Status:       SUCCESS")
print("=" * 60)
