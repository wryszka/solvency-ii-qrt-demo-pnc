# Databricks notebook source
# MAGIC %md
# MAGIC # Run Standard Formula Model
# MAGIC
# MAGIC Loads the **Champion** version of the Standard Formula model from Unity Catalog,
# MAGIC runs it against each quarter's risk factor charges, and writes the SCR breakdown
# MAGIC to `scr_results`.
# MAGIC
# MAGIC **Input:** `risk_factors` (per-quarter sub-module charges)
# MAGIC **Model:** `standard_formula` @ Champion alias
# MAGIC **Output:** `scr_results` (SCR breakdown per quarter)

# COMMAND ----------

import mlflow
import pandas as pd

# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog_name")
except Exception:
    catalog = "lr_serverless_aws_us_catalog"
try:
    schema = dbutils.widgets.get("schema_name")
except Exception:
    schema = "solvency2_qrt"

model_name = f"{catalog}.{schema}.standard_formula"
full_table = f"{catalog}.{schema}.scr_results"

print(f"Catalog:  {catalog}")
print(f"Schema:   {schema}")
print(f"Model:    {model_name} @ Champion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load the Champion Model

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
champion = mlflow.pyfunc.load_model(f"models:/{model_name}@Champion")

# Retrieve calibration info
import json
params_path = champion._model_impl.context.artifacts["parameters"]
with open(params_path, "r") as f:
    cal = json.load(f)

print(f"Model loaded: {cal['calibration_label']}")
print(f"  Calibration year:  {cal['calibration_year']}")
print(f"  Op risk factor:    {cal['op_risk_factor']*100:.1f}%")
print(f"  LAC_DT cap:        {cal['lac_dt_cap']*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run for Each Reporting Period

# COMMAND ----------

# Get all reporting periods
periods_df = spark.sql(f"""
    SELECT DISTINCT reporting_period
    FROM {catalog}.{schema}.risk_factors
    ORDER BY reporting_period
""").toPandas()

print(f"Reporting periods to process: {list(periods_df['reporting_period'])}")

# COMMAND ----------

all_results = []

for _, row in periods_df.iterrows():
    rp = row["reporting_period"]

    # Load risk factors for this quarter
    rf = spark.sql(f"""
        SELECT risk_module, risk_sub_module, charge_eur
        FROM {catalog}.{schema}.risk_factors
        WHERE reporting_period = '{rp}'
    """).toPandas()

    # Run the model
    scr_output = champion.predict(rf)

    # Tag with reporting period and model info
    scr_output["reporting_period"] = rp
    scr_output["model_version"] = f"Champion (v{cal['calibration_year']})"
    scr_output["calibration_year"] = cal["calibration_year"]

    all_results.append(scr_output)
    scr_row = scr_output[scr_output["component"] == "SCR"]
    scr_val = scr_row["amount_eur"].values[0] if len(scr_row) > 0 else 0
    print(f"  {rp}: SCR = EUR {scr_val:,.0f}")

results_df = pd.concat(all_results, ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Unity Catalog

# COMMAND ----------

spark_df = spark.createDataFrame(results_df)

# Overwrite the full table (idempotent — always recalculates from model)
spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table)

spark.sql(f"COMMENT ON TABLE {full_table} IS 'SCR results from Standard Formula model (Champion) — one row per component per quarter'")

cnt = spark.table(full_table).count()
print(f"Wrote {cnt} rows to {full_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quick Validation

# COMMAND ----------

display(spark.sql(f"""
    SELECT reporting_period, component, amount_eur
    FROM {full_table}
    WHERE component IN ('SCR_market', 'SCR_default', 'SCR_non_life', 'SCR_health', 'SCR_life',
                        'BSCR', 'Op_risk', 'LAC_DT', 'SCR')
    ORDER BY reporting_period,
             CASE component
                 WHEN 'SCR_market' THEN 1
                 WHEN 'SCR_default' THEN 2
                 WHEN 'SCR_non_life' THEN 3
                 WHEN 'SCR_health' THEN 4
                 WHEN 'SCR_life' THEN 5
                 WHEN 'BSCR' THEN 6
                 WHEN 'Op_risk' THEN 7
                 WHEN 'LAC_DT' THEN 8
                 WHEN 'SCR' THEN 9
             END
"""))
