# Databricks notebook source
# MAGIC %md
# MAGIC # Solvency II QRT Demo — Full Teardown
# MAGIC
# MAGIC **Removes everything** created by this demo, leaving no trace behind.
# MAGIC
# MAGIC What gets deleted:
# MAGIC - All tables in the demo schema (bronze, silver, gold, scr_results, own_funds, etc.)
# MAGIC - The schema itself
# MAGIC - The MLflow model (`standard_formula`) from Unity Catalog
# MAGIC - The Databricks App (`solvency2-qrt`)
# MAGIC - The three per-QRT workflow jobs (S.06.02, S.05.01, S.25.01)
# MAGIC - DLT pipelines
# MAGIC - Workspace files under `/Workspace/Users/<you>/solvency-ii-qrt-demo` and `/Workspace/Users/<you>/Solvency II QRT Demo`
# MAGIC
# MAGIC **This is irreversible. Run only when you want to completely remove the demo.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog", "Catalog")
dbutils.widgets.text("schema_name", "solvency2demo", "Schema")
dbutils.widgets.text("app_name", "solvency2-qrt", "App Name")
dbutils.widgets.dropdown("confirm", "no", ["no", "yes"], "Confirm teardown?")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
app_name = dbutils.widgets.get("app_name")
confirm = dbutils.widgets.get("confirm")

print(f"Catalog:  {catalog}")
print(f"Schema:   {schema}")
print(f"App:      {app_name}")
print(f"Confirm:  {confirm}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Safety gate

# COMMAND ----------

if confirm != "yes":
    dbutils.notebook.exit(
        "Teardown NOT executed. Set 'confirm' to 'yes' to proceed."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Delete the MLflow model from Unity Catalog
# MAGIC
# MAGIC Must happen **before** dropping the schema, otherwise the model becomes orphaned.

# COMMAND ----------

model_name = f"{catalog}.{schema}.standard_formula"

try:
    from mlflow import MlflowClient
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    client = MlflowClient()

    # Delete all versions first
    versions = client.search_model_versions(f"name='{model_name}'")
    for v in versions:
        print(f"  Deleting model version {v.version} ...")
        client.delete_model_version(model_name, v.version)

    # Delete the registered model
    client.delete_registered_model(model_name)
    print(f"Done — model '{model_name}' deleted.")
except Exception as e:
    print(f"Could not delete model: {e}")
    print("  (This is expected if the model was never registered.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drop schema (CASCADE drops all tables)

# COMMAND ----------

print(f"Dropping schema {catalog}.{schema} CASCADE ...")
try:
    spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE")
    print("Done — schema dropped.")
except Exception as e:
    print(f"Could not drop schema: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delete the Databricks App

# COMMAND ----------

import subprocess, json

def run_cli(args: list[str]) -> tuple[int, str]:
    """Run a databricks CLI command and return (returncode, output)."""
    result = subprocess.run(
        ["databricks"] + args,
        capture_output=True, text=True, timeout=60
    )
    return result.returncode, (result.stdout + result.stderr).strip()

# Stop the app first (must be stopped before delete)
print(f"Stopping app '{app_name}' ...")
rc, out = run_cli(["apps", "stop", app_name])
if rc == 0:
    print("App stop initiated.")
else:
    print(f"App stop: {out}")

# COMMAND ----------

import time

# Wait briefly for stop to take effect
print("Waiting 10s for app to stop ...")
time.sleep(10)

print(f"Deleting app '{app_name}' ...")
rc, out = run_cli(["apps", "delete", app_name])
if rc == 0:
    print("Done — app deleted.")
else:
    # Try force via API if CLI doesn't support delete directly
    print(f"App delete via CLI: {out}")
    print("Attempting via REST API ...")
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        w.apps.delete(name=app_name)
        print("Done — app deleted via SDK.")
    except Exception as e:
        print(f"Could not delete app: {e}")
        print("  You may need to delete it manually from the workspace UI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delete per-QRT workflow jobs and DLT pipelines

# COMMAND ----------

# Job names match the three per-QRT pipeline YMLs in resources/
job_names = [
    "QRT S.06.02",
    "QRT S.05.01",
    "QRT S.25.01",
]

pipeline_names = [
    "S.06.02 List of Assets",
    "S.05.01 Premiums, Claims & Expenses",
    "S.25.01 SCR Template",
]

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    # Delete jobs
    print("Looking for QRT workflow jobs ...")
    all_jobs = list(w.jobs.list())
    deleted_jobs = 0
    for job in all_jobs:
        name = job.settings.name if job.settings else ""
        if any(jn in name for jn in job_names):
            print(f"  Deleting job {job.job_id}: {name}")
            w.jobs.delete(job.job_id)
            deleted_jobs += 1
    if deleted_jobs:
        print(f"Done — deleted {deleted_jobs} job(s).")
    else:
        print("  No matching jobs found.")

    # Delete DLT pipelines
    print("Looking for DLT pipelines ...")
    all_pipelines = list(w.pipelines.list_pipelines())
    deleted_pipelines = 0
    for p in all_pipelines:
        if any(pn in (p.name or "") for pn in pipeline_names):
            print(f"  Deleting pipeline {p.pipeline_id}: {p.name}")
            w.pipelines.delete(p.pipeline_id)
            deleted_pipelines += 1
    if deleted_pipelines:
        print(f"Done — deleted {deleted_pipelines} pipeline(s).")
    else:
        print("  No matching pipelines found.")

except Exception as e:
    print(f"Could not list/delete jobs or pipelines: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delete workspace files

# COMMAND ----------

import os

# Check both possible workspace folder names (deploy_demo.sh vs DAB bundle)
workspace_folders = [
    "Solvency II QRT Demo",
    "solvency-ii-qrt-demo",
    "solvency-ii-qrt-demo-pnc",
]

try:
    w = WorkspaceClient()
    me = w.current_user.me()
    user_email = me.user_name or me.display_name

    for folder in workspace_folders:
        workspace_path = f"/Workspace/Users/{user_email}/{folder}"
        try:
            w.workspace.get_status(workspace_path)
            print(f"Deleting workspace files at {workspace_path} ...")
            w.workspace.delete(workspace_path, recursive=True)
            print(f"  Done — {folder} deleted.")
        except Exception:
            pass  # folder doesn't exist, skip

except Exception as e:
    print(f"Could not delete workspace files: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Try DAB bundle destroy (best-effort)

# COMMAND ----------

print("Attempting `databricks bundle destroy` (best-effort) ...")
rc, out = run_cli(["bundle", "destroy", "--auto-approve"])
if rc == 0:
    print("Done — bundle resources destroyed.")
else:
    print(f"Bundle destroy: {out}")
    print("  (This is expected if running from workspace — bundle destroy works best from local repo.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Everything has been removed. You can safely delete this repo from your workspace too.

# COMMAND ----------

print("=" * 60)
print("TEARDOWN COMPLETE")
print("=" * 60)
print(f"  Schema {catalog}.{schema}     — dropped")
print(f"  MLflow model                   — deleted")
print(f"  App {app_name}                 — deleted")
print(f"  QRT jobs & DLT pipelines       — deleted")
print(f"  Workspace files                — deleted")
print()
print("If anything failed above, check the workspace UI for leftovers.")
