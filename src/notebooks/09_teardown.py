# Databricks notebook source
# MAGIC %md
# MAGIC # Solvency II QRT Demo — Full Teardown
# MAGIC
# MAGIC **Removes everything** created by this demo, leaving no trace behind.
# MAGIC
# MAGIC What gets deleted:
# MAGIC - All tables in the demo schema (bronze, silver, gold, audit)
# MAGIC - The schema itself
# MAGIC - The Databricks App (`solvency2-qrt`)
# MAGIC - The DAB-deployed job (`solvency_ii_qrt_pnc_pipeline`)
# MAGIC - Workspace files under `/Workspace/Users/<you>/solvency2-qrt-app`
# MAGIC
# MAGIC **⚠️ This is irreversible. Run only when you want to completely remove the demo.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog")
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
# MAGIC ## 1. Drop schema (CASCADE drops all tables)

# COMMAND ----------

print(f"Dropping schema {catalog}.{schema} CASCADE ...")
try:
    spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE")
    print("✓ Schema dropped.")
except Exception as e:
    print(f"⚠ Could not drop schema: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delete the Databricks App

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
    print("✓ App stop initiated.")
else:
    print(f"⚠ App stop: {out}")

# COMMAND ----------

import time

# Wait briefly for stop to take effect
print("Waiting 10s for app to stop ...")
time.sleep(10)

print(f"Deleting app '{app_name}' ...")
rc, out = run_cli(["apps", "delete", app_name])
if rc == 0:
    print("✓ App deleted.")
else:
    # Try force via API if CLI doesn't support delete directly
    print(f"⚠ App delete via CLI: {out}")
    print("Attempting via REST API ...")
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        w.apps.delete(name=app_name)
        print("✓ App deleted via SDK.")
    except Exception as e:
        print(f"⚠ Could not delete app: {e}")
        print("  You may need to delete it manually from the workspace UI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delete the DAB-deployed job

# COMMAND ----------

job_name = "solvency_ii_qrt_pnc_pipeline"

print(f"Looking for job '{job_name}' ...")
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    jobs = w.jobs.list(name=job_name)
    deleted = 0
    for job in jobs:
        # Match both exact name and DAB-prefixed name
        if job_name in job.settings.name:
            print(f"  Deleting job {job.job_id}: {job.settings.name}")
            w.jobs.delete(job.job_id)
            deleted += 1
    if deleted:
        print(f"✓ Deleted {deleted} job(s).")
    else:
        print("  No matching jobs found (may already be cleaned up by `bundle destroy`).")
except Exception as e:
    print(f"⚠ Could not list/delete jobs: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delete workspace files

# COMMAND ----------

import os

try:
    w = WorkspaceClient()
    me = w.current_user.me()
    user_email = me.user_name or me.display_name
    workspace_path = f"/Workspace/Users/{user_email}/solvency2-qrt-app"

    print(f"Deleting workspace files at {workspace_path} ...")
    w.workspace.delete(workspace_path, recursive=True)
    print("✓ Workspace files deleted.")
except Exception as e:
    print(f"⚠ Could not delete workspace files: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Try DAB bundle destroy (best-effort)

# COMMAND ----------

print("Attempting `databricks bundle destroy` (best-effort) ...")
rc, out = run_cli(["bundle", "destroy", "--auto-approve"])
if rc == 0:
    print("✓ Bundle resources destroyed.")
else:
    print(f"⚠ Bundle destroy: {out}")
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
print(f"  Schema {catalog}.{schema}  — dropped")
print(f"  App {app_name}             — deleted")
print(f"  Pipeline job               — deleted")
print(f"  Workspace files            — deleted")
print()
print("If anything failed above, check the workspace UI for leftovers.")
