# Databricks notebook source
# MAGIC %md
# MAGIC # Solvency II QRT Demo — Cost Control
# MAGIC
# MAGIC **Stop or restart** the always-on resources to save money when the demo is not in use.
# MAGIC
# MAGIC | Resource | Always-on? | Cost when idle | This notebook |
# MAGIC |----------|-----------|----------------|---------------|
# MAGIC | Databricks App | Yes — runs 24/7 | ~$0.07/hr (app compute) | Stop / Start |
# MAGIC | SQL Warehouse | Auto-stops after idle timeout | $0 when stopped | Stop / Start |
# MAGIC | Pipeline Job | On-demand only | $0 | No action needed |
# MAGIC | Tables & Schema | Storage only | Negligible | No action needed |
# MAGIC
# MAGIC On **Databricks Free** ($20/day), the app alone uses ~$1.70/day. Stopping it when not
# MAGIC demoing saves most of your budget for actual compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("app_name", "solvency2-qrt", "App Name")
dbutils.widgets.dropdown("action", "status", ["status", "stop", "start"], "Action")

app_name = dbutils.widgets.get("app_name")
action = dbutils.widgets.get("action")

print(f"App:    {app_name}")
print(f"Action: {action}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def get_app_status():
    """Get current app status."""
    try:
        app = w.apps.get(name=app_name)
        status = app.status.state if app.status else "UNKNOWN"
        url = app.url or "n/a"
        return status, url
    except Exception as e:
        return f"ERROR: {e}", "n/a"

def get_warehouses():
    """Get all SQL warehouses with their status."""
    try:
        whs = list(w.warehouses.list())
        return [(wh.id, wh.name, wh.state.value if wh.state else "UNKNOWN") for wh in whs]
    except Exception as e:
        print(f"⚠ Could not list warehouses: {e}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show current status

# COMMAND ----------

print("=" * 60)
print("CURRENT STATUS")
print("=" * 60)

# App
app_status, app_url = get_app_status()
emoji = "🟢" if "RUNNING" in str(app_status) else "🔴" if "STOPPED" in str(app_status) else "🟡"
print(f"\n{emoji} App '{app_name}': {app_status}")
print(f"  URL: {app_url}")

# Warehouses
print(f"\nSQL Warehouses:")
for wh_id, wh_name, wh_state in get_warehouses():
    wh_emoji = "🟢" if wh_state == "RUNNING" else "🔴" if wh_state == "STOPPED" else "🟡"
    print(f"  {wh_emoji} {wh_name} ({wh_id}): {wh_state}")

if action == "status":
    print("\nSet action to 'stop' or 'start' to change state.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute action

# COMMAND ----------

if action == "stop":
    print("=" * 60)
    print("STOPPING RESOURCES")
    print("=" * 60)

    # Stop app
    print(f"\nStopping app '{app_name}' ...")
    try:
        w.apps.stop(name=app_name)
        print("✓ App stop initiated. It may take a minute to fully stop.")
    except Exception as e:
        print(f"⚠ Could not stop app: {e}")

    # Stop all running warehouses
    print("\nStopping SQL warehouses ...")
    for wh_id, wh_name, wh_state in get_warehouses():
        if wh_state == "RUNNING":
            try:
                w.warehouses.stop(wh_id)
                print(f"  ✓ Stopped warehouse '{wh_name}'")
            except Exception as e:
                print(f"  ⚠ Could not stop '{wh_name}': {e}")
        else:
            print(f"  - '{wh_name}' already {wh_state}")

    print("\n✓ All resources stopped. $0 compute cost while idle.")

elif action == "start":
    print("=" * 60)
    print("STARTING RESOURCES")
    print("=" * 60)

    # Start app
    print(f"\nStarting app '{app_name}' ...")
    try:
        deploy = w.apps.deploy(app_name, source_code_path=None)
        print("✓ App start/redeploy initiated.")
        status, url = get_app_status()
        print(f"  Status: {status}")
        print(f"  URL: {url}")
    except Exception as e:
        # Fallback: try start directly
        try:
            # Some SDK versions use different methods
            import subprocess
            result = subprocess.run(
                ["databricks", "apps", "start", app_name],
                capture_output=True, text=True, timeout=60
            )
            if result.returncode == 0:
                print("✓ App start initiated via CLI.")
            else:
                print(f"⚠ Could not start app: {result.stderr}")
        except Exception as e2:
            print(f"⚠ Could not start app: {e}")
            print("  Start it manually from Compute > Apps in the workspace UI.")

    # Note: don't auto-start warehouses — they start on-demand when queries hit them
    print("\nSQL warehouses will auto-start when the app runs its first query.")
    print("No need to pre-start them.")

    print("\n✓ Resources starting. App will be ready in ~1-2 minutes.")

else:
    print("Action is 'status' — no changes made.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final status check

# COMMAND ----------

import time

if action in ("stop", "start"):
    print("Waiting 15s before checking status ...")
    time.sleep(15)

print("=" * 60)
print("FINAL STATUS")
print("=" * 60)

app_status, app_url = get_app_status()
emoji = "🟢" if "RUNNING" in str(app_status) else "🔴" if "STOPPED" in str(app_status) else "🟡"
print(f"\n{emoji} App '{app_name}': {app_status}")

for wh_id, wh_name, wh_state in get_warehouses():
    wh_emoji = "🟢" if wh_state == "RUNNING" else "🔴" if wh_state == "STOPPED" else "🟡"
    print(f"  {wh_emoji} {wh_name} ({wh_id}): {wh_state}")

if action == "stop":
    print("\n💰 Tip: Run this notebook with action='start' when you need the demo again.")
elif action == "start":
    print(f"\n🚀 App should be live at: {app_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consumption Tracker
# MAGIC
# MAGIC Query `system.billing.usage` to show how much this demo has consumed.
# MAGIC Works on any workspace with system tables enabled (including Databricks Free).

# COMMAND ----------

from datetime import datetime, timedelta

lookback_days = 7
start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

print(f"Consumption for the last {lookback_days} days (since {start_date})")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily spend by SKU

# COMMAND ----------

try:
    df_daily = spark.sql(f"""
        SELECT
            usage_date,
            sku_name,
            ROUND(SUM(usage_quantity), 4) AS dbus,
            ROUND(SUM(usage_quantity * list_prices.pricing.default), 2) AS est_cost_usd
        FROM system.billing.usage
        LEFT JOIN system.billing.list_prices
            ON usage.sku_name = list_prices.sku_name
            AND usage.usage_date BETWEEN list_prices.price_start_time AND COALESCE(list_prices.price_end_time, '2099-12-31')
        WHERE usage_date >= '{start_date}'
        GROUP BY usage_date, usage.sku_name
        ORDER BY usage_date DESC, est_cost_usd DESC
    """)
    display(df_daily)
except Exception as e:
    # Fallback without list_prices join (not all workspaces have it)
    print(f"⚠ Could not join list_prices ({e}). Showing DBUs only.")
    df_daily = spark.sql(f"""
        SELECT
            usage_date,
            sku_name,
            ROUND(SUM(usage_quantity), 4) AS dbus
        FROM system.billing.usage
        WHERE usage_date >= '{start_date}'
        GROUP BY usage_date, sku_name
        ORDER BY usage_date DESC, dbus DESC
    """)
    display(df_daily)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total spend summary

# COMMAND ----------

try:
    df_total = spark.sql(f"""
        SELECT
            sku_name,
            ROUND(SUM(usage_quantity), 4) AS total_dbus,
            ROUND(SUM(usage_quantity * list_prices.pricing.default), 2) AS est_cost_usd
        FROM system.billing.usage
        LEFT JOIN system.billing.list_prices
            ON usage.sku_name = list_prices.sku_name
            AND usage.usage_date BETWEEN list_prices.price_start_time AND COALESCE(list_prices.price_end_time, '2099-12-31')
        WHERE usage_date >= '{start_date}'
        GROUP BY usage.sku_name
        ORDER BY est_cost_usd DESC
    """)
    display(df_total)

    # Grand total
    row = spark.sql(f"""
        SELECT
            ROUND(SUM(usage_quantity), 4) AS total_dbus,
            ROUND(SUM(usage_quantity * list_prices.pricing.default), 2) AS est_cost_usd
        FROM system.billing.usage
        LEFT JOIN system.billing.list_prices
            ON usage.sku_name = list_prices.sku_name
            AND usage.usage_date BETWEEN list_prices.price_start_time AND COALESCE(list_prices.price_end_time, '2099-12-31')
        WHERE usage_date >= '{start_date}'
    """).first()
    print(f"\n{'=' * 50}")
    print(f"TOTAL ({lookback_days} days):  {row.total_dbus} DBUs  ≈  ${row.est_cost_usd} USD")
    print(f"{'=' * 50}")
except Exception as e:
    print(f"⚠ Could not join list_prices ({e}). Showing DBUs only.")
    df_total = spark.sql(f"""
        SELECT
            sku_name,
            ROUND(SUM(usage_quantity), 4) AS total_dbus
        FROM system.billing.usage
        WHERE usage_date >= '{start_date}'
        GROUP BY sku_name
        ORDER BY total_dbus DESC
    """)
    display(df_total)
    row = spark.sql(f"""
        SELECT ROUND(SUM(usage_quantity), 4) AS total_dbus
        FROM system.billing.usage
        WHERE usage_date >= '{start_date}'
    """).first()
    print(f"\nTOTAL ({lookback_days} days):  {row.total_dbus} DBUs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apps-specific consumption

# COMMAND ----------

try:
    df_apps = spark.sql(f"""
        SELECT
            usage_date,
            sku_name,
            ROUND(SUM(usage_quantity), 4) AS dbus,
            ROUND(SUM(usage_quantity * list_prices.pricing.default), 2) AS est_cost_usd
        FROM system.billing.usage
        LEFT JOIN system.billing.list_prices
            ON usage.sku_name = list_prices.sku_name
            AND usage.usage_date BETWEEN list_prices.price_start_time AND COALESCE(list_prices.price_end_time, '2099-12-31')
        WHERE usage_date >= '{start_date}'
          AND LOWER(sku_name) LIKE '%app%'
        GROUP BY usage_date, usage.sku_name
        ORDER BY usage_date DESC
    """)
    if df_apps.count() > 0:
        display(df_apps)
        app_row = spark.sql(f"""
            SELECT ROUND(SUM(usage_quantity * list_prices.pricing.default), 2) AS app_cost
            FROM system.billing.usage
            LEFT JOIN system.billing.list_prices
                ON usage.sku_name = list_prices.sku_name
                AND usage.usage_date BETWEEN list_prices.price_start_time AND COALESCE(list_prices.price_end_time, '2099-12-31')
            WHERE usage_date >= '{start_date}'
              AND LOWER(sku_name) LIKE '%app%'
        """).first()
        print(f"Apps cost ({lookback_days} days): ${app_row.app_cost} USD")
    else:
        print("No Apps-specific billing entries found (may not be broken out separately).")
except Exception as e:
    print(f"⚠ Could not query apps consumption: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Budget gauge (Databricks Free: $20/day)

# COMMAND ----------

try:
    today_str = datetime.now().strftime("%Y-%m-%d")
    today_row = spark.sql(f"""
        SELECT ROUND(SUM(usage_quantity * list_prices.pricing.default), 2) AS today_cost
        FROM system.billing.usage
        LEFT JOIN system.billing.list_prices
            ON usage.sku_name = list_prices.sku_name
            AND usage.usage_date BETWEEN list_prices.price_start_time AND COALESCE(list_prices.price_end_time, '2099-12-31')
        WHERE usage_date = '{today_str}'
    """).first()
    today_cost = today_row.today_cost or 0.0
    daily_budget = 20.0
    pct = (today_cost / daily_budget) * 100

    bar_len = 40
    filled = int(bar_len * min(pct, 100) / 100)
    bar = "█" * filled + "░" * (bar_len - filled)

    color = "🟢" if pct < 50 else "🟡" if pct < 80 else "🔴"

    print(f"Today's spend: ${today_cost:.2f} / ${daily_budget:.2f}")
    print(f"  {color} [{bar}] {pct:.1f}%")
    print(f"  Remaining: ${daily_budget - today_cost:.2f}")
except Exception as e:
    print(f"⚠ Could not calculate today's budget: {e}")
    print("  (list_prices table may not be available on this workspace)")
