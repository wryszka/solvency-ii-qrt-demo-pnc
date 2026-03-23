#!/usr/bin/env bash
# ============================================================================
# Deploy Solvency II QRT Demo
#
# Uploads notebooks to a visible workspace folder and bootstraps archive data.
# Run this once after cloning the repo.
#
# Usage:
#   ./deploy_demo.sh                          # uses defaults
#   ./deploy_demo.sh --catalog my_catalog     # override catalog
#   ./deploy_demo.sh --profile STAGING        # use a different CLI profile
#
# Prerequisites:
#   - Databricks CLI v0.200+ authenticated
#   - Access to create catalogs/schemas or an existing catalog
# ============================================================================

set -euo pipefail

# Defaults
PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
CATALOG=""
SCHEMA="solvency2demo"
WORKSPACE_DIR=""
YEAR="2025"
ENTITY="Bricksurance SE"
WAREHOUSE_ID="c80acfa212bf1166"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --profile)  PROFILE="$2"; shift 2 ;;
        --catalog)  CATALOG="$2"; shift 2 ;;
        --schema)   SCHEMA="$2"; shift 2 ;;
        --folder)   WORKSPACE_DIR="$2"; shift 2 ;;
        --year)     YEAR="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Auto-detect catalog if not specified: use the first MANAGED_CATALOG
if [[ -z "$CATALOG" ]]; then
    CATALOG=$(databricks catalogs list --profile "$PROFILE" -o json 2>/dev/null \
        | python3 -c "import sys,json; cats=[c['name'] for c in json.load(sys.stdin) if c.get('catalog_type')=='MANAGED_CATALOG']; print(cats[0] if cats else 'main')" 2>/dev/null || echo "main")
    echo "Auto-detected catalog: $CATALOG"
fi

# Auto-detect workspace username for folder path
if [[ -z "$WORKSPACE_DIR" ]]; then
    USERNAME=$(databricks current-user me --profile "$PROFILE" -o json 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])" 2>/dev/null || echo "unknown")
    WORKSPACE_DIR="/Workspace/Users/${USERNAME}/Solvency II QRT Demo"
    echo "Workspace folder: $WORKSPACE_DIR"
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="${SCRIPT_DIR}/src"

echo ""
echo "============================================"
echo "  Solvency II QRT Demo — Deployment"
echo "============================================"
echo "  Profile:    $PROFILE"
echo "  Catalog:    $CATALOG"
echo "  Schema:     $SCHEMA"
echo "  Folder:     $WORKSPACE_DIR"
echo "  Year:       $YEAR"
echo "============================================"
echo ""

# Step 1: Create workspace folder structure
echo ">> Creating workspace folders..."
databricks workspace mkdirs "$WORKSPACE_DIR/00_Generate_Data" --profile "$PROFILE"
databricks workspace mkdirs "$WORKSPACE_DIR/01_QRT_S0602_Assets" --profile "$PROFILE"
databricks workspace mkdirs "$WORKSPACE_DIR/02_QRT_S0501_PnL" --profile "$PROFILE"
databricks workspace mkdirs "$WORKSPACE_DIR/03_QRT_S2501_SCR" --profile "$PROFILE"
databricks workspace mkdirs "$WORKSPACE_DIR/04_QRT_S2606_NL_Risk" --profile "$PROFILE"
databricks workspace mkdirs "$WORKSPACE_DIR/04_App" --profile "$PROFILE"
echo "   Done."

# Step 2: Upload notebooks
echo ">> Uploading notebooks..."

upload_notebook() {
    local src="$1"
    local dest="$2"
    databricks workspace import "$dest" \
        --file "$src" --format SOURCE --language PYTHON --overwrite --profile "$PROFILE" 2>/dev/null \
    && echo "   Uploaded: $dest" \
    || echo "   FAILED:   $dest"
}

# Data generation
upload_notebook "$SRC_DIR/00_Generate_Data/generate_data.py" \
    "$WORKSPACE_DIR/00_Generate_Data/generate_data"
upload_notebook "$SRC_DIR/00_Generate_Data/bootstrap_archive.py" \
    "$WORKSPACE_DIR/00_Generate_Data/bootstrap_archive"
upload_notebook "$SRC_DIR/00_Generate_Data/teardown.py" \
    "$WORKSPACE_DIR/00_Generate_Data/teardown"
upload_notebook "$SRC_DIR/00_Generate_Data/full_teardown.py" \
    "$WORKSPACE_DIR/00_Generate_Data/full_teardown"

# QRT notebooks
for dir in 01_QRT_S0602_Assets 02_QRT_S0501_PnL 03_QRT_S2501_SCR 04_QRT_S2606_NL_Risk; do
    if [[ -d "$SRC_DIR/$dir" ]]; then
        for f in "$SRC_DIR/$dir"/*.py; do
            [[ -f "$f" ]] || continue
            name=$(basename "$f" .py)
            upload_notebook "$f" "$WORKSPACE_DIR/$dir/$name"
        done
    fi
done

echo ""

# Step 3: Bootstrap archive data (Q1-Q3)
echo ">> Bootstrapping archive data (Q1-Q3 $YEAR)..."
echo "   This will generate synthetic data for 3 quarters."
echo "   Running bootstrap_archive notebook..."

RUN_OUTPUT=$(databricks jobs submit \
    --json "{
        \"run_name\": \"QRT Demo Bootstrap\",
        \"tasks\": [{
            \"task_key\": \"bootstrap\",
            \"notebook_task\": {
                \"notebook_path\": \"$WORKSPACE_DIR/00_Generate_Data/bootstrap_archive\",
                \"base_parameters\": {
                    \"catalog_name\": \"$CATALOG\",
                    \"schema_name\": \"$SCHEMA\",
                    \"reporting_year\": \"$YEAR\",
                    \"entity_name\": \"$ENTITY\"
                }
            },
            \"environment_key\": \"default\"
        }],
        \"environments\": [{
            \"environment_key\": \"default\",
            \"spec\": {
                \"client\": \"1\",
                \"dependencies\": [\"numpy\"]
            }
        }]
    }" \
    --profile "$PROFILE" 2>&1)

# Try to extract run_id
RUN_ID=$(echo "$RUN_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])" 2>/dev/null || echo "")

if [[ -n "$RUN_ID" ]]; then
    echo "   Bootstrap job submitted: run_id=$RUN_ID"
    echo "   Waiting for completion..."
    databricks jobs get-run "$RUN_ID" --profile "$PROFILE" --wait 2>/dev/null || true

    STATE=$(databricks jobs get-run "$RUN_ID" --profile "$PROFILE" -o json 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['state']['result_state'])" 2>/dev/null || echo "UNKNOWN")

    if [[ "$STATE" == "SUCCESS" ]]; then
        echo "   Bootstrap complete!"
    else
        echo "   Bootstrap finished with state: $STATE"
        echo "   Check the run in the Databricks UI for details."
    fi
else
    echo "   Could not submit bootstrap job. Output:"
    echo "   $RUN_OUTPUT"
    echo ""
    echo "   You can run it manually from the workspace:"
    echo "   Open: $WORKSPACE_DIR/00_Generate_Data/bootstrap_archive"
    echo "   Set parameters: catalog_name=$CATALOG, schema_name=$SCHEMA, reporting_year=$YEAR"
fi

# Step 3b: Deploy DAB bundle (DLT pipelines + workflow jobs)
echo ">> Deploying DAB bundle (DLT pipelines + jobs)..."
rm -rf "${SCRIPT_DIR}/.databricks/bundle/dev/sync-snapshots" "${SCRIPT_DIR}/.databricks/bundle/dev/fileset-snapshots" 2>/dev/null
databricks bundle deploy --profile "$PROFILE" 2>&1 | while read -r line; do echo "   $line"; done

# Step 3c: Register Standard Formula model
echo ">> Registering Standard Formula model..."
MODEL_OUTPUT=$(databricks jobs submit \
    --json "{
        \"run_name\": \"Register Standard Formula Model\",
        \"tasks\": [{
            \"task_key\": \"register_model\",
            \"notebook_task\": {
                \"notebook_path\": \"$WORKSPACE_DIR/03_QRT_S2501_SCR/register_standard_formula_model\",
                \"base_parameters\": {
                    \"catalog_name\": \"$CATALOG\",
                    \"schema_name\": \"$SCHEMA\"
                }
            },
            \"environment_key\": \"default\"
        }],
        \"environments\": [{
            \"environment_key\": \"default\",
            \"spec\": {
                \"client\": \"1\",
                \"dependencies\": [\"mlflow\", \"numpy\", \"pandas\"]
            }
        }]
    }" \
    --profile "$PROFILE" 2>&1)
MODEL_STATE=$(echo "$MODEL_OUTPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('state',{}).get('result_state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
echo "   Model registration: $MODEL_STATE"

# Step 3d: Run all QRT pipelines
echo ">> Running QRT pipelines..."
databricks jobs list --profile "$PROFILE" -o json 2>/dev/null | python3 -c "
import sys, json, subprocess
jobs = json.load(sys.stdin)
for j in (jobs.get('jobs', jobs) if isinstance(jobs, dict) else jobs):
    name = j.get('settings',{}).get('name','')
    if 'QRT S.' in name:
        jid = str(j['job_id'])
        subprocess.run(['databricks','jobs','run-now',jid,'--no-wait','--profile','$PROFILE'], capture_output=True)
        print(f'   Triggered: {name}')
" 2>/dev/null
echo "   Pipelines triggered (running in background)."

# Step 3e: Add table and column descriptions
echo ">> Adding table descriptions..."
if [[ -f "${SCRIPT_DIR}/scripts/add_descriptions.py" ]]; then
    python3 "${SCRIPT_DIR}/scripts/add_descriptions.py" 2>&1 | grep -E "^(Adding|Done)" | while read -r line; do echo "   $line"; done
fi

# Step 4: Create Lakeview dashboard and Genie space
echo ">> Creating Lakeview dashboard and Genie space..."

if [[ -f "${SCRIPT_DIR}/scripts/create_dashboard.py" ]]; then
    python3 "${SCRIPT_DIR}/scripts/create_dashboard.py" 2>&1 | while read -r line; do
        echo "   $line"
    done
else
    echo "   scripts/create_dashboard.py not found — skipping dashboard creation."
    echo "   Run it manually: python3 scripts/create_dashboard.py"
fi

# Create Genie space with tables (tables must be sorted by identifier)
echo "   Creating Genie space..."
GENIE_PAYLOAD=$(python3 -c "
import json
tables = sorted([
    'assets', 'premiums', 'claims', 'expenses', 'risk_factors',
    'own_funds', 'balance_sheet', 'scr_results', 'counterparties',
    'reinsurance', 'assets_enriched',
    's0602_list_of_assets', 's0602_summary',
    'premiums_by_lob', 'claims_by_lob', 'expenses_by_lob',
    's0501_premiums_claims_expenses', 's0501_summary',
    's2501_scr_breakdown', 's2501_summary',
    's2606_nl_uw_risk', 's2606_summary',
    'cat_risk_by_lob', 'premium_reserve_risk',
    'igloo_results', 'igloo_run_log',
    'claims_triangles', 'volume_measures',
])
print(json.dumps({
    'title': 'Solvency II QRT Assistant',
    'description': 'Ask questions about Bricksurance SE Solvency II data: assets, premiums, claims, SCR, solvency ratio, own funds.',
    'warehouse_id': '$WAREHOUSE_ID',
    'parent_path': '/Workspace/Users/$USERNAME',
    'serialized_space': json.dumps({
        'version': 2,
        'data_sources': {
            'tables': [{'identifier': f'$CATALOG.$SCHEMA.{t}'} for t in tables]
        }
    })
}))
")

echo "$GENIE_PAYLOAD" > /tmp/genie_deploy_payload.json
GENIE_OUTPUT=$(databricks api post /api/2.0/genie/spaces --profile "$PROFILE" --json @/tmp/genie_deploy_payload.json 2>&1)
rm -f /tmp/genie_deploy_payload.json
GENIE_ID=$(echo "$GENIE_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('space_id',''))" 2>/dev/null || echo "")
if [[ -n "$GENIE_ID" ]]; then
    GENIE_TABLES=$(echo "$GENIE_OUTPUT" | python3 -c "import sys,json; ss=json.loads(json.load(sys.stdin).get('serialized_space','{}')); print(len(ss.get('data_sources',{}).get('tables',[])))" 2>/dev/null || echo "0")
    echo "   Genie space created: $GENIE_ID ($GENIE_TABLES tables)"
else
    echo "   Genie space creation failed: $GENIE_OUTPUT"
fi

# Step 5: Deploy the Databricks App
echo ">> Deploying Databricks App..."
APP_NAME="solvency2-qrt"
APP_WS_PATH="/Workspace/Users/${USERNAME}/solvency-ii-qrt-demo/04_App"

# Create the app (ignore error if it already exists)
databricks apps create "$APP_NAME" --profile "$PROFILE" 2>/dev/null || true

# Upload app source files (skip .venv and node_modules)
echo "   Uploading app files..."
databricks workspace mkdirs "$APP_WS_PATH/frontend/dist/assets" --profile "$PROFILE"
databricks workspace mkdirs "$APP_WS_PATH/server/routes" --profile "$PROFILE"

for f in app.py app.yaml requirements.txt; do
    [[ -f "$SRC_DIR/app/$f" ]] && databricks workspace import "$APP_WS_PATH/$f" \
        --file "$SRC_DIR/app/$f" --format AUTO --overwrite --profile "$PROFILE" 2>/dev/null
done
for f in server/__init__.py server/config.py server/sql.py server/routes/__init__.py server/routes/reports.py server/routes/approvals.py; do
    [[ -f "$SRC_DIR/app/$f" ]] && databricks workspace import "$APP_WS_PATH/$f" \
        --file "$SRC_DIR/app/$f" --format AUTO --overwrite --profile "$PROFILE" 2>/dev/null
done
for f in "$SRC_DIR/app/frontend/dist/"*; do
    [[ -f "$f" ]] && databricks workspace import "$APP_WS_PATH/frontend/dist/$(basename "$f")" \
        --file "$f" --format AUTO --overwrite --profile "$PROFILE" 2>/dev/null
done
for f in "$SRC_DIR/app/frontend/dist/assets/"*; do
    [[ -f "$f" ]] && databricks workspace import "$APP_WS_PATH/frontend/dist/assets/$(basename "$f")" \
        --file "$f" --format AUTO --overwrite --profile "$PROFILE" 2>/dev/null
done

echo "   Deploying app..."
databricks apps deploy "$APP_NAME" --source-code-path "$APP_WS_PATH" --profile "$PROFILE" 2>&1 \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'   App status: {d.get(\"status\",{}).get(\"state\",\"unknown\")}')" 2>/dev/null \
    || echo "   App deploy may need manual start."

APP_URL=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null || echo "")

# Grant the app's service principal access to data and warehouse
APP_SP=$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id',''))" 2>/dev/null || echo "")

if [[ -n "$APP_SP" ]]; then
    echo "   Granting permissions to app service principal $APP_SP..."

    # Schema permissions
    databricks api post /api/2.0/sql/statements --json "{
        \"warehouse_id\": \"$WAREHOUSE_ID\",
        \"statement\": \"GRANT USE CATALOG ON CATALOG $CATALOG TO \\\`$APP_SP\\\`\",
        \"wait_timeout\": \"30s\"
    }" --profile "$PROFILE" 2>/dev/null

    databricks api post /api/2.0/sql/statements --json "{
        \"warehouse_id\": \"$WAREHOUSE_ID\",
        \"statement\": \"GRANT ALL PRIVILEGES ON SCHEMA $CATALOG.$SCHEMA TO \\\`$APP_SP\\\`\",
        \"wait_timeout\": \"30s\"
    }" --profile "$PROFILE" 2>/dev/null

    # Warehouse access
    databricks api patch "/api/2.0/permissions/sql/warehouses/$WAREHOUSE_ID" --profile "$PROFILE" --json "{
        \"access_control_list\": [{
            \"service_principal_name\": \"$APP_SP\",
            \"permission_level\": \"CAN_USE\"
        }]
    }" 2>/dev/null

    echo "   Permissions granted."
fi

echo ""
echo "============================================"
echo "  Deployment complete!"
echo "============================================"
echo ""
echo "  Notebooks are at:"
echo "    $WORKSPACE_DIR"
echo ""
echo "  Data is in:"
echo "    $CATALOG.$SCHEMA"
echo ""
echo "  To generate Q4 data for the live demo:"
echo "    Open: $WORKSPACE_DIR/00_Generate_Data/generate_data"
echo "    Set reporting_period=2025-Q4"
echo ""
if [[ -n "$APP_URL" ]]; then
echo "  App URL:"
echo "    $APP_URL"
echo ""
fi
echo "  To tear down everything:"
echo "    Open: $WORKSPACE_DIR/00_Generate_Data/teardown"
echo "    Or run: databricks workspace delete \"$WORKSPACE_DIR\" --recursive --profile $PROFILE"
echo "============================================"
