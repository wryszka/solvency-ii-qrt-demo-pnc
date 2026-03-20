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

# QRT notebooks (will be created in later steps)
for dir in 01_QRT_S0602_Assets 02_QRT_S0501_PnL 03_QRT_S2501_SCR; do
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
    databricks jobs get-run "$RUN_ID" --profile "$PROFILE" --wait 2>/dev/null

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

# Create Genie space
echo "   Creating Genie space..."
GENIE_OUTPUT=$(databricks api post /api/2.0/genie/spaces --profile "$PROFILE" --json "{
    \"title\": \"Solvency II QRT Assistant\",
    \"description\": \"Ask questions about Bricksurance SE Solvency II QRT data.\",
    \"warehouse_id\": \"$WAREHOUSE_ID\",
    \"table_identifiers\": [
        \"$CATALOG.$SCHEMA.assets_enriched\",
        \"$CATALOG.$SCHEMA.s0602_list_of_assets\",
        \"$CATALOG.$SCHEMA.s0602_summary\",
        \"$CATALOG.$SCHEMA.premiums_by_lob\",
        \"$CATALOG.$SCHEMA.claims_by_lob\",
        \"$CATALOG.$SCHEMA.expenses_by_lob\",
        \"$CATALOG.$SCHEMA.s0501_premiums_claims_expenses\",
        \"$CATALOG.$SCHEMA.s0501_summary\",
        \"$CATALOG.$SCHEMA.scr_results\",
        \"$CATALOG.$SCHEMA.s2501_scr_breakdown\",
        \"$CATALOG.$SCHEMA.s2501_summary\",
        \"$CATALOG.$SCHEMA.own_funds\",
        \"$CATALOG.$SCHEMA.balance_sheet\",
        \"$CATALOG.$SCHEMA.risk_factors\",
        \"$CATALOG.$SCHEMA.counterparties\",
        \"$CATALOG.$SCHEMA.reinsurance\",
        \"$CATALOG.$SCHEMA.volume_measures\"
    ],
    \"serialized_space\": \"{\\\"version\\\": 2, \\\"data_sources\\\": {}}\"
}" 2>&1)

GENIE_ID=$(echo "$GENIE_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('space_id',''))" 2>/dev/null || echo "")
if [[ -n "$GENIE_ID" ]]; then
    echo "   Genie space created: $GENIE_ID"
else
    echo "   Genie space creation returned: $GENIE_OUTPUT"
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
echo "  To tear down everything:"
echo "    Open: $WORKSPACE_DIR/00_Generate_Data/teardown"
echo "    Or run: databricks workspace delete \"$WORKSPACE_DIR\" --recursive --profile $PROFILE"
echo "============================================"
