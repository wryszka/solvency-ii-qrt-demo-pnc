#!/bin/bash
set -euo pipefail

# ==============================================================================
# Solvency II QRT Demo — Automated Deployment Script
# ==============================================================================

# --- Colors -------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

info()    { echo -e "${CYAN}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERR]${NC}  $*"; }
step()    { echo -e "\n${BOLD}==> $*${NC}"; }

# --- Script location ----------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Defaults -----------------------------------------------------------------
HOST=""
CATALOG=""
SCHEMA="solvency2demo"
PROFILE=""
APP_NAME="solvency2-qrt"

# --- Usage --------------------------------------------------------------------
usage() {
  cat <<USAGE
Usage: $(basename "$0") [OPTIONS]

Deploy the Solvency II QRT demo to a Databricks workspace.

Required:
  --host URL          Databricks workspace URL (e.g. https://xxx.cloud.databricks.com)

Optional:
  --catalog NAME      Unity Catalog name (default: auto-detect or "main")
  --schema NAME       Schema name (default: solvency2demo)
  --profile NAME      Databricks CLI profile (default: solvency2-demo)
  --help              Show this help message

Examples:
  $(basename "$0") --host https://my-workspace.cloud.databricks.com
  $(basename "$0") --host https://my-workspace.cloud.databricks.com --catalog my_catalog --schema myschema
USAGE
  exit 0
}

# --- Parse arguments ----------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)     HOST="$2"; shift 2 ;;
    --catalog)  CATALOG="$2"; shift 2 ;;
    --schema)   SCHEMA="$2"; shift 2 ;;
    --profile)  PROFILE="$2"; shift 2 ;;
    --help)     usage ;;
    *)          error "Unknown option: $1"; usage ;;
  esac
done

if [[ -z "$HOST" ]]; then
  error "--host is required."
  usage
fi

# Strip trailing slash from host
HOST="${HOST%/}"

# Default profile
if [[ -z "$PROFILE" ]]; then
  PROFILE="solvency2-demo"
fi

# ==============================================================================
# Pre-flight checks
# ==============================================================================
step "Pre-flight checks"

# --- databricks CLI -----------------------------------------------------------
if ! command -v databricks &>/dev/null; then
  error "databricks CLI is not installed."
  echo "  Install it: https://docs.databricks.com/dev-tools/cli/install.html"
  exit 1
fi

DB_VERSION=$(databricks --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "0.0.0")
REQUIRED_VERSION="0.229.0"

version_gte() {
  # Returns 0 (true) if $1 >= $2 using sort -V
  [[ "$(printf '%s\n%s' "$1" "$2" | sort -V | head -1)" == "$2" ]]
}

if ! version_gte "$DB_VERSION" "$REQUIRED_VERSION"; then
  error "databricks CLI version $DB_VERSION is too old. Need >= $REQUIRED_VERSION."
  echo "  Update: pip install --upgrade databricks-cli  OR  brew upgrade databricks"
  exit 1
fi
success "databricks CLI v$DB_VERSION"

# --- node / npm ---------------------------------------------------------------
if ! command -v node &>/dev/null; then
  error "node is not installed. Required for frontend build."
  exit 1
fi
success "node $(node --version)"

if ! command -v npm &>/dev/null; then
  error "npm is not installed. Required for frontend build."
  exit 1
fi
success "npm $(npm --version)"

# ==============================================================================
# Step 1: Authenticate
# ==============================================================================
step "Step 1: Authenticating (profile=${PROFILE})..."

auth_ok=false
if databricks current-user me --profile "$PROFILE" &>/dev/null; then
  auth_ok=true
  success "Already authenticated."
fi

if [[ "$auth_ok" == "false" ]]; then
  info "Running databricks auth login..."
  databricks auth login --host "$HOST" --profile "$PROFILE"
  if ! databricks current-user me --profile "$PROFILE" &>/dev/null; then
    error "Authentication failed. Please check your credentials."
    exit 1
  fi
  success "Authenticated successfully."
fi

USER_JSON=$(databricks current-user me --profile "$PROFILE" --output json 2>/dev/null || databricks current-user me --profile "$PROFILE" 2>/dev/null)
USER_EMAIL=$(echo "$USER_JSON" | grep -oE '"userName"\s*:\s*"[^"]+"' | head -1 | sed 's/.*"userName"\s*:\s*"//;s/"//')

if [[ -z "$USER_EMAIL" ]]; then
  error "Could not determine user email from databricks current-user me."
  exit 1
fi
success "Logged in as: $USER_EMAIL"

# ==============================================================================
# Step 2: Detect catalog
# ==============================================================================
step "Step 2: Detecting catalog..."

if [[ -z "$CATALOG" ]]; then
  info "No --catalog provided. Attempting auto-detection..."

  # Try to list catalogs via SQL
  CATALOG_LIST=$(databricks sql query --query "SHOW CATALOGS" --profile "$PROFILE" --output json 2>/dev/null || echo "")

  if [[ -n "$CATALOG_LIST" ]]; then
    # Pick the first non-system catalog (skip hive_metastore, system, samples, __databricks_internal)
    CATALOG=$(echo "$CATALOG_LIST" \
      | grep -oE '"catalog"\s*:\s*"[^"]+"' \
      | sed 's/.*"catalog"\s*:\s*"//;s/"//' \
      | grep -vE '^(hive_metastore|system|samples|__databricks_internal|main)$' \
      | head -1 || echo "")
  fi

  # If still empty, try deriving from workspace hostname
  if [[ -z "$CATALOG" ]]; then
    # Extract workspace name from host URL, e.g. https://my-workspace.cloud.databricks.com -> my_workspace_catalog
    WS_NAME=$(echo "$HOST" | sed -E 's|https?://||;s|\..*||' | tr '-' '_')
    CANDIDATE="${WS_NAME}_catalog"
    info "Trying derived catalog: $CANDIDATE"

    if databricks sql query --query "USE CATALOG \`${CANDIDATE}\`" --profile "$PROFILE" &>/dev/null; then
      CATALOG="$CANDIDATE"
    else
      CATALOG="main"
      warn "Derived catalog not found. Falling back to 'main'."
    fi
  fi

  success "Auto-detected catalog: $CATALOG"
else
  # Verify the user-provided catalog exists
  if ! databricks sql query --query "USE CATALOG \`${CATALOG}\`" --profile "$PROFILE" &>/dev/null; then
    error "Catalog '${CATALOG}' does not exist or you lack access."
    exit 1
  fi
  success "Using catalog: $CATALOG"
fi

# ==============================================================================
# Step 3: Update databricks.yml
# ==============================================================================
step "Step 3: Updating databricks.yml with workspace settings..."

DATABRICKS_YML="$SCRIPT_DIR/databricks.yml"

if [[ ! -f "$DATABRICKS_YML" ]]; then
  error "databricks.yml not found at $DATABRICKS_YML"
  exit 1
fi

# Update workspace host
sed -i.bak -E "s|^(  host:).*|\1 ${HOST}|" "$DATABRICKS_YML"

# Update default catalog_name in the variables section (line-level, first occurrence)
sed -i.bak -E '/^variables:/,/^[a-z]/{
  /catalog_name:/,/default:/{
    s|(    default:).*|\1 '"${CATALOG}"'|
  }
}' "$DATABRICKS_YML"

# Update dev target catalog
sed -i.bak -E '/^  dev:/,/^  [a-z]/{
  s|(      catalog_name:).*|\1 '"${CATALOG}"'|
}' "$DATABRICKS_YML"

# Update prod target catalog
sed -i.bak -E '/^  prod:/,/^[a-z]/{
  s|(      catalog_name:).*|\1 '"${CATALOG}"'|
}' "$DATABRICKS_YML"

# Update default schema_name
sed -i.bak -E '/^variables:/,/^[a-z]/{
  /schema_name:/,/default:/{
    s|(    default:).*|\1 '"${SCHEMA}"'|
  }
}' "$DATABRICKS_YML"

# Clean up backup files
rm -f "${DATABRICKS_YML}.bak"

success "databricks.yml updated (host=$HOST, catalog=$CATALOG, schema=$SCHEMA)"
warn "These are local overrides — do NOT commit if they are workspace-specific."

# ==============================================================================
# Step 4: Deploy pipeline via DAB
# ==============================================================================
step "Step 4: Deploying data pipeline via Databricks Asset Bundles..."

cd "$SCRIPT_DIR"

info "Running: databricks bundle deploy --target dev"
databricks bundle deploy --target dev --profile "$PROFILE"
success "Bundle deployed."

info "Running: databricks bundle run qrt_pipeline"
databricks bundle run qrt_pipeline --profile "$PROFILE"
success "Core pipeline job completed."

info "Running: databricks bundle run sf_pipeline"
databricks bundle run sf_pipeline --profile "$PROFILE"
success "Standard Formula pipeline job completed."

# ==============================================================================
# Step 5: Build frontend
# ==============================================================================
step "Step 5: Building frontend..."

FRONTEND_DIR="$SCRIPT_DIR/src/app/frontend"

if [[ ! -d "$FRONTEND_DIR" ]]; then
  error "Frontend directory not found: $FRONTEND_DIR"
  exit 1
fi

cd "$FRONTEND_DIR"
info "Installing npm dependencies..."
npm install
info "Building frontend..."
npm run build
cd "$SCRIPT_DIR"
success "Frontend built successfully."

# ==============================================================================
# Step 6: Deploy the Databricks App
# ==============================================================================
step "Step 6: Deploying Databricks App..."

WORKSPACE_PATH="/Workspace/Users/${USER_EMAIL}/solvency2-qrt-app"

# Create the app (handle "already exists" gracefully)
info "Creating app: $APP_NAME"
CREATE_OUTPUT=$(databricks apps create "$APP_NAME" --description "Solvency II QRT Reporting" --profile "$PROFILE" 2>&1 || true)

if echo "$CREATE_OUTPUT" | grep -qi "already exists"; then
  warn "App '$APP_NAME' already exists. Continuing with update."
elif echo "$CREATE_OUTPUT" | grep -qi "error"; then
  # If it's a real error (not "already exists"), print it but continue
  if ! echo "$CREATE_OUTPUT" | grep -qi "already exists"; then
    warn "App create returned: $CREATE_OUTPUT"
  fi
else
  success "App created."
fi

# Sync source files to workspace
info "Syncing app source to $WORKSPACE_PATH ..."
databricks sync "$SCRIPT_DIR/src/app" "$WORKSPACE_PATH" \
  --exclude node_modules \
  --exclude .venv \
  --exclude __pycache__ \
  --exclude .git \
  --exclude "frontend/src" \
  --exclude "frontend/public" \
  --exclude "frontend/README.md" \
  --exclude "frontend/.gitignore" \
  --exclude "frontend/eslint.config.js" \
  --exclude "frontend/tsconfig*" \
  --exclude "frontend/vite.config.ts" \
  --exclude "frontend/package*" \
  --profile "$PROFILE" \
  --watch=false
success "Source files synced."

# Upload frontend dist
info "Uploading frontend dist..."
databricks workspace import-dir "$SCRIPT_DIR/src/app/frontend/dist" \
  "$WORKSPACE_PATH/frontend/dist" \
  --overwrite \
  --profile "$PROFILE"
success "Frontend dist uploaded."

# Deploy the app
info "Deploying app..."
databricks apps deploy "$APP_NAME" \
  --source-code-path "$WORKSPACE_PATH" \
  --profile "$PROFILE"
success "App deployed."

# ==============================================================================
# Step 7: Grant permissions
# ==============================================================================
step "Step 7: Granting permissions to app service principal..."

# Get the app's service principal
APP_JSON=$(databricks apps get "$APP_NAME" --profile "$PROFILE" --output json 2>/dev/null || databricks apps get "$APP_NAME" --profile "$PROFILE" 2>/dev/null)
SP_ID=$(echo "$APP_JSON" | grep -oE '"service_principal_client_id"\s*:\s*"[^"]+"' | head -1 | sed 's/.*"service_principal_client_id"\s*:\s*"//;s/"//')
SP_NAME=$(echo "$APP_JSON" | grep -oE '"service_principal_name"\s*:\s*"[^"]+"' | head -1 | sed 's/.*"service_principal_name"\s*:\s*"//;s/"//')

if [[ -z "$SP_ID" ]]; then
  warn "Could not determine app service principal. Skipping permission grants."
  warn "You may need to manually grant the app's service principal access to:"
  warn "  - A SQL warehouse (CAN_USE)"
  warn "  - Catalog: $CATALOG (USE CATALOG)"
  warn "  - Schema: $CATALOG.$SCHEMA (USE SCHEMA, SELECT, MODIFY, CREATE TABLE)"
else
  info "App service principal: $SP_NAME (client_id=$SP_ID)"

  # --- SQL Warehouse access ---
  info "Finding a SQL warehouse..."
  WH_JSON=$(databricks warehouses list --profile "$PROFILE" --output json 2>/dev/null || echo "[]")
  WH_ID=$(echo "$WH_JSON" | grep -oE '"id"\s*:\s*"[^"]+"' | head -1 | sed 's/.*"id"\s*:\s*"//;s/"//')

  if [[ -n "$WH_ID" ]]; then
    info "Granting CAN_USE on warehouse $WH_ID..."
    databricks api post "/api/2.0/permissions/sql/warehouses/$WH_ID" \
      --profile "$PROFILE" \
      --json "{
        \"access_control_list\": [
          {
            \"service_principal_name\": \"$SP_NAME\",
            \"all_permissions\": [{\"permission_level\": \"CAN_USE\"}]
          }
        ]
      }" &>/dev/null && success "Warehouse permission granted." || warn "Could not grant warehouse permission (may already exist or require admin)."
  else
    warn "No SQL warehouses found. You may need to create one and grant access manually."
  fi

  # --- Catalog / Schema grants via SQL ---
  info "Granting catalog and schema permissions..."

  # Create schema if it doesn't exist
  databricks sql query --query "CREATE SCHEMA IF NOT EXISTS \`${CATALOG}\`.\`${SCHEMA}\`" --profile "$PROFILE" &>/dev/null || true

  GRANT_STATEMENTS=(
    "GRANT USE CATALOG ON CATALOG \`${CATALOG}\` TO \`${SP_NAME}\`"
    "GRANT USE SCHEMA ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SP_NAME}\`"
    "GRANT SELECT ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SP_NAME}\`"
    "GRANT MODIFY ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SP_NAME}\`"
    "GRANT CREATE TABLE ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SP_NAME}\`"
  )

  for stmt in "${GRANT_STATEMENTS[@]}"; do
    databricks sql query --query "$stmt" --profile "$PROFILE" &>/dev/null \
      && success "$stmt" \
      || warn "Failed: $stmt (may require admin privileges)"
  done
fi

# ==============================================================================
# Step 8: Summary
# ==============================================================================
step "Step 8: Deployment Summary"

echo ""
echo -e "${GREEN}${BOLD}Deployment complete!${NC}"
echo ""
echo -e "  ${BOLD}App URL:${NC}         ${HOST}/apps/${APP_NAME}"
echo -e "  ${BOLD}Catalog.Schema:${NC}  ${CATALOG}.${SCHEMA}"
echo -e "  ${BOLD}Workspace Path:${NC}  ${WORKSPACE_PATH}"
echo -e "  ${BOLD}Profile:${NC}         ${PROFILE}"
echo -e "  ${BOLD}User:${NC}            ${USER_EMAIL}"
echo ""
echo -e "To monitor the pipeline, check the ${BOLD}Workflows${NC} page in your Databricks workspace."
echo -e "To view the app, visit: ${CYAN}${HOST}/apps/${APP_NAME}${NC}"
echo ""
