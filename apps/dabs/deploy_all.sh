#!/usr/bin/env bash
# deploy_all.sh — Deploy DABs components in apps/dabs/.
#
# Each subdirectory with a databricks.yml is an autonomous DABs component.
# VERSION is read from the VERSION file inside each component directory and is
# informational only — the one version axis is the SDD release id (VERSION at the
# repo root), and re-deploying the same version is expected within a release; no
# tag-existence check gates a deploy.
#
# Usage:
#   ./deploy_all.sh dev                              # Deploy all to dev
#   ./deploy_all.sh hml                              # Deploy all to hml
#   ./deploy_all.sh prod                             # Deploy all to prod
#   ./deploy_all.sh dev dlt_ethereum job_ddl_setup   # Deploy specific components
#
# Exit codes:
#   0 = all deployments (if any) succeeded
#   1 = missing VERSION file or at least one deploy failed
#
# Environment variables (for CI/CD, F-07 — public-repo CI security audit, 2026-08-23):
#   DATABRICKS_HOST          — Databricks workspace URL
#   DATABRICKS_CLIENT_ID     — OAuth M2M service-principal client id
#   DATABRICKS_CLIENT_SECRET — OAuth M2M service-principal client secret
#
# The Databricks CLI (>=0.218) authenticates natively from these three env vars —
# no `databricks configure`/profile step or PAT is used here. All three are
# GitHub-environment-scoped secrets (dev/hml); production carries none (no prod
# Databricks workspace exists yet).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Parse arguments ──────────────────────────────────────────────────────────
TARGET="${1:-dev}"
shift || true

FILTER_COMPONENTS=("$@")

# ── Colors ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

echo -e "${BOLD}${CYAN}DABs Deploy — target: ${TARGET}${NC}"
echo ""

# ── Render dashboard templates BEFORE any bundle deploy (T-R.2, F-03) ────────
# Dashboard bundles reference a generated, gitignored *.lvdash.json that only
# exists after render_dashboard_templates.sh runs. A fresh CI checkout has none
# of these files — render them here, once, before the per-component deploy loop
# below ever calls `databricks bundle deploy`. Target->catalog: dev->dev,
# hml->hml, prod->prd (Unity Catalog naming, apps/dabs/*/databricks.yml).
case "$TARGET" in
  dev)  DASHBOARD_CATALOG="dev" ;;
  hml)  DASHBOARD_CATALOG="hml" ;;
  prod) DASHBOARD_CATALOG="prd" ;;
  *)
    echo -e "${RED}ERROR: Unknown target '${TARGET}' — expected dev|hml|prod${NC}"
    exit 1
    ;;
esac
echo -e "${CYAN}[RENDER]${NC} dashboard templates -> catalog=${DASHBOARD_CATALOG}"
"${SCRIPT_DIR}/render_dashboard_templates.sh" --catalog "${DASHBOARD_CATALOG}"
echo ""

DEPLOYED=()
FAILED=()

for component_dir in "${SCRIPT_DIR}"/*/; do
  component_name="$(basename "${component_dir}")"

  # Skip hidden dirs, _DEPRECATED etc., and any dir without databricks.yml
  [[ "$component_name" == _* ]] && continue
  [[ "$component_name" == .* ]] && continue
  [[ ! -f "${component_dir}databricks.yml" ]] && continue

  # Optional component filter
  if [[ ${#FILTER_COMPONENTS[@]} -gt 0 ]]; then
    match=0
    for f in "${FILTER_COMPONENTS[@]}"; do
      [[ "$component_name" == "$f" ]] && match=1 && break
    done
    [[ $match -eq 0 ]] && continue
  fi

  # ── Read VERSION (hard fail if missing) ────────────────────────────────
  VERSION_FILE="${component_dir}VERSION"
  if [[ ! -f "$VERSION_FILE" ]]; then
    echo -e "${RED}ERROR: Missing VERSION file in ${component_name}${NC}"
    FAILED+=("$component_name (missing VERSION)")
    continue
  fi
  VERSION=$(tr -d '[:space:]' < "$VERSION_FILE")

  # ── Deploy ─────────────────────────────────────────────────────────────
  echo -e "  ${CYAN}[DEPLOY]${NC}  ${component_name} @ ${VERSION} → target=${TARGET}"
  if (cd "${component_dir}" && databricks bundle deploy --target "${TARGET}"); then
    echo -e "  ${GREEN}[OK]${NC}     ${component_name}"
    DEPLOYED+=("${component_name}")
  else
    echo -e "  ${RED}[FAIL]${NC}   ${component_name}"
    FAILED+=("$component_name")
  fi
  echo ""
done

# ── Summary ─────────────────────────────────────────────────────────────────
echo "══════════════════════════════════════════════════"
echo -e "Deployed: ${GREEN}${#DEPLOYED[@]}${NC}  |  Failed: ${RED}${#FAILED[@]}${NC}"

if [[ ${#DEPLOYED[@]} -gt 0 ]]; then
  echo ""
  echo "Deployed components:"
  for entry in "${DEPLOYED[@]}"; do
    echo -e "  ${GREEN}✔${NC}  ${entry}"
  done
fi

# ── Fail fast if any missing VERSION or deploy failures ──────────────────────
if [[ ${#FAILED[@]} -gt 0 ]]; then
  echo ""
  echo -e "${RED}FAILED:${NC}"
  for f in "${FAILED[@]}"; do
    echo -e "  ${RED}✘${NC}  $f"
  done
  exit 1
fi

echo ""
echo -e "${GREEN}Done.${NC}"
exit 0
