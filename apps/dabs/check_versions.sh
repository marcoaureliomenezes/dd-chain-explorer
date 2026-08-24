#!/usr/bin/env bash
# check_versions.sh — Dry-run: shows which DAB components would be deployed.
#
# Exit codes:
#   0 = one or more components have changes → proceed with deploy
#   1 = a VERSION file is missing → pipeline error
#   2 = nothing to deploy (all components already tagged in PRD) → skip CI DABs jobs
#
# Usage: ./check_versions.sh
#
# Reads VERSION from each component directory, fetches git tags, and compares
# against the PRD tag pattern: dabs/{bundle-name}-v{VERSION}

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Colors ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

echo -e "${BOLD}${CYAN}DABs Version Check — Dry Run${NC}"
echo "──────────────────────────────────────────────"

# Fetch remote tags (non-fatal if git not available, e.g. local dev)
echo "Fetching remote tags..."
git fetch --tags --quiet 2>/dev/null || echo -e "${YELLOW}Warning: could not fetch remote tags (offline?)${NC}"
echo ""

# ── Column header ───────────────────────────────────────────────────────────
printf "%-30s %-10s %-40s %s\n" "COMPONENT" "VERSION" "TAG" "STATUS"
printf "%s\n" "$(printf '%.0s─' {1..90})"

DEPLOY_COUNT=0
SKIP_COUNT=0
MISSING_COUNT=0

for comp_dir in "$SCRIPT_DIR"/*/; do
  comp=$(basename "$comp_dir")

  # Skip non-component dirs
  [[ "$comp" == _* ]] && continue
  [[ "$comp" == .* ]] && continue
  [[ ! -f "$comp_dir/databricks.yml" ]] && continue

  # Read VERSION
  VERSION_FILE="$comp_dir/VERSION"
  if [[ ! -f "$VERSION_FILE" ]]; then
    printf "%-30s %-10s %-40s ${RED}%s${NC}\n" "$comp" "???" "N/A" "MISSING VERSION"
    MISSING_COUNT=$((MISSING_COUNT + 1))
    continue
  fi

  VERSION=$(tr -d '[:space:]' < "$VERSION_FILE")
  BUNDLE_NAME=$(grep -m1 "^  name:" "$comp_dir/databricks.yml" | awk '{print $2}')
  TAG="dabs/${BUNDLE_NAME}-v${VERSION}"

  if git tag -l "$TAG" | grep -q "$TAG"; then
    printf "%-30s %-10s %-40s ${YELLOW}%s${NC}\n" "$comp" "$VERSION" "$TAG" "SKIP (already deployed)"
    SKIP_COUNT=$((SKIP_COUNT + 1))
  else
    printf "%-30s %-10s %-40s ${GREEN}%s${NC}\n" "$comp" "$VERSION" "$TAG" "DEPLOY"
    DEPLOY_COUNT=$((DEPLOY_COUNT + 1))
  fi
done

echo ""
echo "──────────────────────────────────────────────"
echo -e "Summary: ${GREEN}${DEPLOY_COUNT} to deploy${NC} | ${YELLOW}${SKIP_COUNT} skipped${NC} | ${RED}${MISSING_COUNT} missing VERSION${NC}"

# Exit codes
if [[ $MISSING_COUNT -gt 0 ]]; then
  echo -e "\n${RED}ERROR: ${MISSING_COUNT} component(s) are missing a VERSION file.${NC}"
  exit 1
fi

if [[ $DEPLOY_COUNT -eq 0 ]]; then
  echo -e "\n${YELLOW}Nothing to deploy. All components are already up-to-date.${NC}"
  exit 2
fi

echo -e "\n${GREEN}${DEPLOY_COUNT} component(s) will be deployed.${NC}"
exit 0
