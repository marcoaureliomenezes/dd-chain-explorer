#!/usr/bin/env bash
# render_dashboard_templates.sh — materialise the *.lvdash.json files consumed by
# `databricks bundle validate`/`deploy` from their tracked *.lvdash.json.tmpl source.
#
# Lakeview dashboards are opaque JSON uploaded as-is by the bundle sync step — the
# CLI does not apply `${var.x}` substitution to file content referenced via
# `file_path:`, only to bundle-config YAML. The `{{CATALOG}}` placeholder in each
# .tmpl is therefore rendered here, before validate/deploy, into the sibling
# .lvdash.json file that the bundle's `resources/dashboards/*.yml` references.
#
# The rendered .lvdash.json files are generated artifacts (see apps/dabs/.gitignore)
# — never hand-edit them; edit the .tmpl and re-run this script.
#
# This script is the ONE place that maps a bundle target to its Unity Catalog
# name (dev -> dev, prod -> prd, matching apps/dabs/*/databricks.yml). Every
# validate/deploy path (Makefile, deploy_all.sh, CI) calls it before touching a
# dashboard bundle.
#
# Usage: ./render_dashboard_templates.sh --target {dev|prod}

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TARGET=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

case "$TARGET" in
  dev)  CATALOG="dev" ;;
  prod) CATALOG="prd" ;;
  *)
    echo "Usage: $0 --target {dev|prod}" >&2
    exit 1
    ;;
esac

count=0
while IFS= read -r -d '' tmpl; do
  out="${tmpl%.tmpl}"
  sed "s/{{CATALOG}}/${CATALOG}/g" "$tmpl" > "$out"
  echo "rendered: $out (catalog=${CATALOG})"
  count=$((count + 1))
done < <(find "$SCRIPT_DIR" -path '*/src/dashboards/*.lvdash.json.tmpl' -print0)

if [[ "$count" -eq 0 ]]; then
  echo "No *.lvdash.json.tmpl files found under $SCRIPT_DIR" >&2
  exit 1
fi

echo "Rendered $count dashboard(s) for target '${TARGET}' (catalog '${CATALOG}')."
