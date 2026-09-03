################################################################################
# dd-chain-explorer — Makefile
#
# Three-repo segregation (v0.6.0, T-X.1): this repository owns the Databricks
# Asset Bundles surface (apps/dabs), the AWS Lambda handlers + shared
# dm-chain-utils library (apps/lambda, utils), and their tests. It declares no
# Terraform and no infrastructure lifecycle — that lives in the separate
# dd-chain-infrastructure repository. Capture (blockchain ingestion) lives in
# the separate dd-chain-capture repository. Nothing here plans, applies, or
# destroys any infrastructure stack.
#
# `make -n <target>` (dry-run) must resolve for every target below — that is
# this file's own acceptance bar.
################################################################################

SHELL := /bin/bash

.PHONY: help test lint typecheck check build_lambda_layer \
        dabs_validate_all dabs_deploy_all dabs_destroy_all dabs_run \
        dabs_run_dlt_ethereum dabs_run_dlt_app_logs dabs_run_export_gold \
        dabs_deploy_dashboards dabs_status

help:
	@echo "dd-chain-explorer — available targets:"
	@echo ""
	@echo "  Quality gates"
	@echo "    make test              - pytest: tests/"
	@echo "    make lint              - ruff format --check + ruff check (repo-wide)"
	@echo "    make typecheck         - mypy --config-file pyproject.toml (strict scope)"
	@echo "    make check             - lint + typecheck + test"
	@echo ""
	@echo "  Lambda layer"
	@echo "    make build_lambda_layer - scripts/build_lambda_layer.sh (see its docstring)"
	@echo ""
	@echo "  Databricks Asset Bundles (apps/dabs/<bundle>/, default TARGET=dev)"
	@echo "    make dabs_validate_all             - validate every bundle"
	@echo "    make dabs_render TARGET=dev         - render dashboard templates (auto-run by validate/deploy)"
	@echo "    make dabs_deploy_all TARGET=dev     - deploy every bundle"
	@echo "    make dabs_destroy_all               - destroy every bundle (target=dev)"
	@echo "    make dabs_run BUNDLE=<name> JOB=<key> TARGET=dev  - run one job/pipeline"
	@echo "    make dabs_run_dlt_ethereum | dabs_run_dlt_app_logs | dabs_run_export_gold"
	@echo "    make dabs_deploy_dashboards TARGET=dev - deploy all 4 dashboards"
	@echo "    make dabs_status                    - summary of the main dev bundles"
	@echo ""
	@echo "  Infrastructure (Terraform, OIDC bootstrap, IAM) lives in the separate"
	@echo "  dd-chain-infrastructure repository — nothing here plans or applies it."

################################################################################
# Quality gates — same commands the pre-push chokepoint and CI's quality job run
################################################################################

test:
	pytest tests -p no:cacheprovider

lint:
	ruff format --check . --no-cache
	ruff check . --no-cache

typecheck:
	mypy --config-file pyproject.toml

check: lint typecheck test

################################################################################
# Lambda layer — scripts/build_lambda_layer.sh
################################################################################

build_lambda_layer:
	bash scripts/build_lambda_layer.sh

################################################################################
# Databricks Asset Bundles (apps/dabs/<bundle>/)
#
# Every target below discovers bundles by globbing apps/dabs/*/databricks.yml
# rather than hardcoding a bundle list, so it never goes stale as bundles are
# added or removed.
################################################################################

DABS_DIR    := apps/dabs
TARGET      ?= dev

# Dashboard bundles reference a generated, gitignored *.lvdash.json that only
# exists after the template render; a fresh checkout has none. Every
# validate/deploy target below depends on this one, so CI and the operator
# never validate a dashboard bundle against a missing file.
dabs_render:
	@$(DABS_DIR)/render_dashboard_templates.sh --target $(TARGET)

dabs_validate_all: dabs_render
	@echo ">>> Validating every apps/dabs bundle (target=$(TARGET))..."
	@FAILED=""; \
	for d in $(DABS_DIR)/*/; do \
	  name=$$(basename "$$d"); \
	  [[ ! -f "$$d/databricks.yml" ]] && continue; \
	  printf "  %-30s " "$$name"; \
	  if (cd "$$d" && databricks bundle validate --target $(TARGET) > /dev/null 2>&1); then \
	    echo "OK"; \
	  else \
	    echo "FAIL"; FAILED="$$FAILED $$name"; \
	  fi; \
	done; \
	if [ -n "$$FAILED" ]; then echo "FAILED:$$FAILED"; exit 1; fi
	@echo ">>> All bundles OK."

dabs_deploy_all: dabs_render
	@echo ">>> Deploying every apps/dabs bundle (target=$(TARGET))..."
	@for d in $(DABS_DIR)/*/; do \
	  name=$$(basename "$$d"); \
	  [[ ! -f "$$d/databricks.yml" ]] && continue; \
	  echo "  >>> deploy $$name"; \
	  (cd "$$d" && databricks bundle deploy --target $(TARGET)); \
	done
	@echo ">>> Deploy complete."

dabs_destroy_all:
	@echo ">>> Destroying every apps/dabs bundle (target=$(TARGET))..."
	@for d in $(DABS_DIR)/*/; do \
	  name=$$(basename "$$d"); \
	  [[ ! -f "$$d/databricks.yml" ]] && continue; \
	  echo "  >>> destroy $$name"; \
	  (cd "$$d" && databricks bundle destroy --target $(TARGET) --auto-approve 2>&1) || true; \
	done
	@echo ">>> Destroy complete."

# Run one job or pipeline trigger by its bundle resource key.
# Usage: make dabs_run BUNDLE=dlt_ethereum JOB=workflow_trigger_ethereum
dabs_run:
	@if [ -z "$(BUNDLE)" ] || [ -z "$(JOB)" ]; then \
	  echo "Usage: make dabs_run BUNDLE=<bundle-dir-name> JOB=<resource-key> [TARGET=dev]"; \
	  exit 1; \
	fi
	cd $(DABS_DIR)/$(BUNDLE) && databricks bundle run --target $(TARGET) $(JOB)

dabs_run_dlt_ethereum:
	$(MAKE) dabs_run BUNDLE=dlt_ethereum JOB=workflow_trigger_ethereum TARGET=$(TARGET)

dabs_run_dlt_app_logs:
	$(MAKE) dabs_run BUNDLE=dlt_app_logs JOB=workflow_trigger_app_logs TARGET=$(TARGET)

dabs_run_export_gold:
	$(MAKE) dabs_run BUNDLE=job_export_gold JOB=workflow_dm_export_gold TARGET=$(TARGET)

# Deploys every dashboard_* bundle with the first available SQL Warehouse id
# auto-discovered and passed as --var warehouse_id (falls back to no --var if
# the CLI/warehouse lookup is unavailable, matching each bundle's own default).
dabs_deploy_dashboards: dabs_render
	@echo ">>> Deploying dashboards (target=$(TARGET))..."
	@_WH_ID=$$(databricks warehouses list --output json 2>/dev/null \
	  | python3 -c "import sys,json; whs=json.load(sys.stdin).get('warehouses',[]); print(next((w['id'] for w in whs),''))" 2>/dev/null); \
	for d in $(DABS_DIR)/dashboard_*/; do \
	  name=$$(basename "$$d"); \
	  echo "  >>> $$name"; \
	  if [ -n "$$_WH_ID" ]; then \
	    (cd "$$d" && databricks bundle deploy --target $(TARGET) --var "warehouse_id=$$_WH_ID"); \
	  else \
	    (cd "$$d" && databricks bundle deploy --target $(TARGET)); \
	  fi; \
	done
	@echo ">>> Dashboards deployed."

dabs_status:
	@echo ">>> Status of the main dev bundles..."
	@for d in dlt_ethereum dlt_app_logs job_export_gold; do \
	  [[ ! -d "$(DABS_DIR)/$$d" ]] && continue; \
	  echo "=== $$d ==="; \
	  (cd $(DABS_DIR)/$$d && databricks bundle summary --target dev 2>&1 | head -25) || true; \
	  echo ""; \
	done
