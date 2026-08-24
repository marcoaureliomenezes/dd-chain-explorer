# apps/dabs — Databricks Asset Bundles (DABs)

Databricks resources for `dd-chain-explorer`, managed as one
[Databricks Asset Bundle](https://docs.databricks.com/en/dev-tools/bundles/index.html)
per component. Post-capture-retirement scope (v0.4.0+): this repo processes and
serves data delivered by the separate `dd-chain-capture` project — it does not
capture on-chain data itself.

---

## Bundles (7 surviving)

| Bundle | Resource(s) | Notes |
|---|---|---|
| `dlt_ethereum` | DLT pipeline `dm-ethereum` + in-bundle trigger job `dm-trigger-ethereum` | Bronze/Silver/Gold Ethereum medallion, 24 tables |
| `dlt_app_logs` | DLT pipeline `dm-app-logs` + in-bundle trigger job `dm-trigger-app-logs` | Bronze/Silver/Gold application-log medallion, 5 tables |
| `job_export_gold` | Job `dm-dm-export-gold` | Exports `g_api_keys.*` to S3 for the `gold_to_dynamodb` Lambda |
| `dashboard_api_health` | Lakeview dashboard | API key consumption |
| `dashboard_gas_analytics` | Lakeview dashboard | Gas price / consumption |
| `dashboard_hot_contracts` | Lakeview dashboard | Popular-contract ranking |
| `dashboard_network_overview` | Lakeview dashboard | Network metrics + block-production health |

Each bundle is self-contained: its own `databricks.yml`, `resources/`, `src/`,
`VERSION`. There is no root `databricks.yml` and no cross-bundle resource
reference — Databricks Asset Bundles resolve `${resources.*.id}` only within
the bundle that declares the resource (ADR-004 corollary, `specs/memory/architecture.md`).

**Removed in v0.5.0** (see `specs/releases/v0.5.0/CLOSURE.md` for the disposition of
each): `alert_api_keys`, `alert_dynamodb_deadlock`, `genie_ethereum` (the
`alerts`/`queries`/`genie_spaces` resource types are unknown to Databricks CLI
0.270 — these bundles validated but deployed zero live resources; reinstatement is
a deferred backlog candidate once the CLI supports them), `job_reconcile_orphans`
(notebook deleted 2026-05-22, bundle left dangling), `job_trigger_all` and
`job_full_refresh` (cross-bundle jobs superseded by each DLT bundle's own
in-bundle trigger job — see "Full refresh" below), `job_ddl_setup` and
`job_delta_maintenance` (every object either job touched is DLT-owned — DLT
serverless/Unity Catalog refuses to take over a pre-existing non-pipeline table
with the same name, and OPTIMIZE/VACUUM is unsupported on DLT streaming
tables/materialized views; both jobs never ran successfully and had no
non-DLT-owned object left to scope down to).

---

## Targets

Every bundle declares the same three targets. **The workspace host is never a
literal in any `databricks.yml`** — it is resolved from the `DATABRICKS_HOST`
environment variable at validate/deploy time (Databricks CLI 0.270 does not
support `${var.x}` interpolation on `workspace.host`, an authentication field —
the CLI's own validate warning says so). Export it before running any bundle
command:

```bash
export DATABRICKS_HOST="<the Free-Edition workspace URL>"
export DATABRICKS_CONFIG_PROFILE=DEFAULT   # or DATABRICKS_TOKEN — credentials still
                                            # resolve independently of DATABRICKS_HOST
```

| Target | Catalog | `run_as` | Notes |
|---|---|---|---|
| `dev` | `dev` | interactive user (unset — deploys as whoever authenticates) | `[dev] ` name prefix |
| `hml` | `hml` | interactive user (unset) | `[hml] ` name prefix; buckets pinned to `dm-chain-explorer-hml-raw-data` / `dm-chain-explorer-hml-lakehouse` |
| `prod` | `prd` | the `dm_spn_user` service principal | No PRD Databricks workspace exists yet (ADR-002) — `DATABRICKS_HOST` is never set for `prod` in this release, so `bundle validate -t prod` fails closed |

---

## Validate

```bash
for b in apps/dabs/*/; do
  (cd "$b" && databricks bundle validate -t dev && databricks bundle validate -t hml)
done
```

`validate -t prod` must fail (non-zero) whenever `DATABRICKS_HOST` is unset — that
is the guard, not a bug.

## Deploy (dev / hml only — no prod target exists)

```bash
for b in apps/dabs/*/; do
  (cd "$b" && databricks bundle deploy -t dev)
done
```

Dashboard bundles need one extra step first — see below.

## Dashboards — catalog templating

Lakeview dashboard JSON (`.lvdash.json`) is uploaded as an opaque file; the CLI
does not apply `${var.x}` substitution to its content. Each dashboard's dataset
SQL is therefore tracked as a `.lvdash.json.tmpl` source with a `{{CATALOG}}`
placeholder, and `render_dashboard_templates.sh` materialises the real
`.lvdash.json` (gitignored, generated) that the bundle's `file_path:` references:

```bash
./apps/dabs/render_dashboard_templates.sh --catalog dev   # or hml
```

Run this before `validate`/`deploy` on any dashboard bundle. `embed_credentials`
is `false` in every bundle target — the live dashboards currently published with
`embed_credentials=true` will self-correct on the next `bundle deploy` (T-C.6).

## Full refresh

There is no `job_full_refresh` bundle. Full refresh is one CLI call per pipeline:

```bash
databricks bundle summary -t dev   # find the deployed pipeline id
databricks pipelines start-update --full-refresh <pipeline-id>
```

## Version check / deploy helper

`check_versions.sh` and `deploy_all.sh` (both driven by each bundle's `VERSION`
file and `dabs/<bundle-name>-v<VERSION>` git tags) live at `apps/dabs/` top
level and auto-discover every directory carrying a `databricks.yml` — no bundle
list to keep in sync by hand.

---

## Maintenance

`job_ddl_setup` and `job_delta_maintenance` are gone (see "Removed" above) — DLT
owns every table's schema and OPTIMIZE/VACUUM lifecycle for all 29 objects.
There is currently no maintenance job for anything DLT does not own, because
nothing in this repo's catalog is Delta-but-not-DLT.

## Service principal

`prod`'s `run_as` (all 7 bundles) is the `dm_spn_user` service principal —
identified in bundle config by its **application id** (a UUID), never by a
personal email. Look it up when rotating:

```bash
databricks service-principals list --output json
```
