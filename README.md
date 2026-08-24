# dd-chain-explorer

> Default branch: `main` | Active release: see `specs/releases/ACTIVE.md` (specs arrive
> at this repository's C-DAY cutover — v0.6.0)

This is the **main repository of the `dd-chain-explorer` spec context**: once the
`specs/` tree lands here (v0.6.0's C-DAY cutover), `specs/` is authoritative in this
repository, not in any of its siblings.

## Three-repo topology

The platform that used to live in one repository is split into three, by blast radius
and audience:

| Repository | Concern |
|---|---|
| **`dd-chain-infrastructure`** | All Terraform: AWS IAM/OIDC bootstrap, S3, Lambda infrastructure, the Databricks workspace objects declared by import (storage credentials, external locations, catalogs). Private. |
| **`dd-chain-explorer`** (this repo) | Databricks Asset Bundles (DLT pipelines, batch jobs, dashboards) and the two AWS Lambda functions + their shared library. Public. |
| **`dd-chain-capture`** | On-chain data capture, running independently. Untouched by this migration. |

```
dd-chain-capture (separate repo)
        │  writes raw JSON
        ▼
   S3 raw bucket   <──────────────  the ONLY integration boundary
        │                            between capture and everything downstream
        ▼
  Databricks DLT (apps/dabs/dlt_*)          ──┐
  Bronze → Silver → Gold, Unity Catalog       │  workspace infra (storage
        │                                     │  credentials, external locations,
        ├──► Lakeview dashboards               │  catalogs) declared and imported
        │      (apps/dabs/dashboard_*)         │  in dd-chain-infrastructure;
        │                                      │  DLT/workflows/dashboards
        └──► job_export_gold ──► S3 ──► Lambda  │  themselves are DABs, here.
                                    (apps/lambda/gold_to_dynamodb)
                                                 │
  Lambda contracts_ingestion (EventBridge)  ────┘
  Etherscan API → S3 raw/batch/  (a second, independent raw producer)

  All AWS/Terraform infrastructure for the above (IAM/OIDC, S3, Lambda
  function/layer resources) is declared and applied from dd-chain-infrastructure —
  never from this repository.
```

## This repository's two surfaces

| Surface | Path | Technology |
|---|---|---|
| Databricks Asset Bundles | `apps/dabs/` | DLT pipelines, batch jobs, Lakeview dashboards |
| AWS Lambda functions (2) | `apps/lambda/` | Python 3.12 |
| Shared library | `utils/` | `dm_chain_utils` — installed as a PATH requirement into the Lambda layer, never published to a public index |

There is **no `services/` directory** and **no Terraform** in this repository — every
infrastructure resource these two surfaces run on (the Lambda functions themselves, the
layer object, IAM roles, S3 buckets, the imported Databricks workspace objects) is
declared and applied in `dd-chain-infrastructure`. The seam between the two
repositories — the artifacts bucket, its content-addressed key shapes, who publishes and
who resolves — is pinned in `docs/cross-repo-contract.md`.

## Branches

`main` → `develop` → `feature/{M.m.p}`, one live feature branch at a time. Full contract:
`dd-gitflow-default` in the workspace's AI-entity surface (this repository's own
`AGENTS.md`, once authored, states its own copy of the applicable law).

## Tests

`tests/` — the `dabs`, `lambda` and `utils` suites. `pytest -p no:cacheprovider` from
the repository root. See `tests/README.md` for the per-suite breakdown and the one
deliberately-skipped `pyspark` tier.

## See also

- `docs/cross-repo-contract.md` — the artifact seam, the OIDC role map, the Databricks
  split, and the three-repo boundary diagram.
- `docs/runbooks/` — operational runbooks for this repository's surfaces.
