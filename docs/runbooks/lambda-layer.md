# Runbook — resolving the `dm-chain-utils` Lambda layer artifact

**Owner:** whoever runs the first deploy against a fresh environment, or
diagnoses a failed infra-only plan/apply naming `resolve_layer.sh`.

**Re-pointed at v0.6.0 (T-L.3):** this runbook migrated from the legacy
single-repo platform. Every reference to `deploy_all_dm_applications.yml`
below is historical — that workflow DIES with the legacy repository
(superseded); the layer's producer is now **this repository's own
artifact-publish CI job**. Every `services/**` Terraform path below now lives
in the separate `dd-chain-infrastructure` repository, not in this one. The
authoritative statement of the key shape, the publisher/resolver split and
the OIDC roles involved is `docs/cross-repo-contract.md` — this runbook only
adds the operational "what do I do when it fails" detail.

## What this covers

`dd-chain-infrastructure`'s `services/prd/06_lambda` declares `layer_s3_key` /
`layer_sha256_b64` with **no default** — every plan or apply of that stack
must be told which built `dm-chain-utils` layer object to attach.
`layer_sha256_b64` feeds `aws_lambda_layer_version.source_code_hash` directly,
so it MUST be base64 of the raw sha256 digest (what AWS returns as
`Content.CodeSha256`) — never the hex digest, which is only the
content-addressed S3 key's suffix. Two different mechanisms supply those
values, for two different kinds of run:

| Run | Repository | Source of `layer_s3_key`/`layer_sha256_b64` |
|---|---|---|
| This repository's artifact-publish CI job | `dd-chain-explorer` (this repo) | Builds the layer from source **in that run** and uploads it — it always has a real, freshly-built value (see `docs/cross-repo-contract.md` §3). |
| Every infra-only plan/apply — `deploy_cloud_infra.yml` (`prd-plan`/`prd-apply`), `plan_on_pr.yml` (`plan-prd-lambda`), `drift_detection.yml` (`drift-prd-lambda`) | `dd-chain-infrastructure` | No build step of their own. They call `scripts/ci/resolve_layer.sh` to **read** the newest object this repository's CI already uploaded. |

`resolve_layer.sh` never builds or uploads anything — it only lists
`s3://dm-chain-explorer-artifacts/lambda-layers/dm-chain-utils/` and reports
the newest object.

## Why this matters

The layer object key is content-addressed by construction:
`lambda-layers/dm-chain-utils/<sha256>.zip`. An infra-only plan/apply that
made up a placeholder key/hash produced one of two problems depending on the
job:

- A **speculative plan** (`plan_on_pr.yml`, the `prd-plan` pre-gate review)
  that never reflected the layer prd would actually apply — an approver
  reviewing that plan had no signal about a real layer-content change.
- A **scheduled drift check** (`drift_detection.yml`) that reported a
  false-positive `layer_s3_key`/`layer_sha256_b64` diff on every single run,
  because the placeholder never matched what was actually live in state.

## Bootstrapping a fresh environment

On a brand-new environment (or after the artifacts bucket's `lambda-layers/`
prefix has been emptied), `resolve_layer.sh` has nothing to find and **fails
loudly**:

```
::error::No lambda-layer artifact found under s3://dm-chain-explorer-artifacts/lambda-layers/dm-chain-utils/ — ...
```

This is expected — no infra-only job builds the layer. **Run this
repository's artifact-publish CI job at least once first** (`dd-chain-explorer`,
`publish-artifacts.yml`, environment `dev` or `hml` — the artifact is
environment-agnostic, content-addressed by sha256; no `prod` lane exists until
the production Databricks environment does). It builds `dm-chain-utils` from source, computes its sha256,
uploads it to
`s3://dm-chain-explorer-artifacts/lambda-layers/dm-chain-utils/<sha256>.zip`
with `sha256=<sha256>` object metadata, assuming only the
`…-gha-artifacts-publish` role (`docs/cross-repo-contract.md` §4). Only then
can `dd-chain-infrastructure` apply `prd/06_lambda` against that real object.
Every infra-only job run after that can resolve it.

## Steps to unblock a failed infra-only plan/apply

1. Confirm the failure is `resolve_layer.sh`'s "No lambda-layer artifact
   found" (not an unrelated Terraform error) — read the failing step's log,
   in `dd-chain-infrastructure`.
2. Run this repository's (`dd-chain-explorer`) artifact-publish CI job
   (`publish-artifacts.yml`, environment `dev` or `hml`) from the Actions tab.
3. Confirm the object landed:
   ```
   aws s3api list-objects-v2 \
     --bucket dm-chain-explorer-artifacts \
     --prefix lambda-layers/dm-chain-utils/
   ```
4. Re-run the failed infra-only workflow, in `dd-chain-infrastructure`.

## Metadata cross-check

The upload step in this repository's artifact-publish CI job sets
`sha256=<hash>` as S3 object metadata on every upload. `resolve_layer.sh`
(in `dd-chain-infrastructure`) derives the hash from the object key's
basename (`<sha256>.zip`) — that alone is sufficient — and additionally
cross-checks it against this metadata tag when present, failing loudly on a
mismatch. A mismatch means the object's filename and its metadata disagree,
which can only happen from a hand-uploaded or corrupted object; never trust
it, re-run this repository's artifact-publish CI job instead of editing S3
by hand.
