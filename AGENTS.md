# dd-chain-explorer — Repo Context

> Loaded by every harness (Claude Code, Codex, Kimi Code) working in this repo.
> Edit this file directly — it is not lib-originated.

## Repo purpose

This is the Databricks + Lambda application half of the `dd-chain` platform:
Databricks Asset Bundles (DLT pipelines, batch jobs, Lakeview dashboards)
under `apps/dabs/`, two AWS Lambda functions under `apps/lambda/`, and their
shared library under `utils/`. It publishes the deployment artifacts both
sibling repositories consume; it never declares infrastructure itself.

## No infrastructure lives here

This repository declares **no** infrastructure. All Terraform — AWS
IAM/OIDC, S3, Lambda function/layer resources, the Databricks
workspace-level objects (storage credentials, external locations, catalogs)
adopted by import — belongs to `dd-chain-infrastructure`. Blockchain data
capture belongs to `dd-chain-capture`. Never add a `services/` directory,
a `.tf` file, or a Terraform CI workflow here.

The boundary artifacts between this repository and its siblings:

- **The raw S3 bucket** — the capture seam. `dd-chain-capture` writes raw
  blockchain JSON there; this repository's DLT pipelines read from it. No
  other integration point exists between the two repositories.
- **The artifacts bucket** — the Lambda seam. This repository's CI builds
  and publishes the layer and both handler zips to content-addressed keys;
  `dd-chain-infrastructure`'s Terraform resolves and deploys them. The full
  contract — key shapes, prefixes, the five-role OIDC map, the Databricks
  split — is pinned once in `docs/cross-repo-contract.md`; consult it, do
  not restate it.

## This is the main repo of the spec context

`specs/` is authoritative in this repository, not in either sibling. Once
the v0.6.0 cutover lands the tree here, read `specs/constitution.md`,
`specs/releases/ACTIVE.md` and `specs/memory/**` before any change — the
same SDD flow as every dadaia-workspace context (`DADAIA.md` §1/§6).

## This repository is public

Nothing that is not public-grade may ever be committed: no secrets, no
tokens, no AWS account ids, no hostnames, no personal data, no
operator-local paths. Terraform state, IAM role ARNs, and any credential
value stay entirely in `dd-chain-infrastructure`, which is private until
the operator validates it. When in doubt, leave it out and ask.

## Spec structure

1. `specs/constitution.md` — durable, repo-scoped laws.
2. `specs/releases/ACTIVE.md` — active release and phase.
3. `specs/releases/<release-id>/{SPEC,PLAN,TASKS}.md` — each must carry
   `**Status:** Aprovado` before it authorizes implementation.
4. `specs/memory/*.md`, `specs/memory/product/*.md` — current product truth.

Legacy/history: `specs/_archive/**` — read-only, never a source of approval.
