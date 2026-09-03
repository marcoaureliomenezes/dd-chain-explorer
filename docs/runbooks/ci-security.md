# Runbook — CI/CD security posture (public-repo hardening)

**Source:** public-repo CI security audit, 2026-08-23 (security-reviewer). This runbook
records the software-engineer-owned remediations applied to `.github/workflows/**` and
`scripts/ci/**`. Repo-settings-only findings (environment protection rules, branch
protection, Actions allow-list, Dependabot toggles) are operator actions and are not
repeated here.

**Re-pointed at v0.6.0 (T-L.3):** this runbook migrated from the legacy single-repo
platform and documents an audit of the workflows/scripts that existed there.
`scripts/ci/**` and every Terraform-facing workflow this document names
(`deploy_cloud_infra.yml`, `destroy_cloud_infra.yml`, `destroy_all_cloud_infra.yml`,
`drift_detection.yml`, `plan_on_pr.yml`) now live in the separate
`dd-chain-infrastructure` repository, along with every `services/**` path referenced
below. `deploy_all_dm_applications.yml` DIES with the legacy repository (superseded) —
its build/deploy responsibilities are split across this repository's own CI (layer +
handler-zip publish, DABs validate/deploy) and `dd-chain-infrastructure`'s CI (Lambda
Terraform apply). The remediations recorded here (F-01 through F-12) describe the
posture each successor workflow inherited, not a live workflow in this repository.

## F-01 — never upload the `tfplan` binary as a public artifact

**Why this mattered.** `terraform plan -out=tfplan` writes a binary plan file that
contains every referenced value in plaintext, including `TF_VAR_*` secrets (e.g.
`DATABRICKS_UC_EXTERNAL_ID`) — Terraform's `sensitive = true` only redacts *display*
output, never the plan/state contents
(<https://developer.hashicorp.com/terraform/tutorials/configuration-language/sensitive-variables>).
GitHub Actions artifacts of a public repository are downloadable by **anyone with read
access to the repo** — i.e. everyone
(<https://docs.github.com/en/rest/actions/artifacts>). Uploading `tfplan` therefore
published the plaintext secret to the world for the life of the artifact's retention
window.

**What changed.**

- `scripts/ci/plan_env.sh` no longer stages the `tfplan` binary under
  `$PLAN_ARTIFACT_DIR/<stack-id>/` — it stages **only** the redacted `plan.txt` text
  summary that `terraform plan -no-color` already produces (via `scripts/ci/tf_plan.sh`).
- `.github/workflows/deploy_cloud_infra.yml`'s `prd-plan` / `hml-plan` jobs upload
  `path: .plan-artifacts/**/plan.txt` (never a glob that could pick up a `tfplan` file)
  with `retention-days: 1` (down from 7).
- `scripts/ci/deploy_env.sh`'s apply phase (`prd-apply` / `hml-apply`) **never** consumes
  a saved plan binary. Every stack is now unconditionally re-planned locally, under the
  environment-gated deploy role, and the fresh plan is compared against the downloaded
  approved `plan.txt` via the existing divergence guard,
  `scripts/ci/plan_gate_check.sh plan-diff` (ADR-R6-5) — this guard already compared
  sorted resource-address + action sets and add/change/destroy counts, so no new script
  was needed; it is now the *only* path (previously it only ran for stacks downstream of
  an in-run upstream change). A divergence, or a missing approved summary for a
  non-deferred stack, still fails the run closed (exit 3) with a published diff.

**Operator follow-up (tracked separately, not software-engineer scope):** rotate
`DATABRICKS_UC_EXTERNAL_ID` and delete any pre-existing `hml-saved-plans` /
`prd-saved-plans` artifacts that still carry a `tfplan` binary from before this fix
(`gh api -X DELETE repos/<org>/<repo>/actions/artifacts/<id>`).

## F-04 — `id-token: write` at job scope only

Workflow-level `permissions:` in `deploy_all_dm_applications.yml`,
`deploy_cloud_infra.yml`, `destroy_all_cloud_infra.yml`, `destroy_cloud_infra.yml`, and
`drift_detection.yml` is now `contents: read` only. `id-token: write` is granted solely
on the jobs that call `aws-actions/configure-aws-credentials` (matching the pattern
`plan_on_pr.yml` already used).

## F-06 — commit-SHA pins + Dependabot

`aws-actions/configure-aws-credentials` was pinned to `ff717079…`, the SHA of the
**annotated tag object** for the floating `v4` tag — not a commit SHA. It is now pinned
to `7474bc4690e29a8392af63c5b98e7449536d5c3a` (`v4.3.1`'s actual commit, verified via
`gh api repos/aws-actions/configure-aws-credentials/git/ref/tags/v4.3.1` →
`git/tags/<sha>`). The other 6 third-party actions were refreshed the same way, to each
action's current stable release, always resolving through `git/ref/tags/<tag>` and, when
the ref is an annotated tag object, dereferencing to the underlying commit:

| Action | Was | Now |
|---|---|---|
| `actions/checkout` | v4.3.1 | v7.0.1 |
| `actions/setup-python` | v5.6.0 | v7.0.0 |
| `hashicorp/setup-terraform` | v3.1.2 | v4.0.1 |
| `databricks/setup-cli` | v0.218.0 | v1.13.0 |
| `actions/upload-artifact` | v4.6.0 | v7.0.1 |
| `actions/download-artifact` | v4.1.8 | v8.0.1 |

Every input each workflow actually sets (`fetch-depth`, `persist-credentials`,
`python-version`, `terraform_version`, `terraform_wrapper`, `name`, `path`,
`retention-days`, `if-no-files-found`) was confirmed present and unchanged in the new
release's `action.yml` before pinning. `.github/dependabot.yml` now tracks
`github-actions` (repo root) and `pip` (`apps/lambda/`) weekly so pins stay current
automatically going forward.

## F-07 — environment-scoped Databricks secrets, fail-closed auth wiring

`deploy_all_dm_applications.yml`'s `deploy-dabs` job selected `DATABRICKS_HOST`/
`DATABRICKS_TOKEN` via a `cond && secretA || secretB` ternary against repo-scoped
`DATABRICKS_{PROD,HML,DEV}_{HOST,TOKEN}` secrets — several of which were undefined
(drift, F-08), so an empty secret silently fell through to another environment's
credential. The coordinator provisioned OAuth M2M service-principal **environment**
secrets (`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`) on the
`dev` and `hml` GitHub environments; `production` carried none at the time.
*(v0.6.0, 2026-09-02: `hml` retired; `production` now carries its own three secrets
for the official-account workspace — see `docs/cross-repo-contract.md` §5.)* `deploy-dabs` now reads only those three environment-scoped secrets
and asserts all three are non-empty before invoking `deploy_all.sh` — a `target=prod`
dispatch fails closed by design instead of silently deploying with an empty/wrong
credential.

`deploy_cloud_infra.yml`'s `hml-plan`/`hml-apply` and `destroy_cloud_infra.yml`'s
`hml-destroy` jobs referenced repo-scoped `DATABRICKS_ACCOUNT_ID`/`CLIENT_ID`/
`CLIENT_SECRET` that no `databricks` Terraform provider block or CLI call on those
paths ever consumed (grep-verified: no `provider "databricks"` block exists anywhere
in `services/**`) — dead reads of secrets slated for retirement, removed.
`TF_VAR_databricks_hml_uc_external_id` (a real, consumed IAM-trust-policy variable) is
unchanged. `apps/dabs/deploy_all.sh` and `scripts/ci/deploy_env.sh` docstrings updated
to match (the latter's claimed `TF_VAR_databricks_client_id`/`secret` requirement was
stale drift, never actually read by the script).

**Addendum — skip prd/06_lambda advisory plans when the layer store is absent.**
`plan_on_pr.yml`'s `plan-prd-lambda` and `drift_detection.yml`'s `drift-prd-lambda` are
non-gating advisory lanes, but `scripts/ci/resolve_layer.sh` failed the whole job
whenever `s3://dm-chain-explorer-artifacts` had no layer object — and that bucket is
deliberately not provisioned yet during the v0.5.0 live cutover
(`docs/runbooks/v0.5.0-live-cutover.md` §5). `resolve_layer.sh` now collapses a missing
bucket (`NoSuchBucket`) into the same "no artifact found" signal an empty prefix already
produced (any other AWS error still fails hard). The new
`scripts/ci/resolve_layer_or_skip.sh` wraps it for these two advisory lanes only: on
"not yet provisioned" it emits a `::warning::` and reports `skip=true` (exit 0), and the
Terraform Plan step is skipped via `if: steps.resolve-layer.outputs.skip != 'true'`.
`deploy_cloud_infra.yml`'s `prd-plan` (the pre-gate plan feeding the environment-gated
`prd-apply`) is untouched — it still calls `resolve_layer.sh` directly and fails hard.

## F-09 — `env:` indirection for every flagged `run:` interpolation

zizmor's `template-injection` audit (error/high-level, `--persona auditor`) flagged four
raw `${{ }}` expressions substituted directly into `run:` script bodies: `inputs.confirm`
(`destroy_all_cloud_infra.yml`), `github.event.inputs.full_destroy`
(`destroy_cloud_infra.yml`), `github.base_ref` (`plan_on_pr.yml`), and `inputs.target`
(every occurrence in `deploy_all_dm_applications.yml`'s `run:` bodies — `preflight-oidc`,
`check-version`, `deploy-lambda`'s stack-path resolution, `deploy-dabs`). All four now go
through a step-level `env:` block and are read back as a shell variable (`${CONFIRM}`,
`${FULL_DESTROY}`, `${BASE_REF}`, `${TARGET}`) — no `${{ }}` is substituted directly into
a `run:` body for any of them anywhere in the six original workflows.

## F-10 — `persist-credentials: false` on every checkout; `master` -> `main`

Every `actions/checkout` step (37 of 38 — zizmor `artipacked`, warning-level) now carries
`persist-credentials: false`: the default persists the `GITHUB_TOKEN` into `.git/config`
for the rest of the job, unnecessary attack surface once nothing after checkout needs to
authenticate as the runner. The sole exception is `deploy_cloud_infra.yml`'s
`prd-create-tag` job, which legitimately needs the persisted credential to `git push` the
release tag — left at its default (`true`), now with an explanatory comment.

That job's "Tag master" step referenced a branch that has never existed in this repo
(`main` is the only long-lived branch): `git fetch origin master` / `git checkout master`
would have failed outright the first time this job actually ran. Fixed to `main` (fetch,
checkout, step name, and the step-summary text).

## F-11 — `role-session-name` + `mask-aws-account-id`; redacted identity echoes

All 29 `aws-actions/configure-aws-credentials` steps across the six workflows now carry a
static, per-job `role-session-name` (`gha-<workflow>-<job>`, e.g.
`gha-deploy-cloud-infra-prd-apply` — a literal string, never an interpolated event/input
value, so CloudTrail can attribute a session back to the workflow/job that created it) and
`mask-aws-account-id: true`.

The 5 "Verify assumed role (sts get-caller-identity)" evidence steps printed the full,
unmasked 12-digit account id + role ARN to public build logs. Each now prints only the ARN
with the account id masked: `aws sts get-caller-identity --query Arn --output text | sed
-E 's/[0-9]{12}/************/'`.

## F-12 — `harden-runner` on every job, a `zizmor` CI gate, and Scorecard

`step-security/harden-runner@05e31511f85b41b11d1cf0ef85d0992719546e2c` (`v2.21.0`,
commit-verified via `gh api repos/step-security/harden-runner/git/ref/tags/v2.21.0`) is
now the first step of all 48 jobs across the six original workflows,
`egress-policy: audit` (observe-only for now — moving to `block` with a reviewed
allowlist is a follow-up, not blocked on this session).

`plan_on_pr.yml`'s `quality` job now runs `zizmor` (pinned `1.29.0` via `pip install`,
matching the version this audit itself used — not hash-locked into
`apps/lambda/requirements-dev.txt`, since zizmor is a workflow-lint tool with no runtime
coupling to the Lambda dev environment) with `--persona regular --offline --min-severity
high` against `.github/workflows` on every PR. This repo is currently clean at that
threshold: `zizmor --persona auditor --offline .github/workflows` reports 64 findings —
21 informational, 35 low, 8 medium, **0 high** — down from the original audit's 9
error-level findings (all in `template-injection`, now closed by F-09) and 0
`excessive-permissions` (F-04) findings. Remaining findings are non-actionable at
workflow-code level: 46 `template-injection` (info/help — benign `env.*`/`steps.*.outputs.*`/
`vars.*` reads inside `run:`, not attacker-controlled), 7 `secrets-outside-env`
(`DATABRICKS_UC_EXTERNAL_ID` read by the pre-gate plan jobs that deliberately carry no
`environment:` — F-02, operator scope, tracked separately), 8 `undocumented-permissions`
and 2 `concurrency-limits` (help-level style suggestions), and 1 `artipacked`
(`prd-create-tag`'s intentional `persist-credentials: true`, documented above under
F-10).

New `.github/workflows/scorecard.yml` runs OpenSSF Scorecard weekly (Monday 06:30 UTC,
offset from `drift_detection.yml`'s 06:00) plus `workflow_dispatch`, pinned to
`ossf/scorecard-action@2d1146689b8cda280b9bc96326124645441f03bc` (`v2.4.4`, dereferenced
from its annotated tag via `git/tags/<sha>`), workflow-level `permissions: {}` with only
the `analysis` job granted `contents: read` + `id-token: write` (least privilege —
`security-events: write` is correctly omitted: this repo is public and does not upload
SARIF to GitHub code scanning), `publish_results: true` for the public badge/API.

Repo-settings-only findings (F-02, F-03, F-05, F-08, F-13) remain entirely with the
operator, as noted at the top of this runbook.
