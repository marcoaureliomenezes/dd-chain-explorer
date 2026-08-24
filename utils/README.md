# dm-chain-utils

Shared Python library for `dd-chain-explorer`'s AWS Lambda functions
(`apps/lambda/`). **Never published to a public package index** — it is
installed as a local **path** requirement (`pip install ./utils --no-deps`),
which is also what closes the dependency-confusion risk a public `==` pin on
an unclaimed package name would otherwise create. `scripts/build_lambda_layer.sh`
is the only supported way to build a deployable layer from this source.

## Live modules (imported by a Lambda handler today)

| Module | Class | Used by |
|--------|-------|---------|
| `dm_dynamodb` | `DMDynamoDB` | both Lambda handlers — single-table DynamoDB CRUD/query |
| `dm_etherscan` | `EtherscanClient` | `contracts_ingestion` — Etherscan API v2 client |
| `dm_parameter_store` | `ParameterStoreClient` | both handlers — SSM Parameter Store reads |

Every public method of these three modules is covered by
`tests/utils/test_dm_dynamodb.py`, `tests/utils/test_dm_etherscan.py`, and
`tests/utils/test_dm_parameter_store.py` (moto-mocked AWS, no live credentials).

## Legacy capture-era modules — removed

The 6 capture-era helper modules (Kinesis stream handler, SQS queue handler,
Firehose Direct-Put handler, the CloudWatch logging handler, the on-chain RPC
client, and the API-key rotation manager) had zero live callers since capture
retirement — the ingestion they supported is now owned entirely by the
separate `dd-chain-capture` repository, which writes to S3 directly. They were
deleted, along with `utils/pyproject.toml`'s corresponding third-party
dependencies, in release v0.5.0's T-D.3, per the `qa-engineer` deletion
verdict required by `dadaia-test-stewardship`.

## Version

`utils/pyproject.toml`'s `version` and `dm_chain_utils.__version__` track the
SDD release id — see `specs/memory/tech-stack.md` for the single-version-axis
rule (no more `0.2.9`-style artifact version separate from the release).
