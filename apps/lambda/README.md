# apps/lambda — Funções Lambda

Funções AWS Lambda do projeto `dd-chain-explorer`. Cada subfolder é uma função
independente com seu próprio `handler.py` e `requirements.txt`. Ambas
consomem `dm_chain_utils` (`utils/`) via Lambda Layer — construído a partir
do source local, nunca de um pacote público (ver "Build" abaixo).

---

## Funções

### `contracts_ingestion/`

**Trigger**: EventBridge Scheduler — execução horária (a schedule é declarada
em Terraform, `services/prd/06_lambda/`; ver T-B.7 desta release sobre a
pausa temporária dessa regra até `dd-chain-capture` entregar dados).

**Fluxo**: EventBridge → Lambda → Etherscan API → S3 `raw/batch/`

Busca transações de contratos populares na API Etherscan e salva os resultados
como JSON no bucket S3 de ingestão. Consumido depois por DLT (`apps/dabs/dlt_ethereum/`).

| Variável de ambiente | Descrição |
|----------------------|-----------|
| `S3_BUCKET` | Bucket S3 de ingestão raw |
| `S3_BUCKET_PREFIX` | Prefixo de destino (default `batch`) |
| `SSM_ETHERSCAN_PATH` | Path SSM com API keys Etherscan (default `/etherscan-api-keys`) |
| `DYNAMODB_TABLE` | Tabela DynamoDB single-table |
| `NETWORK` | Rede Ethereum (default `mainnet`) |

Suporta um evento `{"dry_run": true}` — valida conectividade SSM + DynamoDB
sem chamar a Etherscan API nem escrever no S3; é o gate de dry-run do CI.

### `gold_to_dynamodb/`

**Trigger**: S3 Event — `s3:ObjectCreated:*` no prefixo `exports/gold_api_keys/`.

**Fluxo**: `job_export_gold` (DABs) → S3 → Lambda → DynamoDB (entidade `CONSUMPTION`)

Lê arquivos JSON (NDJSON) exportados pelas views Gold do Databricks e
sincroniza os dados de consumo de API keys para a tabela DynamoDB. Não importa
`dm_chain_utils` — apenas `boto3` (nativo no runtime Lambda) e a stdlib.

| Variável de ambiente | Descrição |
|----------------------|-----------|
| `DYNAMODB_TABLE` | Tabela DynamoDB single-table (default `dm-chain-explorer`) |

---

## Testes

`tests/lambda/test_contracts_ingestion_handler.py` e
`tests/lambda/test_gold_to_dynamodb_handler.py` (repo-level `tests/`, não
dentro de `apps/lambda/`) — moto-mocked S3/DynamoDB/SSM, sem credenciais AWS
reais. Rodar com `make test` ou `pytest tests -p no:cacheprovider`.

---

## Build (Lambda Layer)

```bash
scripts/build_lambda_layer.sh
# imprime a última linha: LAYER_ZIP=<path> LAYER_SHA256=<hex>
```

Instala as dependências de terceiros hash-pinned de `apps/lambda/requirements.txt`
mais `dm_chain_utils` como **path requirement** (`--no-deps`), e zipa
`build/python/` para `.lambda_zip/dm_chain_utils_layer.zip` (untracked,
gitignored — build determinístico, mesmo sha256 a cada execução com os
mesmos inputs). Nunca instala a partir de um índice público sob o nome
`dm-chain-utils` — esse nome nunca foi publicado lá.

---

## Deploy

Terraform declara `source_code_hash`/`s3_key` a partir de variáveis
(`layer_s3_key`, `layer_sha256`) — nunca de um caminho local no working tree
(T-B.14). CI (`T-A.7`) constrói o layer, faz upload para
`s3://dm-chain-explorer-artifacts/lambda-layers/dm-chain-utils/<sha256>.zip`,
e passa a variável para o `terraform apply` correspondente.

| Ambiente | Stack Terraform |
|----------|------------------|
| DEV | `services/dev/02_lambda/` — `make dev_tf_apply` |
| PRD | `services/prd/06_lambda/` — `make tf_deploy ENV=prd` (via `scripts/ci/deploy_env.sh`, gated) |

As duas funções só existem em `dev` e `prd` (os únicos ambientes da plataforma).

---

## Estrutura

```
apps/lambda/
  requirements.txt          # hash-pinned, terceiros — scripts/build_lambda_layer.sh
  requirements.in            # source do pip-compile
  contracts_ingestion/
    handler.py
    requirements.txt         # dependências diretas (requests)
  gold_to_dynamodb/
    handler.py
    requirements.txt         # nenhuma dependência de terceiros
```
