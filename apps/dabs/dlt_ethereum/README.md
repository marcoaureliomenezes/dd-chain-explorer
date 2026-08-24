# Ethereum DLT Pipeline

DLT pipeline para processamento de dados Ethereum — Bronze + Silver + Gold.

## Files generated

| File | Purpose |
|------|---------|
| `resources/dlt/pipeline_ethereum.yml` | DLT pipeline DABs config |
| `src/streaming/ethereum_pipeline.py` | DLT notebook (Bronze → Silver → Gold) |

## Databricks bundle setup

Add to `databricks.yml` include:
```yaml
include:
  - resources/dlt/pipeline_ethereum.yml
```

Override schedule in PROD target:
```yaml
targets:
  prod:
    resources:
      pipelines:
        pipeline_ethereum:
          schedule:
            pause_status: UNPAUSED
```

## Storage mode

| Environment | Compute | Table type |
|-------------|---------|------------|
| DEV / HML   | Serverless | MANAGED (Unity Catalog) |
| PROD        | Cluster | EXTERNAL (`LOCATION s3://...`) via DDL setup |
