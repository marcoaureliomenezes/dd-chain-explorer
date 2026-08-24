# App Logs DLT Pipeline

DLT pipeline para ingestão de logs de aplicações — Bronze + Silver + Gold.

## Files generated

| File | Purpose |
|------|---------|
| `resources/dlt/pipeline_app_logs.yml` | DLT pipeline DABs config |
| `src/streaming/app_logs_pipeline.py` | DLT notebook (Bronze → Silver → Gold) |

## Databricks bundle setup

Add to `databricks.yml` include:
```yaml
include:
  - resources/dlt/pipeline_app_logs.yml
```

Override schedule in PROD target:
```yaml
targets:
  prod:
    resources:
      pipelines:
        pipeline_app_logs:
          schedule:
            pause_status: UNPAUSED
```

## Storage mode

| Environment | Compute | Table type |
|-------------|---------|------------|
| DEV / HML   | Serverless | MANAGED (Unity Catalog) |
| PROD        | Cluster | EXTERNAL (`LOCATION s3://...`) via DDL setup |
