# dm-export-gold

Export Gold API key consumption views to S3

## Files generated

| File | Purpose |
|------|---------|
| `resources/workflows/workflow_dm_export_gold.yml` | Databricks workflow config |
| `src/batch/dm_export_gold/export_gold.py` | Task implementation |
| `src/batch/dm_export_gold/pyproject.toml` | Isolated wheel definition |

## Storage mode

| Environment | Compute | Table type | `--storage-mode` |
|-------------|---------|------------|-----------------|
| DEV / HML | Serverless | MANAGED | `managed` |
| PROD | Cluster | EXTERNAL (S3) | `external` |

Override in `databricks.yml` for PROD target:
```yaml
targets:
  prod:
    resources:
      jobs:
        workflow_dm_export_gold:
          tasks:
            - task_key: export_gold
              python_wheel_task:
                parameters:
                  - "--storage-mode"
                  - "external"
                  - "--lakehouse-bucket"
                  - ${var.ingestion_s3_bucket}
```

## Databricks bundle setup

1. Add to `databricks.yml` include:
```yaml
include:
  - resources/workflows/workflow_dm_export_gold.yml
```

2. Add artifact to `databricks.yml` artifacts:
```yaml
artifacts:
  dm_export_gold:
    type: whl
    path: ./src/batch/dm_export_gold
```

3. Add library reference to workflow task in `workflow_dm_export_gold.yml`:
```yaml
libraries:
  - whl: //dm_export_gold
```
