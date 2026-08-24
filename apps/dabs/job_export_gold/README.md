# Export Gold Job

Autonomous Databricks Asset Bundle component for exporting Gold layer data to S3.

## Structure

```
job_export_gold/
├── databricks.yml                    # Bundle config with dev/hml/prod targets
├── resources/workflows/
│   └── workflow_dm_export_gold.yml   # Single task: export_gold
└── src/batch/dm_export_gold/         # Python wheel with export logic
    ├── __init__.py
    ├── export_gold.py
    ├── setup.py
    └── pyproject.toml
```

## Deploy

```bash
cd job_export_gold

# Validate bundle
databricks bundle validate --target dev

# Deploy to target
databricks bundle deploy --target dev    # DEV
databricks bundle deploy --target hml    # HML (staging)
databricks bundle deploy --target prod   # PROD

# Run job manually
databricks bundle run workflow_dm_export_gold --target dev
```

## Variables

| Variable | Default | Override |
|----------|---------|----------|
| `catalog` | `dev` | Set in `databricks.yml` target |

## Job Details

- **Single task**: `export_gold` — exports Gold layer tables to S3
- **Artifact**: `dm_export_gold` wheel
- **Entry point**: `dm-export-gold-export-gold` (maps to `dm_export_gold.export_gold:main`)
- **Type**: `python_wheel_task`
- **No schedule** (manual trigger only)

## Entry Point

The job runs the entry point defined in `src/batch/dm_export_gold/pyproject.toml`:

```
dm-export-gold-export-gold = "dm_export_gold.export_gold:main"
```

Parameters passed to the task:
- `--catalog`: target Unity Catalog (dev/hml/dd_chain_explorer)
- `--storage-mode`: `managed`

## References

See `specs/domains/data-analytics/SPEC.md` for gold layer export specifications.
