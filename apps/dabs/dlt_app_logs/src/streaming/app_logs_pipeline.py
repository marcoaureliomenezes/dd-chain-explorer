# Databricks notebook source
# MAGIC %md
# MAGIC # App Logs DLT Pipeline — Bronze + Silver + Gold
# MAGIC
# MAGIC Pipeline dedicado ao processamento dos logs das aplicações Python on-chain.
# MAGIC Pós-migração captura → VPS/Fluent Bit.
# MAGIC
# MAGIC ## Bronze — `b_app_logs`
# MAGIC Auto Loader (cloudFiles) lê NDJSON entregue pelo Fluent Bit no S3:
# MAGIC - `b_app_logs_data` ← Fluent Bit `raw/app_logs/`
# MAGIC
# MAGIC ## Silver — `s_logs`
# MAGIC Lê da bronze via `dlt.read_stream()` (interno ao pipeline):
# MAGIC - `s_logs.logs_streaming`  ← logs dos jobs de streaming (MINED_BLOCKS_WATCHER, etc.)
# MAGIC - `s_logs.logs_batch`      ← logs dos jobs batch (CONTRACT_TRANSACTIONS_CRAWLER)
# MAGIC
# MAGIC ## Gold — `g_api_keys` (Materialized Views)
# MAGIC - `g_api_keys.etherscan_consumption`   ← consumo de API keys Etherscan por janela de tempo
# MAGIC - `g_api_keys.web3_keys_consumption`   ← consumo de API keys Web3 (Infura/Alchemy) por janela

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# ── Configuration ─────────────────────────────────────────────────────────────

INGESTION_BUCKET = spark.conf.get("ingestion.s3.bucket", "dm-chain-explorer-dev-ingestion")
S3_RAW_BASE = f"s3://{INGESTION_BUCKET}/raw"


def _configured_logger_names(key: str, default_csv: str) -> list[str]:
    """Comma-separated `logger` field values read from pipeline configuration.

    dd-chain-capture (the S3 producer) is a separate, external project — its
    logger names are not this repo's source of truth and may drift. Rather
    than hardcode a fixed producer list here, the split between streaming and
    batch loggers is configurable per target via the `streaming_logger_names`/
    `batch_logger_names` bundle variables (see databricks.yml); the defaults
    below are the pre-retirement producer names, kept only as a documented
    starting point until real Fluent-Bit traffic confirms the current ones.
    """
    raw = spark.conf.get(key, default_csv)
    return [name.strip() for name in raw.split(",") if name.strip()]


# APP_NAME constants — valor do campo `logger` nas mensagens de log
STREAMING_APP_NAMES = _configured_logger_names(
    "streaming_logger_names",
    "MINED_BLOCKS_EVENTS,ORPHAN_BLOCKS_CRAWLER,BLOCK_DATA_CRAWLER,RAW_TXS_CRAWLER,TRANSACTION_INPUT_DECODER",
)
BATCH_APP_NAMES = _configured_logger_names(
    "batch_logger_names",
    "CONTRACT_TRANSACTIONS_CRAWLER",
)

# COMMAND ----------


# ── Auto Loader Helper ─────────────────────────────────────────────────────────


def _auto_loader_fluentbit(stream_name: str):
    """
    Auto Loader reader for Fluent Bit NDJSON log files in S3.

    Each line is a JSON object emitted by the Python structured logger:
    timestamp (epoch ms), logger, level, filename, function_name, message.

    Constraint: Fluent Bit docker config MUST preserve the original JSON
    fields (use passthrough/nest filter — no extra envelope wrapping).
    """
    path = f"{S3_RAW_BASE}/{stream_name}/"
    schema = (
        "timestamp LONG, logger STRING, level STRING, filename STRING, function_name STRING, message STRING"
    )
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"s3://{INGESTION_BUCKET}/checkpoints/schemas/{stream_name}_v2")
        .schema(schema)
        .load(path)
    )


# ════════════════════════════════════════════════════════════════════════════
# BRONZE LAYER
# ════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze — `b_app_logs_data`
# MAGIC
# MAGIC Auto Loader lê NDJSON entregue pelo Fluent Bit (VPS sidecar → S3).
# MAGIC Cada linha JSON contém: `timestamp`, `logger`, `level`, `filename`,
# MAGIC `function_name`, `message`.

# COMMAND ----------


@dlt.table(
    name="b_app_logs_data",
    comment="Bronze: logs das aplicações on-chain via Fluent Bit → S3 (NDJSON)",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def bronze_app_logs_data():
    return _auto_loader_fluentbit("app_logs").withColumn("_ingested_at", F.current_timestamp())


# ════════════════════════════════════════════════════════════════════════════
# SILVER LAYER
# ════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver 1 — Streaming App Logs → `s_logs.logs_streaming`
# MAGIC
# MAGIC Logs dos jobs de streaming: `MINED_BLOCKS_WATCHER`, `ORPHAN_BLOCKS_WATCHER`,
# MAGIC `BLOCK_DATA_CRAWLER`, `MINED_TXS_CRAWLER`, `TXS_INPUT_DECODER`.

# COMMAND ----------


@dlt.table(
    name="s_logs.logs_streaming",
    comment="Silver: logs das aplicações de streaming on-chain",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_or_drop("valid_level", "level IS NOT NULL")
@dlt.expect_or_drop("valid_message", "message IS NOT NULL")
def silver_logs_streaming():
    df = dlt.read_stream("b_app_logs_data")
    return df.select(
        F.col("timestamp").alias("event_ts_epoch"),
        F.to_timestamp(F.col("timestamp")).alias("event_time"),
        F.col("logger"),
        F.col("level"),
        F.col("filename"),
        F.col("function_name"),
        F.col("message"),
        F.col("_ingested_at"),
    ).filter(F.col("logger").isin(STREAMING_APP_NAMES))


# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver 2 — Batch App Logs → `s_logs.logs_batch`
# MAGIC
# MAGIC Logs dos jobs batch: `CONTRACT_TRANSACTIONS_CRAWLER`.

# COMMAND ----------


@dlt.table(
    name="s_logs.logs_batch",
    comment="Silver: logs das aplicações batch on-chain",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_or_drop("valid_level", "level IS NOT NULL")
@dlt.expect_or_drop("valid_message", "message IS NOT NULL")
def silver_logs_batch():
    df = dlt.read_stream("b_app_logs_data")
    return df.select(
        F.col("timestamp").alias("event_ts_epoch"),
        F.to_timestamp(F.col("timestamp")).alias("event_time"),
        F.col("logger"),
        F.col("level"),
        F.col("filename"),
        F.col("function_name"),
        F.col("message"),
        F.col("_ingested_at"),
    ).filter(F.col("logger").isin(BATCH_APP_NAMES))


# ════════════════════════════════════════════════════════════════════════════
# GOLD — Materialized Views de consumo de API keys
# ════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 1 — Etherscan API Key Consumption → `g_api_keys.etherscan_consumption`
# MAGIC
# MAGIC Agrega chamadas à API Etherscan por chave, janelas de 1h / 12h / 24h / 48h.
# MAGIC Fonte: mensagens com padrão `etherscan;api_call;api_key_name:{name};action:{act};...`

# COMMAND ----------


@dlt.table(
    name="g_api_keys.etherscan_consumption",
    comment="Gold MV: consumo de API keys Etherscan por janela de tempo (1h/12h/24h/48h)",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_etherscan_consumption():
    """
    Lê dos silver logs_streaming + logs_batch, filtra mensagens de chamada
    Etherscan e agrega por api_key_name com contadores por janela de tempo.

    Padrão de log (emitido por EtherscanClient._track_call e MultiKeyEtherscanClient._track_call):
        etherscan;api_call;api_key_name:{name};action:{action};status:{status};request_count:{n}

    Uma linha por chave com contadores de requisições OK e com erro separados.
    Janelas: 1h, 2h, 12h, 24h, 48h (relativas ao _ingested_at da mensagem).
    """
    df = (
        dlt.read("s_logs.logs_streaming")
        .unionByName(dlt.read("s_logs.logs_batch"))
        .filter(F.col("message").contains("etherscan;api_call;"))
        .withColumn(
            "api_key_name",
            # [^;]+ para capturar só até o próximo ';'
            F.regexp_extract(F.col("message"), r"api_key_name:([^;]+)", 1),
        )
        .withColumn(
            "action",
            F.regexp_extract(F.col("message"), r"action:([^;]+)", 1),
        )
        .withColumn(
            "call_status",
            F.regexp_extract(F.col("message"), r"status:([^;]+)", 1),
        )
        .filter(F.col("api_key_name") != "")
    )

    return df.groupBy("api_key_name").agg(
        F.count("*").alias("calls_total"),
        F.count(F.when(F.col("call_status") == "ok", 1)).alias("calls_ok_total"),
        F.count(F.when(F.col("call_status") != "ok", 1)).alias("calls_error_total"),
        # ── janelas de tempo (baseadas no _ingested_at da mensagem) ──
        F.count(F.when(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 1 HOUR"), 1)).alias(
            "calls_1h"
        ),
        F.count(F.when(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 2 HOURS"), 1)).alias(
            "calls_2h"
        ),
        F.count(F.when(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 12 HOURS"), 1)).alias(
            "calls_12h"
        ),
        F.count(F.when(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 24 HOURS"), 1)).alias(
            "calls_24h"
        ),
        F.count(F.when(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 48 HOURS"), 1)).alias(
            "calls_48h"
        ),
        F.max("_ingested_at").alias("last_call_at"),
        F.current_timestamp().alias("computed_at"),
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 2 — Web3 API Key Consumption → `g_api_keys.web3_keys_consumption`
# MAGIC
# MAGIC Agrega chamadas a provedores Web3 (Infura/Alchemy) por chave, janelas de 1h / 12h / 24h / 48h.
# MAGIC Fonte: mensagens com padrão `API_request;{api_key_name}`, shippadas via Fluent-Bit
# MAGIC NDJSON pelo projeto externo dd-chain-capture (ver `[[capture-layer]]`) para
# MAGIC `raw/app_logs/` no bucket S3 de raw data.

# COMMAND ----------


@dlt.table(
    name="g_api_keys.web3_keys_consumption",
    comment="Gold MV: consumo de API keys Web3 (Alchemy/Infura) por janela de tempo (1h/2h/12h/24h/48h)",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_web3_keys_consumption():
    """
    Lê dos silver logs_streaming + logs_batch, filtra mensagens de chamada
    Web3 e agrega por api_key_name + vendor com contadores por janela de tempo.

    Padrões de log shippados via Fluent-Bit NDJSON pelo projeto externo
    dd-chain-capture (o cliente Web3 que os emite roda naquele repositório,
    fora do escopo deste código — ver `[[capture-layer]]`):
        OK:        API_request;{api_key_name}
        Error:     API_request;{api_key_name};Error:{msg}      (nível ERROR)
        HTTPError: API_request;{api_key_name};HTTPError:{msg}  (nível ERROR)

    A regex usa [^;]+ para extrair apenas o nome da chave, ignorando sufixos
    de erro. O vendor (alchemy/infura) é derivado do nome da chave SSM.
    Uma linha por combinação api_key_name × vendor.
    """
    df = (
        dlt.read("s_logs.logs_streaming")
        .unionByName(dlt.read("s_logs.logs_batch"))
        .filter(F.col("message").contains("API_request;"))
        .withColumn(
            "api_key_name",
            # [^;]+ para capturar só o nome da chave (para antes de ;Error: etc.)
            F.regexp_extract(F.col("message"), r"API_request;([^;]+)", 1),
        )
        .withColumn(
            "vendor",
            F.when(F.lower(F.col("api_key_name")).contains("alchemy"), F.lit("alchemy"))
            .when(F.lower(F.col("api_key_name")).contains("infura"), F.lit("infura"))
            .otherwise(F.lit("unknown")),
        )
        .withColumn(
            "call_status",
            F.when(F.col("message").contains(";Error:"), F.lit("error"))
            .when(F.col("message").contains(";HTTPError:"), F.lit("http_error"))
            .otherwise(F.lit("ok")),
        )
        .filter(F.col("api_key_name") != "")
    )

    return df.groupBy("api_key_name", "vendor").agg(
        F.count("*").alias("calls_total"),
        F.count(F.when(F.col("call_status") == "ok", 1)).alias("calls_ok_total"),
        F.count(F.when(F.col("call_status") != "ok", 1)).alias("calls_error_total"),
        # ── janelas de tempo (baseadas no _ingested_at da mensagem) ──
        F.count(F.when(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 1 HOUR"), 1)).alias(
            "calls_1h"
        ),
        F.count(F.when(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 2 HOURS"), 1)).alias(
            "calls_2h"
        ),
        F.count(F.when(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 12 HOURS"), 1)).alias(
            "calls_12h"
        ),
        F.count(F.when(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 24 HOURS"), 1)).alias(
            "calls_24h"
        ),
        F.count(F.when(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 48 HOURS"), 1)).alias(
            "calls_48h"
        ),
        F.max("_ingested_at").alias("last_call_at"),
        F.current_timestamp().alias("computed_at"),
    )
