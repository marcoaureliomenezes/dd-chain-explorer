# Databricks notebook source
# MAGIC %md
# MAGIC # Ethereum DLT Pipeline — Bronze + Silver + Gold
# MAGIC
# MAGIC Pipeline unificado com três camadas (pós-migração Kafka → Kinesis/SQS/Firehose).
# MAGIC
# MAGIC ## Bronze — `b_ethereum`
# MAGIC Auto Loader (cloudFiles) lê NDJSON entregue pelo Firehose no S3:
# MAGIC - `eth_mined_blocks`       ← Firehose `raw/mainnet-blocks-data/`
# MAGIC - `eth_transactions`       ← Firehose `raw/mainnet-transactions-data/`
# MAGIC - `eth_txs_input_decoded`  ← Firehose `raw/mainnet-transactions-decoded/`
# MAGIC
# MAGIC ## Silver — `s_apps`
# MAGIC Lê da bronze via `dlt.read()` (interno ao pipeline, sem dependência externa):
# MAGIC - `s_apps.eth_blocks`                  ← eth_mined_blocks
# MAGIC - `s_apps.eth_transactions_staging`     ← eth_transactions (raw, staging interno DLT)
# MAGIC - `s_apps.txs_inputs_decoded_fast`      ← eth_txs_input_decoded
# MAGIC - `s_apps.transactions_ethereum`        ← JOIN eth_transactions_staging + txs_inputs_decoded_fast + eth_blocks + eth_canonical_blocks_index
# MAGIC - `s_apps.eth_blocks_withdrawals`           ← eth_blocks.withdrawals explodido (1 linha por saque Beacon Chain)
# MAGIC - `s_apps.eth_canonical_blocks_index`       ← eth_blocks: chain_status = canonical|orphan|unconfirmed via parentHash
# MAGIC
# MAGIC ## Gold — `g_apps` / `g_network` (Materialized Views)
# MAGIC ### g_apps — App-level analytics (transactions + contracts)
# MAGIC - `g_apps.popular_contracts_ranking` ← top 100 contratos por volume de txs canônicas na última hora
# MAGIC - `g_apps.peer_to_peer_txs`          ← transações EOA→EOA canônicas (input vazio/nulo)
# MAGIC - `g_apps.ethereum_gas_consume`      ← consumo de gas por transação canônica com tipo classificado
# MAGIC - `g_apps.transactions_lambda`       ← visão unificada streaming + batch com input decodificado (valid + unconfirmed)
# MAGIC ### g_network — Network-level analytics (blocks only)
# MAGIC - `g_network.network_metrics_hourly`      ← TPS, gas médio, utilização de blocos por hora
# MAGIC - `g_network.eth_burn_hourly`             ← ETH queimado por hora via EIP-1559 (base_fee × gas_used)
# MAGIC - `g_network.validator_activity`          ← concentração de validadores (HHI) janela 24h
# MAGIC - `g_network.withdrawal_metrics`          ← fluxo de saques Beacon Chain (EIP-4895) por hora
# MAGIC - `g_network.block_production_health`     ← slots perdidos estimados e saúde de produção de blocos
# MAGIC - `g_network.chain_health_metrics`        ← taxa de orphan blocks por hora (saúde da cadeia)

# COMMAND ----------

import re

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Unity Catalog identifiers cannot be bind-parameterized in Spark SQL — validate
# the name against a strict allow-list pattern before any f-string interpolation
# (AC-24 / T-C.4: no unvalidated identifier construction).
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validated_identifier(name: str, what: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"invalid {what}: {name!r}")
    return name


# ── Configuration ─────────────────────────────────────────────────────────────
# Post-migration: all data arrives via Firehose → S3 as NDJSON (gzip).
# Auto Loader reads from the Firehose delivery prefix.

INGESTION_BUCKET = spark.conf.get("ingestion.s3.bucket", "dm-chain-explorer-dev-ingestion")
S3_RAW_BASE = f"s3://{INGESTION_BUCKET}/raw"
CATALOG = _validated_identifier(spark.conf.get("catalog", "dev"), "catalog")


# ════════════════════════════════════════════════════════════════════════════
# BRONZE LAYER
# ════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze — Auto Loader tables (Firehose → S3 NDJSON)
# MAGIC
# MAGIC Each Kinesis stream has a dedicated Firehose delivery to S3.
# MAGIC Auto Loader reads NDJSON (gzip) from the Firehose prefix.

# COMMAND ----------


def _auto_loader_json(stream_name: str):
    """Auto Loader reader for NDJSON files delivered by Firehose."""
    path = f"{S3_RAW_BASE}/{stream_name}/"
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"s3://{INGESTION_BUCKET}/checkpoints/schemas/{stream_name}")
        .option("cloudFiles.inferColumnTypes", "true")
        # Suppress Firehose year/month/day/hour path inference — use dat_ref column instead
        .option("cloudFiles.partitionColumns", "")
        .load(path)
    )


@dlt.table(
    name="eth_mined_blocks",
    comment="Bronze: block data from Firehose (mainnet-blocks-data)",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.managed": "true"},
    partition_cols=["dat_ref"],
)
def bronze_eth_mined_blocks():
    return (
        _auto_loader_json("mainnet-blocks-data")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("dat_ref", F.to_date(F.col("_ingested_at")))
    )


@dlt.table(
    name="eth_transactions",
    comment="Bronze: transaction data from Firehose (mainnet-transactions-data)",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.managed": "true"},
    partition_cols=["dat_ref"],
)
def bronze_eth_transactions():
    return (
        _auto_loader_json("mainnet-transactions-data")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("dat_ref", F.to_date(F.col("_ingested_at")))
    )


@dlt.table(
    name="eth_txs_input_decoded",
    comment="Bronze: decoded transaction inputs from Firehose (mainnet-transactions-decoded)",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.managed": "true"},
    partition_cols=["dat_ref"],
)
def bronze_eth_txs_input_decoded():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            f"s3://{INGESTION_BUCKET}/checkpoints/schemas/mainnet-transactions-decoded",
        )
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.partitionColumns", "")
        .option(
            "cloudFiles.schemaHints",
            "tx_hash string, contract_address string, method string, parms string, "
            "method_id string, decode_type string, decode_source string, decode_confidence string",
        )
        .load(f"{S3_RAW_BASE}/mainnet-transactions-decoded/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("dat_ref", F.to_date(F.col("_ingested_at")))
    )


# ════════════════════════════════════════════════════════════════════════════
# SILVER LAYER
# Lê da bronze via dlt.read() — referência interna ao pipeline.
# Tabelas escritas em schemas s_apps / s_logs via name="schema.table".
# ════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — Blocks Data → `s_apps.eth_blocks`
# MAGIC
# MAGIC Reads from bronze `eth_mined_blocks` (JSON fields from Firehose).
# MAGIC Field names match the JSON keys produced by Job 3.

# COMMAND ----------


@dlt.table(
    name="s_apps.eth_blocks",
    comment="Silver: dados completos dos blocos Ethereum (header + withdrawals)",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_or_drop("valid_block_number", "block_number IS NOT NULL")
@dlt.expect_or_drop("valid_hash", "block_hash IS NOT NULL")
def silver_eth_blocks():
    df = dlt.read_stream("eth_mined_blocks")
    return df.select(
        F.col("number").alias("block_number"),
        F.col("hash").alias("block_hash"),
        F.col("parentHash").alias("parent_hash"),
        F.to_timestamp(F.col("timestamp")).alias("block_time"),
        F.col("timestamp").alias("block_timestamp"),
        F.col("miner"),
        F.col("difficulty"),
        F.col("totalDifficulty").alias("total_difficulty"),
        F.col("nonce"),
        F.col("size"),
        F.col("baseFeePerGas").alias("base_fee_per_gas"),
        F.col("gasLimit").alias("gas_limit"),
        F.col("gasUsed").alias("gas_used"),
        F.col("logsBloom").alias("logs_bloom"),
        F.col("extraData").alias("extra_data"),
        F.col("transactionsRoot").alias("transactions_root"),
        F.col("stateRoot").alias("state_root"),
        F.size(F.col("transactions")).alias("transaction_count"),
        F.col("transactions"),
        F.col("withdrawals"),
        F.col("_ingested_at"),
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — Transactions raw → `s_apps.eth_transactions_staging`
# MAGIC
# MAGIC Reads from bronze `eth_transactions` (JSON fields from Firehose).
# MAGIC **Tabela de staging interno DLT** — não consultar diretamente.
# MAGIC Use `s_apps.transactions_ethereum` para análises.
# MAGIC Sem campos decoded (method/parms/decode_type) — esses ficam em `txs_inputs_decoded_fast`.

# COMMAND ----------


@dlt.table(
    name="s_apps.eth_transactions_staging",
    comment="Silver staging: transações Ethereum raw — tabela interna DLT, não consultar diretamente. Use s_apps.transactions_ethereum.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["dat_ref"],
)
@dlt.expect_or_drop("valid_hash", "tx_hash IS NOT NULL")
@dlt.expect_or_drop("valid_block", "block_number IS NOT NULL")
# ISSUE-030 / DE-P-003: from_address promoted from expect → expect_or_drop.
# A transaction without a valid from_address is fundamentally malformed and must not
# propagate into Silver/Gold layers where it would corrupt canonical-chain accounting.
@dlt.expect_or_drop("valid_from_address", "from_address IS NOT NULL AND from_address RLIKE '^0x[a-fA-F0-9]{40}$'")  # fmt: skip
# to_address intentionally remains expect (not expect_or_drop): NULL is valid for
# contract-creation transactions (EIP-155). Dropping these rows would silently omit
# all contract deploys from Silver, breaking g_apps.contract_deploy_metrics_hourly.
@dlt.expect("valid_to_address", "to_address IS NULL OR to_address RLIKE '^0x[a-fA-F0-9]{40}$'")
def silver_eth_transactions_staging():
    df = dlt.read_stream("eth_transactions")
    return df.select(
        F.col("hash").alias("tx_hash"),
        F.col("blockNumber").alias("block_number"),
        F.col("blockHash").alias("block_hash"),
        F.col("transactionIndex").alias("transaction_index"),
        F.col("`from`").alias("from_address"),
        F.col("to").alias("to_address"),
        F.col("value"),
        F.col("input"),
        F.col("gas"),
        F.col("gasPrice").alias("gas_price"),
        F.col("nonce"),
        F.col("v"),
        F.col("r"),
        F.col("s"),
        F.col("type").alias("tx_type"),
        F.col("accessList").alias("access_list"),
        F.col("_ingested_at"),
        F.to_date(F.col("_ingested_at")).alias("dat_ref"),
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — Decoded inputs → `s_apps.txs_inputs_decoded_fast`
# MAGIC
# MAGIC Reads from bronze `eth_txs_input_decoded` (JSON fields from Firehose).

# COMMAND ----------


@dlt.table(
    name="s_apps.txs_inputs_decoded_fast",
    comment="Silver: inputs de transações decodificados — Kinesis stream mainnet-transactions-decoded",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_or_drop("valid_tx_hash", "tx_hash IS NOT NULL")
def silver_txs_inputs_decoded_fast():
    df = dlt.read_stream("eth_txs_input_decoded")
    return df.select(
        F.col("tx_hash"),
        F.col("contract_address"),
        F.col("method"),
        F.col("parms"),
        F.col("method_id"),
        F.col("decode_type"),
        F.col("decode_source"),
        F.col("decode_confidence"),
        F.col("_ingested_at"),
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver 6 — Enriched transactions → `s_apps.transactions_ethereum`
# MAGIC
# MAGIC Stream-Stream JOIN: `transactions_fast` ← `txs_inputs_decoded_fast`
# MAGIC Stream-Static JOIN: resultado ← `eth_blocks` (para tx_timestamp)
# MAGIC
# MAGIC Padrão: Stream-Static JOIN (DLT não suporta Stream-Stream JOIN).
# MAGIC - txs_inputs_decoded_fast lido como snapshot estático (dlt.read)
# MAGIC - eth_blocks lido como batch snapshot (dlt.read) para join stream-static
# MAGIC - Sem necessidade de watermark ou condição de intervalo

# COMMAND ----------


@dlt.table(
    name="s_apps.transactions_ethereum",
    comment=(
        "Silver: transações Ethereum enriquecidas — JOIN streaming com decoded inputs, blocks e eth_canonical_blocks_index. "
        "tx_status = 'valid' | 'orphaned' | 'unconfirmed'. "
        "Gold tables devem filtrar por tx_status = 'valid'."
    ),
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["dat_ref"],
)
@dlt.expect_or_drop("valid_hash", "tx_hash IS NOT NULL")
@dlt.expect_or_drop("valid_block", "block_number IS NOT NULL")
# Consistent with eth_transactions_staging: from_address must be valid (expect_or_drop).
# Records reaching this table already passed the staging filter, but the expectation
# is preserved here for DLT lineage tracking and explicit documentation of the contract.
@dlt.expect_or_drop("valid_from_address", "from_address IS NOT NULL AND from_address RLIKE '^0x[a-fA-F0-9]{40}$'")  # fmt: skip
# to_address remains expect: NULL is valid for contract-creation transactions.
@dlt.expect("valid_to_address", "to_address IS NULL OR to_address RLIKE '^0x[a-fA-F0-9]{40}$'")
def silver_transactions_ethereum():
    # ── Stream: transações raw ───────────────────────────────────────────────
    # dlt.read_stream → Streaming Table (consume novos registros incrementalmente)
    txs = dlt.read_stream("s_apps.eth_transactions_staging")

    # ── Static: inputs decodificados (snapshot) ──────────────────────────────
    # DLT NÃO suporta Stream-Stream JOIN. Decoded inputs são lidos como
    # snapshot estático (dlt.read) — será atualizado a cada update do pipeline.
    decoded = dlt.read("s_apps.txs_inputs_decoded_fast").select(
        F.col("tx_hash").alias("d_tx_hash"),
        F.col("contract_address"),
        F.col("method"),
        F.col("parms"),
        F.col("method_id"),
        F.col("decode_type"),
        F.col("decode_source"),
        F.col("decode_confidence"),
    )

    # ── Stream-Static LEFT JOIN por tx_hash ──────────────────────────────────
    joined = txs.join(decoded, txs["tx_hash"] == decoded["d_tx_hash"], "left")

    # ── Stream-Static JOIN: enriquece com dados do bloco (timestamp + gas) ───
    # dlt.read() retorna snapshot do bloco — suportado como stream-static join.
    blocks = dlt.read("s_apps.eth_blocks").select(
        F.col("block_number").alias("b_block_number"),
        F.from_unixtime("block_timestamp", "yyyy-MM-dd HH:mm:ss").alias("tx_timestamp"),
        F.col("gas_limit").alias("block_gas_limit"),
        F.col("gas_used").alias("block_gas_used"),
        F.col("base_fee_per_gas"),
    )

    # ── Stream-Static JOIN: status de canonicidade do bloco ─────────────────
    # eth_canonical_blocks_index classifica cada bloco via parentHash:
    # chain_status 'canonical' → tx_status 'valid'
    # chain_status 'orphan'    → tx_status 'orphaned'
    # chain_status 'unconfirmed' / NULL (bloco novo, índice ainda sem cobertura)
    #                          → tx_status 'unconfirmed'
    canonical = dlt.read("s_apps.eth_canonical_blocks_index").select(
        F.col("block_number").alias("c_block_number"),
        F.col("block_hash").alias("c_block_hash"),
        F.col("chain_status"),
    )

    return (
        joined.join(blocks, joined["block_number"] == blocks["b_block_number"], "left")
        .join(
            canonical,
            (joined["block_number"] == canonical["c_block_number"])
            & (joined["block_hash"] == canonical["c_block_hash"]),
            "left",
        )
        .select(
            # Campos da transação
            F.col("tx_hash"),
            F.col("block_number"),
            F.col("block_hash"),
            F.col("transaction_index"),
            F.col("from_address"),
            F.col("to_address"),
            F.col("value"),
            F.col("input"),
            F.col("gas"),
            F.col("gas_price"),
            F.col("nonce"),
            F.col("tx_type"),
            # Classificação semântica do tipo de transação (P02)
            F.when(F.col("to_address").isNull(), F.lit("contract_deploy"))
            .when(
                F.col("input").isNull() | (F.col("input") == F.lit("0x")) | (F.col("input") == F.lit("")),
                F.lit("peer_to_peer"),
            )
            .otherwise(F.lit("contract_interaction"))
            .alias("tx_type_semantic"),
            # Campos do bloco
            F.col("tx_timestamp"),
            F.col("block_gas_limit"),
            F.col("block_gas_used"),
            F.col("base_fee_per_gas"),
            # Campos do decoder (P03 — method_id + decode_source + decode_confidence)
            F.col("contract_address"),
            F.col("method"),
            F.col("parms"),
            F.col("method_id"),
            F.col("decode_type"),
            F.col("decode_source"),
            F.col("decode_confidence"),
            # Status de canonicidade derivado do bloco
            F.when(F.col("chain_status") == "canonical", F.lit("valid"))
            .when(F.col("chain_status") == "orphan", F.lit("orphaned"))
            .otherwise(F.lit("unconfirmed"))
            .alias("tx_status"),
            # Placeholders para enriquecimento batch (P04 — preenchidos via transactions_batch)
            F.lit(None).cast("long").alias("receipt_status"),
            F.lit(None).cast("long").alias("is_error"),
            F.col("_ingested_at"),
            F.to_date(F.col("_ingested_at")).alias("dat_ref"),
        )
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — Beacon Chain withdrawals → `s_apps.eth_blocks_withdrawals`
# MAGIC
# MAGIC Explode do campo `withdrawals` de `eth_blocks`: cada validator que sacou
# MAGIC ETH excedente (acima de 32 ETH) gera uma linha. Introduzido no EIP-4895
# MAGIC (Shanghai/Capella, Abril 2023). Máximo de 16 saques por bloco.

# COMMAND ----------


@dlt.table(
    name="s_apps.eth_blocks_withdrawals",
    comment=(
        "Silver: saques ETH da Beacon Chain (EIP-4895) — uma linha por withdrawal por bloco. "
        "Campo amount em Gwei (÷1e9 = ETH)."
    ),
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
def silver_eth_blocks_withdrawals():
    return (
        dlt.read_stream("s_apps.eth_blocks")
        .filter(F.col("withdrawals").isNotNull() & (F.size(F.col("withdrawals")) > 0))
        .select(
            F.col("block_number"),
            F.from_unixtime("block_timestamp", "yyyy-MM-dd HH:mm:ss").alias("block_timestamp"),
            F.col("miner"),
            F.col("_ingested_at"),
            F.explode("withdrawals").alias("wd"),
        )
        .select(
            F.col("block_number"),
            F.col("block_timestamp"),
            F.col("miner"),
            F.col("wd.index").alias("withdrawal_index"),
            F.col("wd.validatorIndex").alias("validator_index"),
            F.col("wd.address").alias("withdrawal_address"),
            F.col("wd.amount").alias("amount_gwei"),
            (F.col("wd.amount") / F.lit(1e9)).alias("amount_eth"),
            F.col("_ingested_at"),
        )
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — Canonical Blocks Index → `s_apps.eth_canonical_blocks_index`
# MAGIC
# MAGIC Tabela batch (não-streaming) que classifica cada bloco da Silver `eth_blocks`
# MAGIC usando a cadeia de `parent_hash` como prova forense de canonicidade:
# MAGIC - **`canonical`** — `block_hash` é referenciado como `parent_hash` por algum bloco filho
# MAGIC   dentro da janela de análise, OU bloco está além da janela (presume-se canônico por ser antigo).
# MAGIC - **`unconfirmed`** — bloco dentro da janela de finalização Casper FFG (últimos 64 blocos).
# MAGIC - **`orphan`** — dentro da janela de análise (últimos 1.000 blocos): hash não referenciado
# MAGIC   por nenhum filho (fork perdedor).
# MAGIC
# MAGIC **ISSUE-003 / DE-P-001 — Bounded rolling window (1.000 blocos):**
# MAGIC A implementação anterior realizava scan total da tabela em `parent_refs` e cross-join
# MAGIC com todos os blocos, produzindo complexidade O(N²) e warnings de scan no DLT event log.
# MAGIC A nova implementação restringe o trabalho aos 1.000 blocos mais recentes:
# MAGIC - Blocos fora da janela são marcados como `canonical` sem verificação de parent_hash.
# MAGIC   Trade-off aceito: orphans com > 1.000 blocos de idade são extremamente improváveis
# MAGIC   (o maior reorg registrado na mainnet Ethereum foi de 7 blocos; 1.000 blocos ≈ 3,3 h
# MAGIC   de buffer é mais do que suficiente).
# MAGIC - O scan de `parent_refs` é restrito à mesma janela de 1.000 blocos, evitando o
# MAGIC   full-table scan.
# MAGIC
# MAGIC Recomputado integralmente a cada update do pipeline (batch MV).

# COMMAND ----------

# Rolling window size for canonical block analysis (ISSUE-003 / DE-P-001).
# 1,000 blocks ≈ 3.3 hours at 12 s/block. This comfortably exceeds the maximum observed
# mainnet reorg depth (~7 blocks). Blocks older than this window are presumed canonical.
_CANONICAL_WINDOW_BLOCKS = 1_000


@dlt.table(
    name="s_apps.eth_canonical_blocks_index",
    comment=(
        "Silver: índice de status de blocos derivado de parentHash — rolling window de 1.000 blocos. "
        "chain_status = 'canonical' | 'orphan' | 'unconfirmed' (últimos 64 blocos). "
        "Blocos fora da janela de 1.000 são marcados 'canonical' por presunção de finalidade. "
        "Recomputado integralmente a cada execução do pipeline."
    ),
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
def silver_eth_canonical_blocks_index():
    """
    Bounded rolling window canonical block classifier (ISSUE-003 / DE-P-001).

    Algorithm:
      1. Determine max_block from eth_blocks (single aggregate, no scan).
      2. Compute window_floor = max_block - CANONICAL_WINDOW_BLOCKS.
      3. Blocks with block_number <= window_floor: return as 'canonical' directly
         (no parent_hash lookup required — older orphans are accepted risk, documented above).
      4. For the rolling window [window_floor+1 .. max_block]:
         a. Build parent_refs from that window only (O(window) not O(N)).
         b. Classify each block as 'unconfirmed' (last 64), 'canonical', or 'orphan'.
      5. UNION ALL both sets.

    Output schema is unchanged: block_number, block_hash, chain_status.
    All downstream Gold MVs that filter chain_status IN ('canonical','orphan') continue
    to work correctly.
    """
    return spark.sql(f"""
        WITH max_block AS (
          SELECT MAX(block_number) AS max_num
          FROM {CATALOG}.s_apps.eth_blocks
        ),
        -- Blocks outside the rolling window: presumed canonical, no orphan detection needed.
        -- Trade-off: orphans older than {_CANONICAL_WINDOW_BLOCKS} blocks are not detected.
        -- This is an accepted risk — the largest observed mainnet reorg is ~7 blocks.
        outside_window AS (
          SELECT
              bf.block_number,
              bf.block_hash,
              'canonical' AS chain_status
          FROM {CATALOG}.s_apps.eth_blocks bf
          CROSS JOIN max_block mb
          WHERE bf.block_number <= (mb.max_num - {_CANONICAL_WINDOW_BLOCKS})
        ),
        -- Restrict orphan detection to the rolling window only (bounded scan).
        window_blocks AS (
          SELECT bf.block_number, bf.block_hash, bf.parent_hash
          FROM {CATALOG}.s_apps.eth_blocks bf
          CROSS JOIN max_block mb
          WHERE bf.block_number > (mb.max_num - {_CANONICAL_WINDOW_BLOCKS})
        ),
        -- Build parent reference set from the window only — O(window) not O(N).
        parent_refs AS (
          SELECT DISTINCT parent_hash AS referenced_hash
          FROM window_blocks
        ),
        -- Classify blocks inside the rolling window.
        inside_window AS (
          SELECT DISTINCT
              wb.block_number,
              wb.block_hash,
              CASE
                WHEN wb.block_number > (mb.max_num - 64) THEN 'unconfirmed'
                WHEN pr.referenced_hash IS NOT NULL       THEN 'canonical'
                ELSE                                           'orphan'
              END AS chain_status
          FROM window_blocks wb
          CROSS JOIN max_block mb
          LEFT JOIN parent_refs pr ON wb.block_hash = pr.referenced_hash
        )
        SELECT block_number, block_hash, chain_status FROM outside_window
        UNION ALL
        SELECT block_number, block_hash, chain_status FROM inside_window
    """)


# ════════════════════════════════════════════════════════════════════════════
# GOLD — Materialized Views (atualizadas a cada execução do pipeline)
# ════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 1 — Popular Contracts Ranking → `s_apps.popular_contracts_ranking`
# MAGIC
# MAGIC Top 100 contratos Ethereum com mais transações na última hora.
# MAGIC Serve como fonte para o Job Batch que captura transações históricas.

# COMMAND ----------


@dlt.table(
    name="g_apps.popular_contracts_ranking",
    comment="Gold MV: top 100 contratos mais populares por volume de transações",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_popular_contracts_ranking():
    """
    Lê de transactions_ethereum (com tx_status) e calcula ranking dos 100 contratos
    que mais receberam transações canônicas válidas na última hora.
    Filtra tx_status = 'valid' para excluir transações de blocos orphan.
    """
    df = dlt.read("s_apps.transactions_ethereum")

    df = df.filter(F.col("to_address").isNotNull())
    df = df.filter(F.col("tx_status") == "valid")
    df = df.filter(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 1 HOUR"))

    return (
        df.groupBy("to_address")
        .agg(
            F.count("*").alias("tx_count"),
            F.max("_ingested_at").alias("last_seen"),
            F.min("_ingested_at").alias("first_seen"),
            F.countDistinct("from_address").alias("unique_senders"),
        )
        .orderBy(F.desc("tx_count"))
        .limit(100)
        .select(
            F.col("to_address").alias("contract_address"),
            "tx_count",
            "unique_senders",
            "first_seen",
            "last_seen",
            F.current_timestamp().alias("computed_at"),
        )
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 2 — Peer-to-Peer transactions → `s_apps.peer_to_peer_txs`
# MAGIC
# MAGIC Transações ETH diretas entre endereços EOA (Externally Owned Accounts).
# MAGIC Identificadas por campo `input` vazio/nulo/"0x" e `to_address` não nulo.

# COMMAND ----------


@dlt.table(
    name="g_apps.peer_to_peer_txs",
    comment="Gold MV: transferências ETH diretas entre endereços EOA (input vazio ou nulo)",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_peer_to_peer_txs():
    return (
        dlt.read("s_apps.transactions_ethereum")
        .filter(F.col("tx_status") == "valid")
        .filter(
            F.col("to_address").isNotNull()
            & (F.col("input").isNull() | (F.col("input") == "") | (F.col("input") == "0x"))
        )
        .select(
            F.col("tx_hash"),
            F.col("block_number"),
            F.col("from_address"),
            F.col("to_address"),
            F.col("value"),
            F.col("gas"),
            F.col("gas_price"),
            F.col("tx_timestamp"),
            F.col("base_fee_per_gas"),
            F.col("_ingested_at"),
        )
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 3 — Gas consumption → `s_apps.ethereum_gas_consume`
# MAGIC
# MAGIC Materialized view com consumo de gas por transação e tipo classificado:
# MAGIC - `peer_to_peer`        → input nulo/vazio/"0x" e to_address não nulo
# MAGIC - `contract_deploy`     → to_address nulo e input não vazio (cria contrato)
# MAGIC - `contract_interaction`→ demais (chama função de contrato)

# COMMAND ----------


@dlt.table(
    name="g_apps.ethereum_gas_consume",
    comment="Gold MV: consumo de gas por transação com classificação de tipo (peer_to_peer | contract_interaction | contract_deploy)",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_ethereum_gas_consume():
    txs = dlt.read("s_apps.transactions_ethereum").filter(F.col("tx_status") == "valid")

    # Porcentagem de gas que esta tx consumiu no bloco.
    # Nota: `gas` aqui é o gas limit da tx (streaming não carrega
    # gas_used do receipt). block_gas_used é o total efetivo do bloco.
    gas_pct_of_block = F.when(
        F.col("block_gas_used").isNotNull() & (F.col("block_gas_used") > 0),
        F.round(F.col("gas").cast("double") / F.col("block_gas_used") * 100, 4),
    ).otherwise(F.lit(None).cast("double"))

    return txs.select(
        F.col("block_number"),
        F.col("tx_hash"),
        F.col("from_address"),
        F.col("to_address"),
        F.col("value"),
        F.col("gas_price"),
        F.col("gas").alias("gas_limit"),
        F.col("tx_timestamp"),
        F.col("tx_type_semantic"),
        F.col("block_gas_limit"),
        F.col("block_gas_used"),
        gas_pct_of_block.alias("gas_pct_of_block"),
        F.col("base_fee_per_gas"),
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 4 — Transactions Lambda → `s_apps.transactions_lambda`
# MAGIC
# MAGIC Materialized view que combina dados de streaming (`transactions_ethereum`)
# MAGIC com dados batch (`popular_contracts_txs`) para criar uma visão unificada
# MAGIC (arquitetura Lambda) das transações dos contratos populares, sempre com
# MAGIC o input decodificado mais preciso disponível.

# COMMAND ----------


@dlt.table(
    name="g_apps.transactions_lambda",
    comment="Gold MV: visão Lambda de transações de contratos populares com input decodificado",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_transactions_lambda():
    """
    Transações dos contratos mais populares com input decodificado (streaming only).
    Filtra transactions_ethereum pelos contratos em popular_contracts_ranking.
    Inclui tx_status 'valid' e 'unconfirmed' — exclui apenas 'orphaned'.
    """
    df_ranking = dlt.read("g_apps.popular_contracts_ranking").select("contract_address")
    df_stream = dlt.read("s_apps.transactions_ethereum").filter(
        F.col("tx_status").isin("valid", "unconfirmed")
    )

    return df_stream.join(df_ranking, df_stream.to_address == df_ranking.contract_address, "inner").select(
        F.col("tx_hash"),
        F.col("block_number"),
        F.col("from_address"),
        F.col("to_address").alias("contract_address"),
        F.col("value"),
        F.col("gas"),
        F.col("gas_price"),
        F.col("input"),
        F.col("method"),
        F.col("parms"),
        F.col("method_id"),
        F.col("decode_type"),
        F.col("decode_source"),
        F.col("decode_confidence"),
        F.col("tx_type_semantic"),
        F.col("tx_status"),
        F.col("tx_timestamp").alias("event_time"),
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 5 — Network Metrics Hourly → `g_network.network_metrics_hourly`
# MAGIC
# MAGIC Métricas agredadas da rede Ethereum por hora: TPS médio, preço de gas,
# MAGIC utilização de blocos e volume de transações (TODO-P06).

# COMMAND ----------


@dlt.table(
    name="g_network.network_metrics_hourly",
    comment="Gold MV: métricas horárias da rede Ethereum — TPS, gas médio, utilização de blocos",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_network_metrics_hourly():
    """
    Agrega métricas da rede Ethereum por hora usando eth_blocks e transactions_fast.

    Métricas calculadas:
    - block_count               : blocos produzidos na hora
    - tx_count                  : total de transações
    - tps_avg                   : TPS médio (tx_count / 3600 segundos)
    - avg_gas_price_gwei        : preço médio do gas em Gwei
    - avg_block_gas_used        : gas médio usado por bloco
    - avg_block_gas_limit       : gas limit médio por bloco
    - avg_block_utilization_pct : utilização média do bloco (gas_used/gas_limit × 100)
    - avg_txs_per_block         : média de transações por bloco
    """
    blocks = dlt.read("s_apps.eth_blocks").select(
        F.date_trunc("hour", F.col("block_time")).alias("hour_bucket"),
        F.col("block_number"),
        F.col("gas_used").alias("block_gas_used"),
        F.col("gas_limit").alias("block_gas_limit"),
        F.col("transaction_count"),
    )

    txs = dlt.read("s_apps.eth_transactions_staging").select(
        F.date_trunc("hour", F.col("_ingested_at")).alias("hour_bucket"),
        F.col("tx_hash"),
        F.col("gas_price"),
    )

    blocks_agg = blocks.groupBy("hour_bucket").agg(
        F.count("block_number").alias("block_count"),
        F.sum("transaction_count").alias("tx_count_from_blocks"),
        F.avg("block_gas_used").alias("avg_block_gas_used"),
        F.avg("block_gas_limit").alias("avg_block_gas_limit"),
        F.avg(
            F.when(
                F.col("block_gas_limit") > 0,
                F.col("block_gas_used").cast("double") / F.col("block_gas_limit") * 100,
            ).otherwise(F.lit(None))
        ).alias("avg_block_utilization_pct"),
        F.avg("transaction_count").alias("avg_txs_per_block"),
    )

    txs_agg = txs.groupBy("hour_bucket").agg(
        F.count("tx_hash").alias("tx_count"),
        F.avg(F.col("gas_price").cast("double") / 1e9).alias("avg_gas_price_gwei"),
    )

    return blocks_agg.join(txs_agg, "hour_bucket", "left").select(
        F.col("hour_bucket"),
        F.col("block_count"),
        F.coalesce(F.col("tx_count"), F.col("tx_count_from_blocks")).alias("tx_count"),
        F.round(
            F.coalesce(F.col("tx_count"), F.col("tx_count_from_blocks")).cast("double") / 3600,
            2,
        ).alias("tps_avg"),
        F.round(F.col("avg_gas_price_gwei"), 4).alias("avg_gas_price_gwei"),
        F.round(F.col("avg_block_gas_used"), 0).alias("avg_block_gas_used"),
        F.round(F.col("avg_block_gas_limit"), 0).alias("avg_block_gas_limit"),
        F.round(F.col("avg_block_utilization_pct"), 2).alias("avg_block_utilization_pct"),
        F.round(F.col("avg_txs_per_block"), 1).alias("avg_txs_per_block"),
        F.current_timestamp().alias("computed_at"),
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 5 — Gas Price Distribution Hourly → `g_apps.gas_price_distribution_hourly`
# MAGIC
# MAGIC Distribuição de `gas_price` por tipo semântico de transação por hora.
# MAGIC Percentis P25/P50/P75/P95 para detectar picos de fee e comparar comportamento
# MAGIC entre P2P, chamadas de contrato e deploys. (UC2)

# COMMAND ----------


@dlt.table(
    name="g_apps.gas_price_distribution_hourly",
    comment="Gold MV (UC2): distribuição de gas_price por tipo semântico de transação por hora",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_gas_price_distribution_hourly():
    txs = (
        dlt.read("s_apps.transactions_ethereum")
        .filter(F.col("tx_status") == "valid")
        .filter(F.col("gas_price").isNotNull())
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("_ingested_at")))
        .withColumn("gas_price_gwei", F.col("gas_price").cast("double") / F.lit(1e9))
    )

    return (
        txs.groupBy("hour_bucket", "tx_type_semantic")
        .agg(
            F.count("tx_hash").alias("tx_count"),
            F.percentile_approx("gas_price_gwei", 0.25).alias("gas_price_p25_gwei"),
            F.percentile_approx("gas_price_gwei", 0.50).alias("gas_price_p50_gwei"),
            F.percentile_approx("gas_price_gwei", 0.75).alias("gas_price_p75_gwei"),
            F.percentile_approx("gas_price_gwei", 0.95).alias("gas_price_p95_gwei"),
            F.round(F.avg("gas_price_gwei"), 4).alias("gas_price_avg_gwei"),
        )
        .select(
            F.col("hour_bucket"),
            F.col("tx_type_semantic"),
            F.col("tx_count"),
            F.col("gas_price_p25_gwei"),
            F.col("gas_price_p50_gwei"),
            F.col("gas_price_p75_gwei"),
            F.col("gas_price_p95_gwei"),
            F.col("gas_price_avg_gwei"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy("hour_bucket", "tx_type_semantic")
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 6 — P2P Transfer Metrics Hourly → `g_apps.p2p_transfer_metrics_hourly`
# MAGIC
# MAGIC Métricas horárias de transferências ETH puras (peer_to_peer):
# MAGIC volume total, senders/receivers únicos, média e máximo por tx. (UC3)

# COMMAND ----------


@dlt.table(
    name="g_apps.p2p_transfer_metrics_hourly",
    comment="Gold MV (UC3): métricas horárias de transferências ETH P2P (peer_to_peer)",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_p2p_transfer_metrics_hourly():
    txs = (
        dlt.read("s_apps.transactions_ethereum")
        .filter(F.col("tx_status") == "valid")
        .filter(F.col("tx_type_semantic") == "peer_to_peer")
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("_ingested_at")))
        .withColumn("eth_value", F.col("value").cast("double") / F.lit(1e18))
    )

    return (
        txs.groupBy("hour_bucket")
        .agg(
            F.count("tx_hash").alias("tx_count"),
            F.countDistinct("from_address").alias("unique_senders"),
            F.countDistinct("to_address").alias("unique_receivers"),
            F.round(F.sum("eth_value"), 8).alias("total_eth_transferred"),
            F.round(F.avg("eth_value"), 8).alias("avg_eth_per_tx"),
            F.round(F.max("eth_value"), 8).alias("max_eth_per_tx"),
        )
        .select(
            F.col("hour_bucket"),
            F.col("tx_count"),
            F.col("unique_senders"),
            F.col("unique_receivers"),
            F.col("total_eth_transferred"),
            F.col("avg_eth_per_tx"),
            F.col("max_eth_per_tx"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy("hour_bucket")
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 7 — Contract Method Activity → `g_apps.contract_method_activity`
# MAGIC
# MAGIC TOP 50 métodos mais chamados por contrato nas últimas 24 horas.
# MAGIC Identifica funções mais ativas, padrões de uso e anomalias de decodificação. (UC4)

# COMMAND ----------


@dlt.table(
    name="g_apps.contract_method_activity",
    comment="Gold MV (UC4): ranking de métodos chamados por contrato nas últimas 24h (TOP 50)",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_contract_method_activity():
    txs = (
        dlt.read("s_apps.transactions_ethereum")
        .filter(F.col("tx_status") == "valid")
        .filter(F.col("tx_type_semantic") == "contract_interaction")
        .filter(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 24 HOURS"))
        .filter(F.col("contract_address").isNotNull())
        .filter(F.col("method").isNotNull())
    )

    ranked = txs.groupBy("contract_address", "method", "method_id", "decode_type").agg(
        F.count("tx_hash").alias("call_count"),
        F.countDistinct("from_address").alias("unique_callers"),
        F.min(F.col("_ingested_at")).alias("first_seen"),
        F.max(F.col("_ingested_at")).alias("last_seen"),
    )

    ranking_window = Window.partitionBy("contract_address").orderBy(F.desc("call_count"))

    return (
        ranked.withColumn("rank", F.row_number().over(ranking_window))
        .filter(F.col("rank") <= 50)
        .select(
            F.col("contract_address"),
            F.col("method"),
            F.col("method_id"),
            F.col("decode_type"),
            F.col("call_count"),
            F.col("unique_callers"),
            F.col("first_seen"),
            F.col("last_seen"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy(F.desc("call_count"))
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 8 — Contract Deploy Metrics Hourly → `g_apps.contract_deploy_metrics_hourly`
# MAGIC
# MAGIC Métricas horárias de deploys de contratos Ethereum (tx_type_semantic = contract_deploy).
# MAGIC Monitora ritmo de implantação, deployers únicos e custo médio de gas. (UC5)

# COMMAND ----------


@dlt.table(
    name="g_apps.contract_deploy_metrics_hourly",
    comment="Gold MV (UC5): métricas horárias de deploys de contratos Ethereum",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_contract_deploy_metrics_hourly():
    txs = (
        dlt.read("s_apps.transactions_ethereum")
        .filter(F.col("tx_status") == "valid")
        .filter(F.col("tx_type_semantic") == "contract_deploy")
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("_ingested_at")))
        .withColumn("gas_price_gwei", F.col("gas_price").cast("double") / F.lit(1e9))
    )

    return (
        txs.groupBy("hour_bucket")
        .agg(
            F.count("tx_hash").alias("deploy_count"),
            F.countDistinct("from_address").alias("unique_deployers"),
            F.round(F.avg("gas_price_gwei"), 4).alias("avg_gas_price_gwei"),
        )
        .select(
            F.col("hour_bucket"),
            F.col("deploy_count"),
            F.col("unique_deployers"),
            F.col("avg_gas_price_gwei"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy("hour_bucket")
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 9 — Contract Volume Ranking → `g_apps.contract_volume_ranking`
# MAGIC
# MAGIC Ranking de contratos por volume de ETH recebido nas últimas 24 horas.
# MAGIC Identifica contratos de maior relevância financeira para monitoring e alertas. (UC7)

# COMMAND ----------


@dlt.table(
    name="g_apps.contract_volume_ranking",
    comment="Gold MV (UC7): ranking de contratos por volume de ETH recebido nas últimas 24h",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_contract_volume_ranking():
    txs = (
        dlt.read("s_apps.transactions_ethereum")
        .filter(F.col("tx_status") == "valid")
        .filter(F.col("tx_type_semantic") == "contract_interaction")
        .filter(F.col("_ingested_at") >= F.expr("current_timestamp() - INTERVAL 24 HOURS"))
        .filter(F.col("to_address").isNotNull())
        .withColumn("eth_value", F.col("value").cast("double") / F.lit(1e18))
    )

    return (
        txs.groupBy("to_address")
        .agg(
            F.count("tx_hash").alias("tx_count"),
            F.countDistinct("from_address").alias("unique_senders"),
            F.round(F.sum("eth_value"), 8).alias("total_eth_received"),
            F.round(F.avg("eth_value"), 8).alias("avg_eth_per_tx"),
            F.min(F.col("_ingested_at")).alias("first_seen"),
            F.max(F.col("_ingested_at")).alias("last_seen"),
        )
        .select(
            F.col("to_address").alias("contract_address"),
            F.col("tx_count"),
            F.col("unique_senders"),
            F.col("total_eth_received"),
            F.col("avg_eth_per_tx"),
            F.col("first_seen"),
            F.col("last_seen"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy(F.desc("total_eth_received"))
    )


# ════════════════════════════════════════════════════════════════════════════
# GOLD — Block-Only Materialized Views  (g_network.*)
# Fonte exclusiva: s_apps.eth_blocks e s_apps.eth_blocks_withdrawals
# ════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 6 — ETH Burn Hourly → `g_network.eth_burn_hourly`
# MAGIC
# MAGIC ETH destruído por hora via mecanismo EIP-1559: cada unidade de gas consumida
# MAGIC é cobrada pela `base_fee_per_gas`, que é integralmente queimada (não vai ao
# MAGIC validador). Indicador primário de deflação do protocolo Ethereum.
# MAGIC
# MAGIC Fórmula: `eth_burned = base_fee_per_gas × gas_used / 1e18`

# COMMAND ----------


@dlt.table(
    name="g_network.eth_burn_hourly",
    comment=(
        "Gold MV: ETH queimado por hora via EIP-1559 (base_fee_per_gas × gas_used / 1e18). "
        "Indicador de deflação do protocolo e pressão de demanda da rede."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_eth_burn_hourly():
    """
    Agrega o ETH queimado por hora a partir dos dados de blocos (eth_blocks).
    ETH burned por bloco = base_fee_per_gas (Wei) × gas_used / 1e18 (conversão para ETH).
    base_fee_per_gas aumenta ≤12,5% quando o bloco anterior ultrapassou 50% de utilização.
    """
    blocks = dlt.read("s_apps.eth_blocks")

    burn_wei = F.col("base_fee_per_gas").cast("double") * F.col("gas_used").cast("double")

    return (
        blocks.withColumn("eth_burned_block", burn_wei / F.lit(1e18))
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("block_time")))
        .groupBy("hour_bucket")
        .agg(
            F.count("block_number").alias("block_count"),
            F.round(F.sum("eth_burned_block"), 8).alias("eth_burned_total"),
            F.round(F.avg("eth_burned_block"), 8).alias("eth_burned_per_block_avg"),
            F.round(F.max("eth_burned_block"), 8).alias("eth_burned_per_block_max"),
            F.round(F.avg(F.col("base_fee_per_gas").cast("double") / F.lit(1e9)), 4).alias(
                "avg_base_fee_gwei"
            ),
            F.round(F.max(F.col("base_fee_per_gas").cast("double") / F.lit(1e9)), 4).alias(
                "max_base_fee_gwei"
            ),
            F.round(
                F.avg(
                    F.when(
                        F.col("gas_limit") > 0, F.col("gas_used").cast("double") / F.col("gas_limit") * 100
                    )
                ),
                2,
            ).alias("avg_block_utilization_pct"),
        )
        .select(
            F.col("hour_bucket"),
            F.col("block_count"),
            F.col("eth_burned_total"),
            F.col("eth_burned_per_block_avg"),
            F.col("eth_burned_per_block_max"),
            F.col("avg_base_fee_gwei"),
            F.col("max_base_fee_gwei"),
            F.col("avg_block_utilization_pct"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy("hour_bucket")
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 7 — Validator Activity → `g_network.validator_activity`
# MAGIC
# MAGIC Concentração de validadores usando **janela deslizante de 24 horas**.
# MAGIC O campo `miner` em PoS representa o `fee_recipient` — endereço que recebe as
# MAGIC gorjetas de prioridade (`maxPriorityFeePerGas`).
# MAGIC
# MAGIC O **Índice Herfindahl-Hirschman (HHI)** mede concentração de mercado:
# MAGIC - HHI < 1500: descentralizado   · 1500–2500: moderado   · > 2500: concentrado
# MAGIC
# MAGIC Fórmula: `HHI = Σ(pct_share²)` para todos os validadores ativos nas últimas 24h.

# COMMAND ----------


@dlt.table(
    name="g_network.validator_activity",
    comment=(
        "Gold MV: concentração de validadores Ethereum nas últimas 24h. "
        "Inclui participação percentual, HHI por validador e HHI total da rede. "
        "HHI < 1500 = descentralizado | 1500-2500 = moderado | > 2500 = concentrado."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_validator_activity():
    """
    Calcula a concentração de produção de blocos por validador (fee_recipient) nas últimas 24h.
    Usa Window function para evitar collect() — pct_share e HHI calculados distribuídos.
    Em PoS, 'miner' = endereço de destino da taxa de prioridade (fee recipient).
    """
    blocks_24h = (
        dlt.read("s_apps.eth_blocks")
        .filter(F.col("block_time") >= F.expr("current_timestamp() - INTERVAL 24 HOURS"))
        .filter(F.col("miner").isNotNull())
    )

    # Agrupa por validador (fee recipient)
    by_validator = blocks_24h.groupBy("miner").agg(
        F.count("block_number").alias("blocks_produced"),
        F.min("block_time").alias("first_block_time"),
        F.max("block_time").alias("last_block_time"),
    )

    # Janela global para calcular participação percentual sem .collect()
    global_window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    total_blocks_col = F.sum("blocks_produced").over(global_window)
    pct_share_col = F.col("blocks_produced").cast("double") / total_blocks_col * 100
    hhi_component = pct_share_col * pct_share_col

    return (
        by_validator.withColumn("total_blocks_24h", total_blocks_col)
        .withColumn("pct_share", F.round(pct_share_col, 4))
        .withColumn("hhi_component", F.round(hhi_component, 4))
        .withColumn("hhi_total", F.round(F.sum(hhi_component).over(global_window), 2))
        .select(
            F.col("miner").alias("fee_recipient"),
            F.col("blocks_produced"),
            F.col("total_blocks_24h"),
            F.col("pct_share"),
            F.col("hhi_component"),
            F.col("hhi_total"),
            F.col("first_block_time"),
            F.col("last_block_time"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy(F.desc("blocks_produced"))
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 8 — Withdrawal Metrics → `g_network.withdrawal_metrics`
# MAGIC
# MAGIC Fluxo horário de saques da Beacon Chain (EIP-4895, Shanghai/Capella, abril 2023).
# MAGIC Cada validador com saldo > 32 ETH tem o excedente automaticamente sacado.
# MAGIC Máximo de **16 saques por bloco**. `amount` em Gwei (÷1e9 = ETH).
# MAGIC
# MAGIC Permite monitorar pressão de venda potencial e concentração de stake.

# COMMAND ----------


@dlt.table(
    name="g_network.withdrawal_metrics",
    comment=(
        "Gold MV: fluxo horário de saques da Beacon Chain (EIP-4895). "
        "Agrega volume de ETH sacado, número de validadores únicos e endereços de destino por hora. "
        "Indicador de pressão de venda e concentração de stake."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_withdrawal_metrics():
    """
    Métricas horárias de saques da Beacon Chain a partir de eth_blocks_withdrawals.
    Nota: block_timestamp nesta tabela Silver é string formatada (FROM_UNIXTIME 'yyyy-MM-dd HH:mm:ss')
    — converte para timestamp antes de date_trunc.
    """
    withdrawals = dlt.read("s_apps.eth_blocks_withdrawals")

    return (
        withdrawals.withColumn(
            "hour_bucket",
            F.date_trunc("hour", F.to_timestamp(F.col("block_timestamp"), "yyyy-MM-dd HH:mm:ss")),
        )
        .groupBy("hour_bucket")
        .agg(
            F.count("*").alias("withdrawal_count"),
            F.countDistinct("validator_index").alias("unique_validators"),
            F.countDistinct("withdrawal_address").alias("unique_addresses"),
            F.round(F.sum("amount_eth"), 6).alias("total_eth_withdrawn"),
            F.round(F.avg("amount_eth"), 6).alias("avg_eth_per_withdrawal"),
            F.round(F.max("amount_eth"), 6).alias("max_single_withdrawal_eth"),
            F.round(F.min("amount_eth"), 6).alias("min_single_withdrawal_eth"),
        )
        .select(
            F.col("hour_bucket"),
            F.col("withdrawal_count"),
            F.col("unique_validators"),
            F.col("unique_addresses"),
            F.col("total_eth_withdrawn"),
            F.col("avg_eth_per_withdrawal"),
            F.col("max_single_withdrawal_eth"),
            F.col("min_single_withdrawal_eth"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy("hour_bucket")
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold 9 — Block Production Health → `g_network.block_production_health`
# MAGIC
# MAGIC Saúde horária da produção de blocos Ethereum baseada em **intervalo entre blocos**.
# MAGIC Em PoS, cada slot tem **12 segundos**. Quando o gap entre blocos consecutivos é > 12s,
# MAGIC slots foram perdidos (validador escalado falhou em propor um bloco a tempo).
# MAGIC
# MAGIC Slots perdidos estimados por gap:
# MAGIC `missed_slots = FLOOR(gap_segundos / 12) - 1`   (quando gap > 12s)

# COMMAND ----------


@dlt.table(
    name="g_network.block_production_health",
    comment=(
        "Gold MV: saúde horária da produção de blocos — slots perdidos estimados, gaps de intervalo. "
        "Em PoS Ethereum, slot = 12s. Gaps > 12s indicam slots/validadores perdidos. "
        "missed_slot_rate_pct = missed_slots / (block_count + missed_slots) × 100."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_block_production_health():
    """
    Calcula a taxa de slots perdidos por hora usando LAG sobre block_timestamp (long segundos).
    block_timestamp em eth_blocks é long (Unix epoch), não string — aritmética direta.
    missed_slots_estimated = Σ(FLOOR(gap/12) - 1) para todos os gaps > 12s na hora.
    """
    blocks = dlt.read("s_apps.eth_blocks")

    ordering_window = Window.orderBy("block_number")

    blocks_with_gap = (
        blocks.withColumn(
            "prev_timestamp",
            F.lag(F.col("block_timestamp").cast("long")).over(ordering_window),
        )
        .withColumn(
            "slot_gap_sec",
            F.col("block_timestamp").cast("long") - F.col("prev_timestamp"),
        )
        .filter(F.col("slot_gap_sec").isNotNull())
        .withColumn(
            "missed_slots",
            F.when(
                F.col("slot_gap_sec") > 12,
                (F.col("slot_gap_sec") / 12).cast("long") - 1,
            ).otherwise(F.lit(0)),
        )
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("block_time")))
    )

    hourly = blocks_with_gap.groupBy("hour_bucket").agg(
        F.count("block_number").alias("block_count"),
        F.sum("missed_slots").alias("missed_slots_estimated"),
        F.round(F.avg("slot_gap_sec"), 2).alias("avg_slot_gap_sec"),
        F.round(F.max("slot_gap_sec"), 0).alias("max_slot_gap_sec"),
        F.count(F.when(F.col("slot_gap_sec") > 12, True)).alias("gap_events_count"),
    )

    return (
        hourly.withColumn(
            "missed_slot_rate_pct",
            F.round(
                F.when(
                    (F.col("block_count") + F.col("missed_slots_estimated")) > 0,
                    F.col("missed_slots_estimated").cast("double")
                    / (F.col("block_count") + F.col("missed_slots_estimated"))
                    * 100,
                ).otherwise(F.lit(0.0)),
                4,
            ),
        )
        .select(
            F.col("hour_bucket"),
            F.col("block_count"),
            F.col("missed_slots_estimated"),
            F.col("missed_slot_rate_pct"),
            F.col("avg_slot_gap_sec"),
            F.col("max_slot_gap_sec"),
            F.col("gap_events_count"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy("hour_bucket")
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ### G3. Chain Health Metrics — `g_network.chain_health_metrics`
# MAGIC
# MAGIC Taxa de orphan blocks por hora via `eth_canonical_blocks_index` + `eth_blocks`.
# MAGIC Indicador de instabilidade de rede — reorgs e latência de propagação.

# COMMAND ----------


@dlt.table(
    name="g_network.chain_health_metrics",
    comment=(
        "Gold MV: saúde horária da cadeia — taxa de blocos orphan por hora. "
        "Orphan rate alto indica instabilidade de rede (reorgs, latência de propagação). "
        "Fonte: eth_canonical_blocks_index JOIN eth_blocks."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_chain_health_metrics():
    """
    Classifica blocos por chain_status (canonical/orphan/unconfirmed) e calcula
    orphan_rate_pct por hora. Combina eth_canonical_blocks_index com eth_blocks
    para obter o timestamp do bloco para agregação horária.
    """
    index = dlt.read("s_apps.eth_canonical_blocks_index")
    blocks = dlt.read("s_apps.eth_blocks")

    block_status = index.join(
        blocks.select("block_number", "block_hash", "block_time"),
        on=["block_number", "block_hash"],
        how="inner",
    ).withColumn("hour_bucket", F.date_trunc("hour", F.col("block_time")))

    return (
        block_status.groupBy("hour_bucket")
        .agg(
            F.count("*").alias("total_blocks"),
            F.sum(F.when(F.col("chain_status") == "canonical", 1).otherwise(0)).alias("canonical_count"),
            F.sum(F.when(F.col("chain_status") == "orphan", 1).otherwise(0)).alias("orphan_count"),
            F.sum(F.when(F.col("chain_status") == "unconfirmed", 1).otherwise(0)).alias("unconfirmed_count"),
        )
        .withColumn(
            "orphan_rate_pct",
            F.round(
                F.when(
                    F.col("total_blocks") > 0,
                    F.col("orphan_count").cast("double") / F.col("total_blocks") * 100,
                ).otherwise(F.lit(0.0)),
                4,
            ),
        )
        .select(
            F.col("hour_bucket"),
            F.col("total_blocks"),
            F.col("canonical_count"),
            F.col("orphan_count"),
            F.col("unconfirmed_count"),
            F.col("orphan_rate_pct"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy("hour_bucket")
    )
