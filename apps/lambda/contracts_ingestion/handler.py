"""
Lambda: Contracts Ingestion — Etherscan → S3

Captura transações de contratos populares via API Etherscan e grava
arquivos JSON no S3 no formato esperado pelo notebook Databricks
1_s3_to_bronze_contracts_txs.py:

    {bucket}/{prefix}/year={Y}/month={M}/day={D}/hour={H}/txs_{addr}.json

Trigger: EventBridge Scheduler (rate 1 hour)
Runtime: Python 3.12  |  Timeout: 300s  |  Memory: 256 MB

Dependências (via Lambda Layer dm-chain-utils):
    dm_chain_utils.dm_dynamodb
    dm_chain_utils.dm_etherscan
    dm_chain_utils.dm_parameter_store
    requests
"""

import json
import logging
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from dm_chain_utils.dm_dynamodb import DMDynamoDB
from dm_chain_utils.dm_etherscan import EtherscanClient
from dm_chain_utils.dm_parameter_store import ParameterStoreClient

logger = logging.getLogger("contracts_ingestion")
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# ContractTransactionsCrawler — adapted from
# docker/onchain-batch-txs/src/batch_ingestion/1_capture_and_ingest_contracts_txs.py
#
# Changes vs original:
#   - Removed CloudWatchLoggingHandler (Lambda logs natively to CloudWatch)
#   - Removed local filesystem writes (uses /tmp/ for transient files)
#   - Uses json.dumps → S3 put_object (no upload_file)
# ---------------------------------------------------------------------------


class ContractTransactionsCrawler:
    def __init__(self, log: logging.Logger) -> None:
        self.logger = log
        self.timestamp_interval: tuple[int, int] | None = None
        self.block_interval: tuple[str, str] | None = None
        self.paths: Any | None = None
        self._configure_pagination()

    def _configure_pagination(
        self, page: int = 1, offset: int = 1000, sort: str = "asc"
    ) -> "ContractTransactionsCrawler":
        self.page = page
        self.offset = offset
        self.sort = sort
        return self

    def read_config(
        self, dynamodb_client: DMDynamoDB, etherscan_client: EtherscanClient
    ) -> "ContractTransactionsCrawler":
        self.etherscan_client = etherscan_client
        self.dynamodb_client = dynamodb_client
        return self

    def get_contracts(self) -> list[str]:
        items = self.dynamodb_client.query("CONTRACT")
        data = [(item.get("sk", ""), int(item.get("tx_count", 0))) for item in items]
        return [addr for addr, _ in sorted(data, key=lambda x: x[1], reverse=True)]

    def write_config(
        self, s3_client: Any, bucket: str, bucket_prefix: str, overwrite: bool = False
    ) -> "ContractTransactionsCrawler":
        self.s3_client = s3_client
        self.bucket = bucket
        self.s3_bucket_prefix = bucket_prefix
        self.overwrite = overwrite
        return self

    def interval_config(self, end_date: str, window_hours: int = 1) -> "ContractTransactionsCrawler":
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S%z")
        start_date = end_date_dt - timedelta(hours=window_hours)
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date_dt.timestamp())
        self.timestamp_interval = (start_ts, end_ts)
        self.logger.info(f"timestamp_start:{start_ts};timestamp_end:{end_ts}")
        self.block_interval = self._get_block_interval()
        if self.block_interval is None:
            raise RuntimeError(
                f"Etherscan returned NOTOK for block timestamp lookup — "
                f"interval={self.timestamp_interval}. "
                "Check that EXEC_DATE is not in the future and that the API key is valid."
            )
        return self

    def _config_file_name(self, contract_addr: str) -> "ContractTransactionsCrawler":
        assert self.timestamp_interval is not None
        dat_hour = datetime.fromtimestamp(self.timestamp_interval[1])
        blocks_interval = (
            f"year={dat_hour.year}/month={dat_hour.month}/day={dat_hour.day}/hour={dat_hour.hour}"
        )
        s3_key = f"{self.s3_bucket_prefix}/{blocks_interval}/txs_{contract_addr}.json"
        self.current_s3_key = s3_key
        self.logger.info(f"s3_key:{s3_key}")
        return self

    def _get_block_interval(self) -> tuple[str, str] | None:
        def get_block(ts: int, closest: str) -> dict[str, Any]:
            return self.etherscan_client.get_block_by_timestamp(ts, closest=closest)

        assert self.timestamp_interval is not None
        block_before = get_block(self.timestamp_interval[0], closest="after")
        block_after = get_block(self.timestamp_interval[1], closest="before")
        if block_before["message"] != "OK" or block_after["message"] != "OK":
            return None
        self.logger.info(f"block_before:{block_before['result']};block_after:{block_after['result']}")
        return (block_before["result"], block_after["result"])

    @staticmethod
    def _normalize_tx(raw: dict[str, Any], contract_address: str) -> dict[str, Any]:
        """
        Normalize a raw Etherscan ``txlist`` record to snake_case schema.

        Maps Etherscan camelCase field names to the schema expected by
        ``b_ethereum.popular_contracts_txs`` (DDL in job_ddl_setup).

        Fields included:
          contract_address  — injected (not in Etherscan response)
          tx_hash           — Etherscan: hash
          block_number      — Etherscan: blockNumber (cast to int)
          timestamp         — Etherscan: timeStamp (Unix epoch as int)
          from_address      — Etherscan: from
          to_address        — Etherscan: to
          value             — Etherscan: value (Wei string, 256-bit precision)
          gas_used          — Etherscan: gasUsed (int)
          receipt_status    — Etherscan: txreceipt_status (int: 1=ok, 0=reverted)
          is_error          — Etherscan: isError (int: 0=ok, 1=error)
          method_id         — Etherscan: methodId (4-byte hex selector)
          function_name     — Etherscan: functionName (human-readable, may be empty)
          input             — Etherscan: input (full calldata hex)
        """
        return {
            "contract_address": contract_address,
            "tx_hash": raw.get("hash", ""),
            "block_number": int(raw.get("blockNumber", 0)),
            "timestamp": int(raw.get("timeStamp", 0)),
            "from_address": raw.get("from", ""),
            "to_address": raw.get("to", ""),
            "value": raw.get("value", "0"),
            "gas_used": int(raw.get("gasUsed", 0)),
            "receipt_status": int(raw.get("txreceipt_status", 0) or 0),
            "is_error": int(raw.get("isError", 0) or 0),
            "method_id": raw.get("methodId", ""),
            "function_name": raw.get("functionName", ""),
            "input": raw.get("input", "0x"),
        }

    def _get_transactions(self, contract_address: str) -> Iterator[list[dict[str, Any]]]:
        assert self.block_interval is not None
        lower_limit, upper_limit = self.block_interval
        block_before, block_after = lower_limit, str(int(upper_limit) - 1)
        data: list[dict[str, Any]] = []
        while block_before < block_after:
            result = self.etherscan_client.get_contract_txs_by_block_interval(
                contract_address,
                int(block_before),
                int(block_after),
                page=self.page,
                offset=self.offset,
                sort=self.sort,
            )
            if result["message"] != "OK":
                return
            if data == result["result"]:
                return
            data = result["result"]
            block_before = str(data[-1]["blockNumber"])
            yield [self._normalize_tx(tx, contract_address) for tx in data]

    def _check_file_exists(self, key: str) -> bool:
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def _write_to_s3(self, data: list[dict[str, Any]]) -> None:
        if self._check_file_exists(self.current_s3_key):
            self.logger.info(f"File {self.current_s3_key} already exists in S3. Skipping...")
            return
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=self.current_s3_key,
            Body=json.dumps(data).encode("utf-8"),
            ContentType="application/json",
        )

    def run(self) -> dict[str, int]:
        contracts = self.get_contracts()
        contracts_processed = 0
        total_txs = 0
        for contract in contracts:
            all_contract_data: list[dict[str, Any]] = []
            self._config_file_name(contract)
            for data in self._get_transactions(contract):
                all_contract_data.extend(data)
            self.logger.info(f"Contract {contract} has {len(all_contract_data)} transactions")
            self._write_to_s3(all_contract_data)
            contracts_processed += 1
            total_txs += len(all_contract_data)

        self.logger.info(
            f"etherscan;api_summary;"
            f"contracts_processed:{contracts_processed};"
            f"total_transactions:{total_txs};"
            f"request_count:{self.etherscan_client.call_count}"
        )
        return {"contracts_processed": contracts_processed, "total_txs": total_txs}


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------


def _dry_run_validation(ssm_path: str, dynamodb_table: str) -> dict[str, Any]:
    """
    Validates connectivity to upstream dependencies without calling Etherscan
    or writing to S3.  Used by the CI dry-run Lambda test gate.

    Returns a dict with:
        etherscan_keys  — number of API keys found in SSM
        contracts_found — number of CONTRACT items in DynamoDB
        status          — "ok" | "warning"
        warnings        — list of non-blocking issues (e.g. no contracts yet)
    """
    warnings: list[str] = []

    ssm = ParameterStoreClient()
    etherscan_keys = ssm.get_parameters_by_path(ssm_path)
    if not etherscan_keys:
        raise RuntimeError(f"dry_run: No Etherscan keys found under {ssm_path}")
    logger.info(f"dry_run: {len(etherscan_keys)} Etherscan key(s) found")

    dynamodb_client = DMDynamoDB(logger=logger, table_name=dynamodb_table)
    contracts = dynamodb_client.query("CONTRACT")
    contracts_found = len(contracts) if contracts else 0
    if contracts_found == 0:
        warnings.append(
            "DynamoDB has 0 CONTRACT items — Gold MV popular_contracts_ranking "
            "may not have run yet. Lambda will produce no output until contracts are populated."
        )
        logger.warning(f"dry_run: 0 CONTRACT items in {dynamodb_table}")
    else:
        logger.info(f"dry_run: {contracts_found} CONTRACT item(s) found in {dynamodb_table}")

    return {
        "etherscan_keys": len(etherscan_keys),
        "contracts_found": contracts_found,
        "status": "warning" if warnings else "ok",
        "warnings": warnings,
    }


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    EventBridge Scheduler invokes with empty event or custom payload.
    Optional event fields:
        exec_date  — "YYYY-MM-DD HH:MM:SS+0000" (defaults to current hour truncated)
        dry_run    — if True, validate SSM + DynamoDB connectivity only (no Etherscan, no S3 writes)
    """
    ssm_path = os.environ.get("SSM_ETHERSCAN_PATH", "/etherscan-api-keys")
    dynamodb_table = os.environ.get("DYNAMODB_TABLE", "dm-chain-explorer")

    if event.get("dry_run"):
        result = _dry_run_validation(ssm_path, dynamodb_table)
        return {"statusCode": 200, "body": json.dumps({"dry_run": True, **result})}

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    exec_date = event.get("exec_date", now.strftime("%Y-%m-%d %H:%M:%S%z"))

    s3_bucket = os.environ["S3_BUCKET"]
    s3_prefix = os.environ.get("S3_BUCKET_PREFIX", "batch")
    network = os.environ.get("NETWORK", "mainnet")

    s3_client = boto3.client("s3")
    dynamodb_client = DMDynamoDB(logger=logger, table_name=dynamodb_table)

    ssm = ParameterStoreClient()
    etherscan_keys = ssm.get_parameters_by_path(ssm_path)
    if not etherscan_keys:
        raise RuntimeError(f"No Etherscan keys found under {ssm_path}")
    api_key_name, api_key = next(iter(etherscan_keys.items()))
    etherscan_client = EtherscanClient(logger, api_key, network=network, api_key_name=api_key_name)

    run_result = (
        ContractTransactionsCrawler(logger)
        .read_config(dynamodb_client, etherscan_client)
        .write_config(s3_client, bucket=s3_bucket, bucket_prefix=s3_prefix, overwrite=False)
        .interval_config(exec_date)
        .run()
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"exec_date": exec_date, **run_result}),
    }
