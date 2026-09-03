"""Unit tests for the ``contracts_ingestion`` Lambda handler.

Intent: CONTRACT — AC-23 (live-surface pyramid: both Lambda handlers).
Size: unit — every AWS call is mocked (moto for DynamoDB/SSM/S3), no network egress,
no live credentials, sub-second runtime.
"""

from __future__ import annotations

import json
import logging

import boto3
import pytest
from moto import mock_aws

from tests.conftest import LAMBDA_CONTRACTS_INGESTION, load_module_from_path

HANDLER_PATH = LAMBDA_CONTRACTS_INGESTION / "handler.py"
REGION = "sa-east-1"


@pytest.fixture()
def handler_module():
    """Load contracts_ingestion/handler.py under a collision-safe module name."""
    return load_module_from_path("contracts_ingestion_handler", HANDLER_PATH)


@pytest.fixture()
def aws(monkeypatch):
    """A moto-mocked AWS account with the DynamoDB table + SSM path this Lambda reads."""
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name=REGION)
        table = ddb.create_table(
            TableName="dm-chain-explorer-test",
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="dm-chain-explorer-test")
        ssm = boto3.client("ssm", region_name=REGION)
        yield {"table": table, "ssm": ssm}


class TestNormalizeTx:
    """Pure-function coverage of the Etherscan -> internal schema mapping."""

    def test_normalize_tx_maps_all_fields(self, handler_module):
        raw = {
            "hash": "0xabc",
            "blockNumber": "123",
            "timeStamp": "1700000000",
            "from": "0xfrom",
            "to": "0xto",
            "value": "1000000000000000000",
            "gasUsed": "21000",
            "txreceipt_status": "1",
            "isError": "0",
            "methodId": "0x38ed1739",
            "functionName": "swap(uint256)",
            "input": "0xdeadbeef",
        }
        result = handler_module.ContractTransactionsCrawler._normalize_tx(raw, "0xcontract")

        assert result == {
            "contract_address": "0xcontract",
            "tx_hash": "0xabc",
            "block_number": 123,
            "timestamp": 1700000000,
            "from_address": "0xfrom",
            "to_address": "0xto",
            "value": "1000000000000000000",
            "gas_used": 21000,
            "receipt_status": 1,
            "is_error": 0,
            "method_id": "0x38ed1739",
            "function_name": "swap(uint256)",
            "input": "0xdeadbeef",
        }

    def test_normalize_tx_defaults_missing_optional_fields(self, handler_module):
        # A minimal record still produces a well-typed row — no KeyError, integer
        # fields default to 0, string fields default to "" (or "0x" for input).
        result = handler_module.ContractTransactionsCrawler._normalize_tx({}, "0xcontract")

        assert result["tx_hash"] == ""
        assert result["block_number"] == 0
        assert result["is_error"] == 0
        assert result["input"] == "0x"


class TestDryRunValidation:
    """``_dry_run_validation`` is the CI dry-run gate's connectivity probe."""

    def test_returns_ok_with_keys_and_contracts(self, handler_module, aws):
        aws["ssm"].put_parameter(
            Name="/etherscan-api-keys/api-key-1",
            Value="fake-key-value",
            Type="SecureString",
        )
        aws["table"].put_item(Item={"pk": "CONTRACT", "sk": "0xabc", "tx_count": 5})

        result = handler_module._dry_run_validation("/etherscan-api-keys", "dm-chain-explorer-test")

        assert result["status"] == "ok"
        assert result["etherscan_keys"] == 1
        assert result["contracts_found"] == 1
        assert result["warnings"] == []

    def test_warns_when_no_contracts_found(self, handler_module, aws):
        aws["ssm"].put_parameter(
            Name="/etherscan-api-keys/api-key-1",
            Value="fake-key-value",
            Type="SecureString",
        )
        # No CONTRACT items written — DynamoDB table is empty.

        result = handler_module._dry_run_validation("/etherscan-api-keys", "dm-chain-explorer-test")

        assert result["status"] == "warning"
        assert result["contracts_found"] == 0
        assert len(result["warnings"]) == 1

    def test_raises_when_no_ssm_keys_found(self, handler_module, aws):
        # No SSM parameters at all under the path — this is a hard failure, not a warning.
        with pytest.raises(RuntimeError, match="No Etherscan keys found"):
            handler_module._dry_run_validation("/etherscan-api-keys", "dm-chain-explorer-test")


class TestGetBlockInterval:
    """``_get_block_interval`` — E731 lambda-to-def refactor (T-D.5); this test
    pins the observable behavior across that refactor."""

    def test_returns_before_and_after_block_numbers_on_ok(self, handler_module):
        crawler = handler_module.ContractTransactionsCrawler(logging.getLogger("test"))
        crawler.timestamp_interval = (1000, 2000)
        calls = []

        class _FakeEtherscan:
            def get_block_by_timestamp(self, ts, closest):
                calls.append((ts, closest))
                return {"message": "OK", "result": str(ts)}

        crawler.etherscan_client = _FakeEtherscan()

        result = crawler._get_block_interval()

        assert result == ("1000", "2000")
        assert calls == [(1000, "after"), (2000, "before")]

    def test_returns_none_when_either_side_is_not_ok(self, handler_module):
        crawler = handler_module.ContractTransactionsCrawler(logging.getLogger("test"))
        crawler.timestamp_interval = (1000, 2000)

        class _FakeEtherscan:
            def get_block_by_timestamp(self, ts, closest):
                return {"message": "NOTOK", "result": None}

        crawler.etherscan_client = _FakeEtherscan()

        assert crawler._get_block_interval() is None


class TestHandlerDryRunEvent:
    """The handler's ``dry_run`` branch, exercised end-to-end (no Etherscan network calls)."""

    def test_handler_returns_200_body_with_dry_run_result(self, handler_module, aws, monkeypatch):
        aws["ssm"].put_parameter(
            Name="/etherscan-api-keys/api-key-1",
            Value="fake-key-value",
            Type="SecureString",
        )
        monkeypatch.setenv("SSM_ETHERSCAN_PATH", "/etherscan-api-keys")
        monkeypatch.setenv("DYNAMODB_TABLE", "dm-chain-explorer-test")

        response = handler_module.handler({"dry_run": True}, context=None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["dry_run"] is True
        assert body["etherscan_keys"] == 1
        assert body["status"] == "warning"  # 0 CONTRACT items in this fixture


class TestContractTransactionsCrawlerS3Write:
    """S3 write path — idempotent-by-key, verified against a moto S3 bucket."""

    @pytest.fixture()
    def s3(self, monkeypatch):
        monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        with mock_aws():
            client = boto3.client("s3", region_name=REGION)
            client.create_bucket(
                Bucket="dm-chain-explorer-test-ingestion",
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
            yield client

    def test_write_to_s3_skips_when_key_already_exists(self, handler_module, s3):
        bucket = "dm-chain-explorer-test-ingestion"
        key = "batch/year=2026/month=8/day=23/hour=10/txs_0xabc.json"
        s3.put_object(Bucket=bucket, Key=key, Body=b'{"already": "there"}')

        crawler = handler_module.ContractTransactionsCrawler(logging.getLogger("test"))
        crawler.write_config(s3, bucket=bucket, bucket_prefix="batch", overwrite=False)
        crawler.current_s3_key = key

        crawler._write_to_s3([{"new": "data"}])

        # The pre-existing object must be untouched — write_to_s3's whole contract
        # is "never overwrite an already-ingested hour".
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        assert json.loads(body) == {"already": "there"}

    def test_write_to_s3_writes_new_key(self, handler_module, s3):
        bucket = "dm-chain-explorer-test-ingestion"
        key = "batch/year=2026/month=8/day=23/hour=10/txs_0xdef.json"

        crawler = handler_module.ContractTransactionsCrawler(logging.getLogger("test"))
        crawler.write_config(s3, bucket=bucket, bucket_prefix="batch", overwrite=False)
        crawler.current_s3_key = key

        crawler._write_to_s3([{"tx_hash": "0x1"}])

        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        assert json.loads(body) == [{"tx_hash": "0x1"}]
