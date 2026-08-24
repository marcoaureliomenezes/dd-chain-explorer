"""Unit tests for the ``gold_to_dynamodb`` Lambda handler.

Intent: CONTRACT — AC-23 (live-surface pyramid: both Lambda handlers).
Size: unit — moto-mocked S3 + DynamoDB, no network egress, sub-second runtime.

The module under test builds its ``boto3`` clients (``s3_client``, ``table``) at
*import* time, keyed off ``DYNAMODB_TABLE``. That means moto must be active and the
env var set **before** the module is loaded — the ``handler_module`` fixture below
enters the moto context and sets the table name before ever calling
``load_module_from_path``.
"""

from __future__ import annotations

import json

import boto3
import pytest
from moto import mock_aws

from tests.conftest import LAMBDA_GOLD_TO_DYNAMODB, load_module_from_path

HANDLER_PATH = LAMBDA_GOLD_TO_DYNAMODB / "handler.py"
REGION = "sa-east-1"
TABLE_NAME = "dm-chain-explorer-test"
BUCKET_NAME = "dm-chain-explorer-test-lakehouse"


@pytest.fixture()
def handler_module(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("DYNAMODB_TABLE", TABLE_NAME)

    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name=REGION)
        table = ddb.create_table(
            TableName=TABLE_NAME,
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
        table.meta.client.get_waiter("table_exists").wait(TableName=TABLE_NAME)

        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration={"LocationConstraint": REGION})

        module = load_module_from_path("gold_to_dynamodb_handler", HANDLER_PATH)
        yield module, table, s3


def _s3_event(key: str) -> dict:
    return {"Records": [{"s3": {"bucket": {"name": BUCKET_NAME}, "object": {"key": key}}}]}


class TestGoldToDynamoDBHandler:
    def test_writes_consumption_items_from_ndjson(self, handler_module):
        module, table, s3 = handler_module
        key = "exports/gold_api_keys/part-00000.json"
        body = "\n".join(
            [
                json.dumps({"source": "etherscan", "api_key_name": "api-key-1", "calls_total": 42}),
                json.dumps({"source": "web3", "api_key_name": "infura-1", "calls_total": 7}),
            ]
        )
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body.encode("utf-8"))

        response = module.handler(_s3_event(key), context=None)

        assert response == {"statusCode": 200, "records_processed": 2}
        item = table.get_item(Key={"pk": "CONSUMPTION", "sk": "etherscan#api-key-1"})["Item"]
        assert item["calls_total"] == "42"
        assert item["source"] == "etherscan"

    def test_skips_non_json_keys(self, handler_module):
        module, table, s3 = handler_module
        key = "exports/gold_api_keys/_SUCCESS"
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"")

        response = module.handler(_s3_event(key), context=None)

        assert response == {"statusCode": 200, "records_processed": 0}

    def test_skips_malformed_json_lines_but_processes_the_rest(self, handler_module):
        module, table, s3 = handler_module
        key = "exports/gold_api_keys/part-00001.json"
        body = "\n".join(
            [
                "{not valid json",
                json.dumps({"source": "etherscan", "api_key_name": "api-key-2"}),
            ]
        )
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body.encode("utf-8"))

        response = module.handler(_s3_event(key), context=None)

        assert response == {"statusCode": 200, "records_processed": 1}
        item = table.get_item(Key={"pk": "CONSUMPTION", "sk": "etherscan#api-key-2"})["Item"]
        assert item["api_key_name"] == "api-key-2"

    def test_empty_file_produces_no_writes(self, handler_module):
        module, table, s3 = handler_module
        key = "exports/gold_api_keys/part-00002.json"
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"\n\n")

        response = module.handler(_s3_event(key), context=None)

        assert response == {"statusCode": 200, "records_processed": 0}

    def test_defaults_missing_source_and_key_name_to_unknown(self, handler_module):
        module, table, s3 = handler_module
        key = "exports/gold_api_keys/part-00003.json"
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps({}).encode("utf-8"))

        module.handler(_s3_event(key), context=None)

        item = table.get_item(Key={"pk": "CONSUMPTION", "sk": "unknown#unknown"})["Item"]
        assert item["source"] == "unknown"
        assert item["api_key_name"] == "unknown"
