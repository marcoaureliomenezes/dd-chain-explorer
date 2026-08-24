"""Unit tests for ``dm_chain_utils.dm_dynamodb.DMDynamoDB`` — the only single-table
DynamoDB client used by the live Lambda surface.

Intent: CONTRACT — AC-23 (live-surface pyramid: kept dm_chain_utils modules).
Size: unit — moto-mocked DynamoDB, no network egress, sub-second runtime.
"""

from __future__ import annotations

import logging

import boto3
import pytest
from dm_chain_utils.dm_dynamodb import DMDynamoDB
from moto import mock_aws

REGION = "sa-east-1"
TABLE_NAME = "dm-chain-explorer-test"


@pytest.fixture()
def db(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
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
        yield DMDynamoDB(table_name=TABLE_NAME, region=REGION, logger=logging.getLogger("test"))


class TestPutAndGetItem:
    def test_put_then_get_roundtrips_attrs(self, db):
        db.put_item("CONTRACT", "0xabc", attrs={"tx_count": 3})

        item = db.get_item("CONTRACT", "0xabc")

        assert item == {"pk": "CONTRACT", "sk": "0xabc", "tx_count": 3}

    def test_get_item_returns_none_when_absent(self, db):
        assert db.get_item("CONTRACT", "0xmissing") is None

    def test_put_item_converts_floats_to_decimal_and_back(self, db):
        # DynamoDB rejects native float; the client must round-trip it transparently.
        db.put_item("CONTRACT", "0xabc", attrs={"score": 1.5})

        item = db.get_item("CONTRACT", "0xabc")

        assert item["score"] == 1.5

    def test_put_item_with_ttl_sets_ttl_attribute(self, db):
        db.put_item("CONTRACT", "0xabc", ttl_seconds=60)

        item = db.get_item("CONTRACT", "0xabc")

        assert "ttl" in item
        assert item["ttl"] > 0


class TestDeleteAndExists:
    def test_delete_item_removes_it(self, db):
        db.put_item("CONTRACT", "0xabc")
        db.delete_item("CONTRACT", "0xabc")

        assert db.get_item("CONTRACT", "0xabc") is None

    def test_item_exists_true_and_false(self, db):
        db.put_item("CONTRACT", "0xabc")

        assert db.item_exists("CONTRACT", "0xabc") is True
        assert db.item_exists("CONTRACT", "0xmissing") is False


class TestQuery:
    def test_query_returns_all_items_for_pk(self, db):
        db.put_item("CONTRACT", "0xabc")
        db.put_item("CONTRACT", "0xdef")
        db.put_item("CONSUMPTION", "etherscan#key-1")  # different pk — must not leak

        items = db.query("CONTRACT")

        assert {item["sk"] for item in items} == {"0xabc", "0xdef"}

    def test_query_with_sk_prefix_filters(self, db):
        db.put_item("CONSUMPTION", "etherscan#key-1")
        db.put_item("CONSUMPTION", "web3#key-1")

        items = db.query("CONSUMPTION", sk_prefix="etherscan#")

        assert len(items) == 1
        assert items[0]["sk"] == "etherscan#key-1"

    def test_query_all_keys_returns_only_sort_keys(self, db):
        db.put_item("CONTRACT", "0xabc")
        db.put_item("CONTRACT", "0xdef")

        assert sorted(db.query_all_keys("CONTRACT")) == ["0xabc", "0xdef"]


class TestConditionalPutItem:
    def test_returns_true_when_condition_passes(self, db):
        ok = db.conditional_put_item("SEMAPHORE", "key-1", condition_expression="attribute_not_exists(pk)")

        assert ok is True

    def test_returns_false_when_condition_fails(self, db):
        db.put_item("SEMAPHORE", "key-1")

        ok = db.conditional_put_item("SEMAPHORE", "key-1", condition_expression="attribute_not_exists(pk)")

        assert ok is False


class TestBatchOperations:
    def test_batch_write_then_batch_delete(self, db):
        db.batch_write([{"pk": "CONTRACT", "sk": "0x1"}, {"pk": "CONTRACT", "sk": "0x2"}])

        assert len(db.query("CONTRACT")) == 2

        db.batch_delete([{"pk": "CONTRACT", "sk": "0x1"}, {"pk": "CONTRACT", "sk": "0x2"}])

        assert db.query("CONTRACT") == []

    def test_delete_all_by_pk_returns_deleted_count(self, db):
        db.put_item("CONTRACT", "0x1")
        db.put_item("CONTRACT", "0x2")
        db.put_item("CONTRACT", "0x3")

        deleted = db.delete_all_by_pk("CONTRACT")

        assert deleted == 3
        assert db.query("CONTRACT") == []


class TestUpdateItem:
    def test_update_item_sets_new_attribute(self, db):
        db.put_item("CONTRACT", "0xabc", attrs={"tx_count": 1})

        updated = db.update_item("CONTRACT", "0xabc", updates={"tx_count": 2})

        assert updated["tx_count"] == 2
        assert db.get_item("CONTRACT", "0xabc")["tx_count"] == 2


class TestPing:
    def test_ping_true_when_table_reachable(self, db):
        assert db.ping() is True
