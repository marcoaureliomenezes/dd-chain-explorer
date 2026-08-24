"""
Triggered by S3 PutObject events on the Gold API keys export path.
Reads exported JSON rows and batch-writes them to the DynamoDB single-table
using PK="CONSUMPTION", SK="{source}#{api_key_name}".

Environment variables:
    DYNAMODB_TABLE  — DynamoDB table name (default: dm-chain-explorer)
"""

import json
import logging
import os
import urllib.parse
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "dm-chain-explorer")

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Process S3 event — each record is a PutObject on the exports prefix."""
    records_processed = 0

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        # Skip non-JSON files (e.g. _SUCCESS markers)
        if not key.endswith(".json"):
            logger.info(f"Skipping non-JSON file: {key}")
            continue

        logger.info(f"Processing s3://{bucket}/{key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read().decode("utf-8")

        items = []
        for line in body.strip().split("\n"):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                items.append(row)
            except json.JSONDecodeError:
                logger.warning(f"Skipping malformed JSON line: {line[:100]}")

        if not items:
            logger.info(f"No items found in {key}")
            continue

        with table.batch_writer() as batch:
            for row in items:
                source = row.get("source", "unknown")
                api_key_name = row.get("api_key_name", "unknown")
                item = {
                    "pk": "CONSUMPTION",
                    "sk": f"{source}#{api_key_name}",
                    "api_key_name": api_key_name,
                    "source": source,
                }

                # Copy all metric fields
                for field in [
                    "calls_total",
                    "calls_ok_total",
                    "calls_error_total",
                    "calls_1h",
                    "calls_2h",
                    "calls_12h",
                    "calls_24h",
                    "calls_48h",
                    "vendor",
                    "last_call_at",
                    "computed_at",
                ]:
                    if field in row and row[field] is not None:
                        item[field] = str(row[field])

                batch.put_item(Item=item)
                records_processed += 1

    logger.info(f"Processed {records_processed} consumption records to DynamoDB")
    return {"statusCode": 200, "records_processed": records_processed}
