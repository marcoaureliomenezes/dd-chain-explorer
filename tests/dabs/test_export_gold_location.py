"""Unit tests for ``job_export_gold``'s pure ``_location`` helper.

Intent: CONTRACT — AC-23 (live-surface pyramid: surviving DABs job scripts' pure
functions).
Size: unit — imports the real module (requires ``pyspark`` to be importable, since
``export_gold.py`` imports ``pyspark.sql`` at module scope; no ``SparkSession`` is ever
created or invoked in this test — ``_location`` touches only ``self.storage_mode`` and
``self.lakehouse_bucket``).

``job_ddl_setup`` and ``job_delta_maintenance`` are intentionally NOT covered here:
WS-C is reshaping both under this same release (T-C.4), so testing them now would pin
behavior about to change under a workstream this dispatch does not own.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip(
    "pyspark", reason="pyspark not installed — deliberately excluded from CI, see tests/README.md"
)

import sys  # noqa: E402

from tests.conftest import DABS_EXPORT_GOLD_SRC  # noqa: E402  (after importorskip)

if str(DABS_EXPORT_GOLD_SRC) not in sys.path:
    sys.path.insert(0, str(DABS_EXPORT_GOLD_SRC))

from dm_export_gold.export_gold import Exportgold  # noqa: E402


class TestLocation:
    def test_managed_mode_returns_empty_location_clause(self):
        job = Exportgold(spark=None, catalog="dev", export_s3_path="s3://bucket/exports")

        assert job._location("gold_api_keys") == ""

    def test_external_mode_with_bucket_returns_location_clause(self):
        job = Exportgold(
            spark=None,
            catalog="prod",
            export_s3_path="s3://bucket/exports",
            storage_mode="external",
            lakehouse_bucket="dm-chain-explorer-lakehouse",
        )

        assert job._location("gold_api_keys") == "LOCATION 's3://dm-chain-explorer-lakehouse/gold_api_keys'"

    def test_external_mode_without_bucket_returns_empty_string(self):
        # storage_mode="external" alone is not enough — an empty bucket must not
        # produce a malformed `LOCATION 's3:///...'` clause.
        job = Exportgold(
            spark=None,
            catalog="prod",
            export_s3_path="s3://bucket/exports",
            storage_mode="external",
            lakehouse_bucket="",
        )

        assert job._location("gold_api_keys") == ""
