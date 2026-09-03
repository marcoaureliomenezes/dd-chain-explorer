"""dm-export-gold — export_gold task.

Export Gold API key consumption views to S3

Usage (local):
    python -m dm_export_gold.export_gold \\
        --catalog dev \\
        --export-s3-path s3://bucket/exports \\
        --storage-mode managed

Usage (Databricks wheel task):
    entry_point: dm-export-gold-export-gold
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


class Exportgold:
    """Main logic for the export_gold task."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        export_s3_path: str,
        storage_mode: str = "managed",
        lakehouse_bucket: str = "",
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.export_s3_path = export_s3_path
        self.storage_mode = storage_mode
        self.lakehouse_bucket = lakehouse_bucket

    def _table(self, schema: str, name: str) -> str:
        return f"`{self.catalog}`.{schema}.{name}"

    def _location(self, relative_path: str) -> str:
        """Returns LOCATION clause for EXTERNAL tables (PROD only)."""
        if self.storage_mode == "external" and self.lakehouse_bucket:
            return f"LOCATION 's3://{self.lakehouse_bucket}/{relative_path}'"
        return ""

    def run(self) -> None:
        """Export Gold API key consumption views to S3."""
        _log.info(
            "Running export_gold | catalog=%s | export_s3_path=%s | storage_mode=%s",
            self.catalog,
            self.export_s3_path,
            self.storage_mode,
        )

        cat = self.catalog
        df_etherscan = self.spark.table(f"`{cat}`.g_api_keys.etherscan_consumption").withColumn(
            "source", F.lit("etherscan")
        )
        df_web3 = self.spark.table(f"`{cat}`.g_api_keys.web3_keys_consumption").withColumn(
            "source", F.lit("web3")
        )
        df_combined = df_etherscan.unionByName(df_web3, allowMissingColumns=True)

        count = df_combined.count()
        _log.info("Exporting %d rows to S3", count)

        (df_combined.coalesce(1).write.mode("overwrite").json(f"{self.export_s3_path}/gold_api_keys"))
        _log.info("Gold API key consumption exported to %s/gold_api_keys", self.export_s3_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="dm-export-gold — export_gold")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    parser.add_argument(
        "--export-s3-path", required=True, help="S3 path for exporting Gold views (e.g., s3://bucket/exports)"
    )
    parser.add_argument(
        "--storage-mode",
        default="managed",
        choices=["managed", "external"],
        help="Table storage mode: managed (DEV) or external (PROD)",
    )
    parser.add_argument("--lakehouse-bucket", default="", help="S3 bucket for EXTERNAL tables (PROD only)")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    Exportgold(
        spark=spark,
        catalog=args.catalog,
        export_s3_path=args.export_s3_path,
        storage_mode=args.storage_mode,
        lakehouse_bucket=args.lakehouse_bucket,
    ).run()


if __name__ == "__main__":
    main()
