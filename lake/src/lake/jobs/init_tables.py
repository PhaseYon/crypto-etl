"""One-shot job: create every lake table (idempotent) before the streaming jobs start.

Creating tables up front means a job never starts against a missing source table, and no
two processes race to write a table's first Delta commit (unsafe on S3 with the default
log store).
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession

from lake.config import LakeConfig
from lake.jobs._runner import configure_logging
from lake.schemas import BRONZE_TRADES, GOLD_OHLCV_1M, OPS_TRADE_GAPS, SILVER_QUARANTINE, SILVER_TRADES
from lake.spark import build_spark
from lake.tables import ensure_table

NAME = "init_tables"
log = logging.getLogger("lake.init_tables")


def init_tables(spark: SparkSession, config: LakeConfig) -> None:
    ensure_table(spark, config.bronze_trades, BRONZE_TRADES, partition_by="ingest_date")
    ensure_table(spark, config.silver_trades, SILVER_TRADES, partition_by="trade_date")
    ensure_table(spark, config.silver_quarantine, SILVER_QUARANTINE)
    ensure_table(spark, config.gold_ohlcv_1m, GOLD_OHLCV_1M, partition_by="window_date")
    ensure_table(spark, config.ops_trade_gaps, OPS_TRADE_GAPS)
    log.info("lake tables ready under %s", config.lake_root)


def main() -> int:
    configure_logging()
    config = LakeConfig.from_env()
    spark = build_spark(NAME, config, master=config.spark_master)
    try:
        init_tables(spark, config)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
