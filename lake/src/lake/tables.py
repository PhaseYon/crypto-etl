"""Delta table I/O: creating tables and idempotent MERGE writes."""

from __future__ import annotations

import datetime as dt
import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


def ensure_table(spark: SparkSession, path: str, schema: StructType, partition_by: str | None = None) -> DeltaTable:
    """Create the Delta table at `path` if it doesn't exist (no-op otherwise)."""
    builder = (
        DeltaTable.createIfNotExists(spark)
        .location(path)
        .addColumns(schema)
        .property("delta.autoOptimize.autoCompact", "true")
    )
    if partition_by:
        builder = builder.partitionedBy(partition_by)
    return builder.execute()


def _date_list(dates: list[dt.date]) -> str:
    return ", ".join(f"DATE'{d.isoformat()}'" for d in sorted(dates))


def distinct_dates(df: DataFrame, column: str) -> list[dt.date]:
    return [row[0] for row in df.select(column).distinct().collect() if row[0] is not None]


def insert_new_trades(spark: SparkSession, path: str, trades: DataFrame) -> None:
    """Insert trades whose (exchange, product_id, trade_id) isn't in silver yet.

    Insert-only MERGE: replaying the same batch inserts nothing, and the table only ever
    gets AddFile actions, so it stays readable as a streaming source.
    """
    dates = distinct_dates(trades, "trade_date")
    if not dates:
        return
    condition = (
        f"t.trade_date IN ({_date_list(dates)}) AND t.trade_date = s.trade_date "
        "AND t.exchange = s.exchange AND t.product_id = s.product_id AND t.trade_id = s.trade_id"
    )
    DeltaTable.forPath(spark, path).alias("t").merge(trades.alias("s"), condition).whenNotMatchedInsertAll().execute()


def insert_new_quarantine(spark: SparkSession, path: str, rows: DataFrame) -> None:
    """Insert quarantined rows not already recorded (keyed by their Kafka position)."""
    condition = "t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset"
    DeltaTable.forPath(spark, path).alias("t").merge(rows.alias("s"), condition).whenNotMatchedInsertAll().execute()


def upsert_candles(spark: SparkSession, path: str, candles: DataFrame, dates: list[dt.date]) -> None:
    """Replace or insert candles keyed by (product_id, window_start)."""
    if not dates:
        return
    condition = (
        f"t.window_date IN ({_date_list(dates)}) AND t.window_date = s.window_date "
        "AND t.product_id = s.product_id AND t.window_start = s.window_start"
    )
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(candles.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
