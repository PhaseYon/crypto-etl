"""Silver job: bronze/trades -> silver/trades (+ silver/trades_quarantine).

Each micro-batch is parsed and validated; invalid rows go to quarantine with a reason,
valid rows are deduped and inserted with an insert-only MERGE on
(exchange, product_id, trade_id). There is deliberately no watermark: backfilled trades can
be hours late and must still land. A replayed batch (after a crash) merges to the same result.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from lake.config import LakeConfig
from lake.jobs._runner import run_streaming
from lake.schemas import SILVER_QUARANTINE, SILVER_TRADES
from lake.tables import ensure_table, insert_new_quarantine, insert_new_trades
from lake.transforms import dedupe_trades, parse_and_validate, quarantined, valid_trades

NAME = "silver_trades"
log = logging.getLogger("lake.silver")


def ensure_tables(spark: SparkSession, config: LakeConfig) -> None:
    ensure_table(spark, config.silver_trades, SILVER_TRADES, partition_by="trade_date")
    ensure_table(spark, config.silver_quarantine, SILVER_QUARANTINE)


def process_batch(batch: DataFrame, batch_id: int, config: LakeConfig) -> None:
    spark = batch.sparkSession
    parsed = parse_and_validate(batch).persist()
    try:
        bad = quarantined(parsed)
        bad_count = bad.count()
        if bad_count:
            log.warning("batch %d: %d rows quarantined", batch_id, bad_count)
            insert_new_quarantine(spark, config.silver_quarantine, bad)
        insert_new_trades(spark, config.silver_trades, dedupe_trades(valid_trades(parsed)))
    finally:
        parsed.unpersist()


def start(spark: SparkSession, config: LakeConfig, trigger: dict) -> StreamingQuery:
    ensure_tables(spark, config)
    return (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 500)
        .load(config.bronze_trades)
        .writeStream.foreachBatch(lambda df, batch_id: process_batch(df, batch_id, config))
        .option("checkpointLocation", config.checkpoint(NAME))
        .queryName(NAME)
        .trigger(**trigger)
        .start()
    )


if __name__ == "__main__":
    run_streaming(NAME, start, lambda c: c.silver_trigger)
