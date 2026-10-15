"""Gold job: silver/trades -> gold/ohlcv_1m.

Late data corrects candles. For each micro-batch of new silver rows:
  1. find the (product_id, minute) windows they touch,
  2. recompute those windows from ALL silver trades in them (not just the new rows),
  3. MERGE the result into gold on (product_id, window_start).
Recomputing from silver makes the write idempotent: replaying a batch yields the same candles.

Silver is read as a plain Delta stream (its writer only ever inserts). Change data feed is
not needed because the batch's own rows identify the affected windows.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from lake.config import LakeConfig
from lake.jobs._runner import run_streaming
from lake.schemas import GOLD_OHLCV_1M
from lake.tables import distinct_dates, ensure_table, upsert_candles
from lake.transforms import minute_windows, ohlcv_1m

NAME = "gold_ohlcv_1m"


def process_batch(batch: DataFrame, batch_id: int, config: LakeConfig) -> None:
    spark = batch.sparkSession
    windows = minute_windows(batch).persist()
    try:
        dates = distinct_dates(windows.select(F.to_date("window_start").alias("d")), "d")
        if not dates:
            return
        silver = spark.read.format("delta").load(config.silver_trades).where(F.col("trade_date").isin(dates))
        affected = silver.withColumn("window_start", F.date_trunc("minute", "trade_time")).join(
            F.broadcast(windows), on=["product_id", "window_start"], how="inner"
        )
        upsert_candles(spark, config.gold_ohlcv_1m, ohlcv_1m(affected.drop("window_start")), dates)
    finally:
        windows.unpersist()


def start(spark: SparkSession, config: LakeConfig, trigger: dict) -> StreamingQuery:
    ensure_table(spark, config.gold_ohlcv_1m, GOLD_OHLCV_1M, partition_by="window_date")
    return (
        spark.readStream.format("delta")
        # Silver only receives inserts; skip any other commit type rather than crash the stream
        .option("skipChangeCommits", "true")
        .option("maxFilesPerTrigger", 500)
        .load(config.silver_trades)
        .writeStream.foreachBatch(lambda df, batch_id: process_batch(df, batch_id, config))
        .option("checkpointLocation", config.checkpoint(NAME))
        .queryName(NAME)
        .trigger(**trigger)
        .start()
    )


if __name__ == "__main__":
    run_streaming(NAME, start, lambda c: c.gold_trigger)
