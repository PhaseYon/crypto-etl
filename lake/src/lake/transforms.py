"""Pure DataFrame transformations (no reads or writes), unit-tested in isolation."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from lake.schemas import EXCHANGE, PRICE_TYPE, TRADE_JSON_SCHEMA

# ---------------------------------------------------------------- bronze


def kafka_to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Kafka source rows -> bronze/trades rows. The value is kept as raw JSON text."""
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    ).withColumn("ingest_date", F.to_date("ingested_at"))


# ---------------------------------------------------------------- silver


def _validation_error(j: Column, trade_id: Column, price: Column, size: Column, side: Column, ts: Column) -> Column:
    """First failed check as a short reason, or null if the trade is valid."""
    all_missing = j.isNull() | (j["type"].isNull() & j["product_id"].isNull() & j["trade_id"].isNull())
    return (
        F.when(all_missing, F.lit("invalid_json"))
        .when(j["type"] != F.lit("match"), F.lit("unexpected_type"))
        .when(j["product_id"].isNull() | (F.trim(j["product_id"]) == ""), F.lit("missing_product_id"))
        .when(trade_id.isNull(), F.lit("missing_or_invalid_trade_id"))
        .when(trade_id < 0, F.lit("negative_trade_id"))
        .when(price.isNull(), F.lit("missing_or_invalid_price"))
        .when(price <= 0, F.lit("non_positive_price"))
        .when(size.isNull(), F.lit("missing_or_invalid_size"))
        .when(size <= 0, F.lit("non_positive_size"))
        .when(side.isNull() | ~side.isin("buy", "sell"), F.lit("invalid_side"))
        .when(ts.isNull(), F.lit("missing_or_invalid_time"))
    )


def parse_and_validate(bronze_df: DataFrame) -> DataFrame:
    """Parse bronze JSON into typed columns plus an `error` column (null = valid).

    Uses try_* conversions so one bad message never fails a whole batch (Spark 4 runs in
    ANSI mode, where a plain cast would raise).
    """
    j = F.from_json(F.col("value"), TRADE_JSON_SCHEMA)
    parsed = bronze_df.select("value", "kafka_partition", "kafka_offset", "ingested_at", j.alias("j"))
    j = F.col("j")
    trade_id = j["trade_id"].try_cast("long")
    price = j["price"].try_cast(PRICE_TYPE)
    size = j["size"].try_cast(PRICE_TYPE)
    side = F.lower(F.trim(j["side"]))
    ts = F.try_to_timestamp(j["time"])
    return parsed.select(
        "value",
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
        F.lit(EXCHANGE).alias("exchange"),
        F.trim(j["product_id"]).alias("product_id"),
        trade_id.alias("trade_id"),
        price.alias("price"),
        size.alias("size"),
        side.alias("side"),
        ts.alias("trade_time"),
        F.coalesce(j["source"], F.lit("ws")).alias("source"),
        _validation_error(j, trade_id, price, size, side, ts).alias("error"),
    )


def valid_trades(parsed: DataFrame) -> DataFrame:
    """Valid rows in the silver/trades schema."""
    return parsed.where(F.col("error").isNull()).select(
        "exchange",
        "product_id",
        "trade_id",
        "price",
        "size",
        "side",
        "trade_time",
        "source",
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
        F.to_date("trade_time").alias("trade_date"),
    )


def quarantined(parsed: DataFrame) -> DataFrame:
    """Invalid rows in the silver/trades_quarantine schema."""
    return parsed.where(F.col("error").isNotNull()).select(
        "value",
        "error",
        "kafka_partition",
        "kafka_offset",
        F.current_timestamp().alias("quarantined_at"),
    )


def dedupe_trades(trades: DataFrame) -> DataFrame:
    """One row per (exchange, product_id, trade_id).

    Deterministic winner: live WebSocket copy over REST backfill, then earliest Kafka position.
    """
    order = Window.partitionBy("exchange", "product_id", "trade_id").orderBy(
        F.when(F.col("source") == "ws", 0).otherwise(1),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
    )
    return trades.withColumn("_rn", F.row_number().over(order)).where(F.col("_rn") == 1).drop("_rn")


# ---------------------------------------------------------------- gold


def minute_windows(trades: DataFrame) -> DataFrame:
    """Distinct (product_id, window_start) minutes touched by these trades."""
    return trades.select("product_id", F.date_trunc("minute", "trade_time").alias("window_start")).distinct()


def ohlcv_1m(trades: DataFrame) -> DataFrame:
    """1-minute candles from silver trades. Open/close by trade_time, tie-broken by trade_id."""
    price = F.col("price").cast("double")
    size = F.col("size").cast("double")
    order = F.struct("trade_time", "trade_id")
    candles = (
        trades.withColumn("window_start", F.date_trunc("minute", "trade_time"))
        .groupBy("product_id", "window_start")
        .agg(
            F.min_by(price, order).alias("open"),
            F.max(price).alias("high"),
            F.min(price).alias("low"),
            F.max_by(price, order).alias("close"),
            F.sum(size).alias("volume"),
            F.sum(price * size).alias("quote_volume"),
            F.count(F.lit(1)).alias("trade_count"),
            F.sum(F.when(F.col("side") == "buy", size).otherwise(0.0)).alias("buy_volume"),
            F.sum(F.when(F.col("side") == "sell", size).otherwise(0.0)).alias("sell_volume"),
        )
    )
    return candles.select(
        "product_id",
        "window_start",
        (F.col("window_start") + F.expr("INTERVAL 1 MINUTE")).alias("window_end"),
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        F.when(F.col("volume") > 0, F.col("quote_volume") / F.col("volume")).alias("vwap"),
        "trade_count",
        "buy_volume",
        "sell_volume",
        F.current_timestamp().alias("updated_at"),
        F.to_date("window_start").alias("window_date"),
    )


# ---------------------------------------------------------------- data quality


def trade_id_gaps(trades: DataFrame) -> DataFrame:
    """Missing trade_id ranges per product (inclusive bounds), within the given trades."""
    ids = trades.select("product_id", "trade_id").distinct()
    w = Window.partitionBy("product_id").orderBy("trade_id")
    with_next = ids.withColumn("next_id", F.lead("trade_id").over(w))
    return with_next.where(F.col("next_id") - F.col("trade_id") > 1).select(
        "product_id",
        (F.col("trade_id") + 1).alias("gap_start_id"),
        (F.col("next_id") - 1).alias("gap_end_id"),
        (F.col("next_id") - F.col("trade_id") - 1).alias("missing_count"),
        F.current_timestamp().alias("detected_at"),
    )
