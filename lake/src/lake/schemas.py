"""Table schemas, mirroring docs/CONTRACTS.md. Change both together."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EXCHANGE = "coinbase"
PRICE_TYPE = DecimalType(38, 18)

# JSON value of a raw.trades message. Numbers stay strings here and are converted with
# try_cast during validation, so a bad value quarantines a row instead of failing the batch.
TRADE_JSON_SCHEMA = StructType(
    [
        StructField("type", StringType()),
        StructField("trade_id", StringType()),
        StructField("product_id", StringType()),
        StructField("price", StringType()),
        StructField("size", StringType()),
        StructField("side", StringType()),
        StructField("time", StringType()),
        StructField("source", StringType()),
    ]
)

BRONZE_TRADES = StructType(
    [
        StructField("kafka_key", StringType()),
        StructField("value", StringType()),
        StructField("topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("ingested_at", TimestampType()),
        StructField("ingest_date", DateType()),
    ]
)

SILVER_TRADES = StructType(
    [
        StructField("exchange", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("trade_id", LongType(), nullable=False),
        StructField("price", PRICE_TYPE, nullable=False),
        StructField("size", PRICE_TYPE, nullable=False),
        StructField("side", StringType(), nullable=False),
        StructField("trade_time", TimestampType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("ingested_at", TimestampType()),
        StructField("trade_date", DateType(), nullable=False),
    ]
)

SILVER_QUARANTINE = StructType(
    [
        StructField("value", StringType()),
        StructField("error", StringType(), nullable=False),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("quarantined_at", TimestampType()),
    ]
)

GOLD_OHLCV_1M = StructType(
    [
        StructField("product_id", StringType(), nullable=False),
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("quote_volume", DoubleType()),
        StructField("vwap", DoubleType()),
        StructField("trade_count", LongType()),
        StructField("buy_volume", DoubleType()),
        StructField("sell_volume", DoubleType()),
        StructField("updated_at", TimestampType()),
        StructField("window_date", DateType(), nullable=False),
    ]
)

OPS_TRADE_GAPS = StructType(
    [
        StructField("product_id", StringType(), nullable=False),
        StructField("gap_start_id", LongType(), nullable=False),
        StructField("gap_end_id", LongType(), nullable=False),
        StructField("missing_count", LongType(), nullable=False),
        StructField("detected_at", TimestampType(), nullable=False),
    ]
)
