import datetime as dt

from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from lake.schemas import BRONZE_TRADES
from lake.transforms import kafka_to_bronze

KAFKA_SOURCE_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


def test_kafka_rows_become_bronze_rows_with_raw_value(spark):
    ts = dt.datetime(2026, 10, 1, 9, 14, 26)
    raw = '{"type":"match","trade_id":1,"product_id":"BTC-USD"}'
    kafka = spark.createDataFrame(
        [(b"BTC-USD", raw.encode(), "raw.trades", 2, 76, ts), (None, None, "raw.trades", 0, 1, ts)],
        KAFKA_SOURCE_SCHEMA,
    )
    bronze = kafka_to_bronze(kafka)

    assert [(f.name, f.dataType) for f in bronze.schema] == [(f.name, f.dataType) for f in BRONZE_TRADES]
    first, tombstone = sorted(bronze.collect(), key=lambda r: r.kafka_offset, reverse=True)
    assert first.kafka_key == "BTC-USD"
    assert first.value == raw  # untouched
    assert (first.kafka_partition, first.kafka_offset, first.kafka_timestamp) == (2, 76, ts)
    assert first.ingest_date == first.ingested_at.date()
    assert tombstone.kafka_key is None and tombstone.value is None
