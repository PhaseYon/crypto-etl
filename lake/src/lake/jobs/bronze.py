"""Bronze job: Kafka raw.trades -> bronze/trades (append-only, raw JSON untouched).

Exactly-once: Kafka offsets are tracked in the checkpoint and the Delta sink records each
batch id, so a batch replayed after a crash is not written twice.

failOnDataLoss (FAIL_ON_DATA_LOSS, default true): if the checkpoint points at offsets Kafka
has already deleted (job down longer than topic retention) the job fails loudly instead of
silently skipping. See the runbook in lake/README.md.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from lake.config import LakeConfig
from lake.jobs._runner import run_streaming
from lake.transforms import kafka_to_bronze

NAME = "bronze_trades"


def start(spark: SparkSession, config: LakeConfig, trigger: dict) -> StreamingQuery:
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", "earliest")  # only used on the very first run
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .option("failOnDataLoss", str(config.fail_on_data_loss).lower())
        .option("kafka.request.timeout.ms", "30000")
        .option("kafka.default.api.timeout.ms", "60000")
        .option("fetchOffset.numRetries", "5")
        .load()
    )
    return (
        kafka_to_bronze(source)
        .writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingest_date")
        .option("checkpointLocation", config.checkpoint(NAME))
        .queryName(NAME)
        .trigger(**trigger)
        .start(config.bronze_trades)
    )


if __name__ == "__main__":
    run_streaming(NAME, start, lambda c: c.bronze_trigger)
