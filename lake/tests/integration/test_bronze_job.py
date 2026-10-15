import pytest

from lake.jobs import bronze

from .kafka_helpers import produce, trade

pytestmark = pytest.mark.integration


def run_bronze(spark, config):
    query = bronze.start(spark, config, {"availableNow": True})
    query.awaitTermination()
    assert query.exception() is None


def test_bronze_lands_every_message_once_across_restarts(spark, pipeline_config):
    t = "2026-10-01T09:14:26.443591Z"
    first = [trade("BTC-USD", i, "84000.1", "0.01", t) for i in range(1, 6)] + ["not json at all"]
    produce(pipeline_config.kafka_bootstrap, pipeline_config.kafka_topic, first)
    run_bronze(spark, pipeline_config)

    # Second run from the same checkpoint only picks up the new messages
    produce(pipeline_config.kafka_bootstrap, pipeline_config.kafka_topic, [trade("ETH-USD", 7, "2700", "1", t)])
    run_bronze(spark, pipeline_config)
    run_bronze(spark, pipeline_config)  # nothing new: must not duplicate

    rows = spark.read.format("delta").load(pipeline_config.bronze_trades).collect()
    assert len(rows) == 7
    assert len({(r.kafka_partition, r.kafka_offset) for r in rows}) == 7
    assert "not json at all" in {r.value for r in rows}
    assert {r.kafka_key for r in rows} == {"BTC-USD", "ETH-USD", None}
    assert all(r.topic == pipeline_config.kafka_topic for r in rows)
