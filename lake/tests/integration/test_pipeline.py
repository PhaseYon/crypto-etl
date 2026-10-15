"""End to end: Kafka -> bronze -> silver -> gold on the object store, with restarts."""

import datetime as dt

import pytest

from lake.jobs import bronze, gold, silver

from .kafka_helpers import produce, trade

pytestmark = pytest.mark.integration


def run_all(spark, config):
    for job in (bronze, silver, gold):
        query = job.start(spark, config, {"availableNow": True})
        query.awaitTermination()
        assert query.exception() is None, f"{job.NAME} failed: {query.exception()}"


def load(spark, path):
    return spark.read.format("delta").load(path).collect()


def test_pipeline_end_to_end_with_duplicates_bad_data_late_backfill_and_restarts(spark, pipeline_config):
    cfg = pipeline_config
    produce(
        cfg.kafka_bootstrap,
        cfg.kafka_topic,
        [
            trade("BTC-USD", 1, "100", "1", "2026-10-15T09:14:10Z"),
            trade("BTC-USD", 2, "110", "2", "2026-10-15T09:14:40Z", side="sell"),
            trade("BTC-USD", 3, "120", "1", "2026-10-15T09:15:05Z"),
            trade("BTC-USD", 3, "120", "1", "2026-10-15T09:15:05Z"),  # duplicate (e.g. reconnect)
            trade("BTC-USD", 4, "115", "1", "2026-10-15T09:15:50Z"),
            trade("ETH-USD", 1, "2700", "0.5", "2026-10-15T09:14:30Z"),
            "{this is not json",
            {"type": "heartbeat", "product_id": "BTC-USD"},
        ],
    )
    run_all(spark, cfg)

    silver_rows = load(spark, cfg.silver_trades)
    assert sorted((r.product_id, r.trade_id) for r in silver_rows) == [
        ("BTC-USD", 1),
        ("BTC-USD", 2),
        ("BTC-USD", 3),
        ("BTC-USD", 4),
        ("ETH-USD", 1),
    ]
    assert sorted(r.error for r in load(spark, cfg.silver_quarantine)) == ["invalid_json", "unexpected_type"]

    candles = {(r.product_id, r.window_start): r for r in load(spark, cfg.gold_ohlcv_1m)}
    m14, m15 = dt.datetime(2026, 10, 15, 9, 14), dt.datetime(2026, 10, 15, 9, 15)
    assert set(candles) == {("BTC-USD", m14), ("BTC-USD", m15), ("ETH-USD", m14)}
    btc14 = candles[("BTC-USD", m14)]
    assert (btc14.open, btc14.high, btc14.low, btc14.close, btc14.trade_count) == (100.0, 110.0, 100.0, 110.0, 2)
    assert candles[("BTC-USD", m15)].trade_count == 2  # duplicate of trade 3 not double counted

    # Restart: a late backfilled trade that is the new open of 09:14, plus a re-delivered trade
    produce(
        cfg.kafka_bootstrap,
        cfg.kafka_topic,
        [
            trade("BTC-USD", 0, "95", "3", "2026-10-15T09:14:01Z", source="rest_backfill"),
            trade("BTC-USD", 4, "115", "1", "2026-10-15T09:15:50Z"),
        ],
    )
    run_all(spark, cfg)
    run_all(spark, cfg)  # nothing new: must change nothing

    assert len(load(spark, cfg.bronze_trades)) == 10  # every Kafka message exactly once
    silver_rows = load(spark, cfg.silver_trades)
    assert len(silver_rows) == 6
    assert {r.trade_id: r.source for r in silver_rows if r.product_id == "BTC-USD"}[0] == "rest_backfill"
    assert len(load(spark, cfg.silver_quarantine)) == 2

    candles = {(r.product_id, r.window_start): r for r in load(spark, cfg.gold_ohlcv_1m)}
    assert len(candles) == 3
    btc14 = candles[("BTC-USD", m14)]
    assert (btc14.open, btc14.low, btc14.close, btc14.trade_count) == (95.0, 95.0, 110.0, 3)
    assert btc14.volume == pytest.approx(6.0)
    assert candles[("BTC-USD", m15)].trade_count == 2
