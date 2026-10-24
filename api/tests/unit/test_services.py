import asyncio
import threading
from datetime import UTC, datetime, timedelta

import pytest
from conftest import FakeKafka, FakeLake
from crypto_api.config import Settings
from crypto_api.runner import SourceRunner
from crypto_api.services import PipelineService
from crypto_api.sources.base import NotDeployedKafka, PartitionWatermarks, TableSnapshot


def make_service(settings, kafka=None, lake=None, timeout=0.5):
    runner = SourceRunner(timeout_seconds=timeout, ttl_seconds=0)
    return PipelineService(settings, kafka or FakeKafka(), lake or FakeLake(), runner)


def stages(response):
    return {s.id: s for s in response.stages}


def tables(response):
    return {t.id: t for t in response.tables}


def test_healthy_pipeline(settings):
    response = asyncio.run(make_service(settings).pipeline())
    by_id = stages(response)
    assert [s.id for s in response.stages] == ["coinbase", "producer", "kafka", "bronze", "silver", "gold", "ml"]
    assert by_id["kafka"].status == "ok"
    assert by_id["bronze"].status == "ok"
    assert response.lag.total_lag == 1  # partition 0: 100 - 99
    assert response.kafka.retention_hours == 168
    assert by_id["coinbase"].status == "inactive"


def test_kafka_down_keeps_lake_stages(settings):
    service = make_service(settings, kafka=FakeKafka(error=ConnectionError("broker unreachable")))
    response = asyncio.run(service.pipeline())
    by_id = stages(response)
    assert by_id["kafka"].status == "down"
    assert "broker unreachable" in by_id["kafka"].reason
    assert by_id["producer"].status == "inactive"
    assert response.lag.status == "down"
    # Lake stages are unaffected, except bronze, which also reports the unknown lag
    assert by_id["silver"].status == "ok"
    assert by_id["gold"].status == "ok"
    assert by_id["bronze"].status == "down"


def test_lake_down_keeps_kafka_stats(settings):
    lake = FakeLake()
    lake.error = OSError("connection refused")
    response = asyncio.run(make_service(settings, lake=lake).pipeline())
    by_id = stages(response)
    assert by_id["kafka"].status == "ok"
    assert response.kafka.partitions
    for stage in ("bronze", "silver", "gold", "ml"):
        assert by_id[stage].status == "down", stage
    assert all("connection refused" in t.reason for t in response.tables)


def test_hung_lake_times_out_without_blocking_kafka(settings):
    release = threading.Event()
    lake = FakeLake()
    lake.hook = lambda: release.wait(5)
    try:
        response = asyncio.run(make_service(settings, lake=lake, timeout=0.2).pipeline())
    finally:
        release.set()
    assert stages(response)["kafka"].status == "ok"
    assert stages(response)["gold"].status == "down"
    assert "timed out" in stages(response)["gold"].reason


def test_missing_tables_core_down_optional_inactive(settings):
    lake = FakeLake()
    lake.missing = {"gold", "predictions", "quarantine", "trade_gaps"}
    response = asyncio.run(make_service(settings, lake=lake).pipeline())
    by_table = tables(response)
    assert by_table["gold"].status == "down" and not by_table["gold"].exists
    assert by_table["predictions"].status == "inactive"
    assert by_table["quarantine"].status == "inactive"
    assert stages(response)["ml"].reason == "ML predictions not available yet"


def test_missing_bronze_counts_whole_topic_as_lag(settings):
    lake = FakeLake()
    lake.missing = {"bronze"}
    response = asyncio.run(make_service(settings, lake=lake).pipeline())
    assert response.lag.total_lag == 150
    assert stages(response)["bronze"].status == "down"


def test_lag_over_threshold_makes_bronze_stale(settings):
    settings.lag_stale_messages = 10
    kafka = FakeKafka(marks=[PartitionWatermarks(0, 0, 1_000)])
    lake = FakeLake()
    lake.offsets = {0: 500}
    response = asyncio.run(make_service(settings, kafka=kafka, lake=lake).pipeline())
    assert response.lag.status == "stale"
    assert stages(response)["bronze"].status == "stale"
    assert "499 messages behind" in stages(response)["bronze"].reason


def test_stale_table(settings):
    lake = FakeLake()
    old = lake.now - timedelta(seconds=settings.gold_stale_seconds + 60)
    lake.snapshots["gold"] = TableSnapshot(3, old, 10, old)
    response = asyncio.run(make_service(settings, lake=lake).pipeline())
    assert stages(response)["gold"].status == "stale"
    assert "threshold" in stages(response)["gold"].reason


def test_market_states(settings):
    lake = FakeLake()
    service = make_service(settings, lake=lake)
    assert asyncio.run(service.market("BTC-USD", 60)).status == "ok"
    lake.missing = {"gold"}
    missing = asyncio.run(make_service(settings, lake=lake).market("BTC-USD", 60))
    assert missing.status == "inactive" and missing.candles == []
    lake.missing, lake.error = set(), OSError("boom")
    down = asyncio.run(make_service(settings, lake=lake).market("BTC-USD", 60))
    assert down.status == "down" and "boom" in down.error


def test_quality_parts_degrade_independently(settings):
    lake = FakeLake()
    lake.missing = {"trade_gaps"}
    quality = asyncio.run(make_service(settings, lake=lake).quality())
    assert quality.quarantine.status == "ok"
    assert [r.error for r in quality.quarantine.reasons] == ["bad side", "missing price"]  # by count desc
    assert quality.gaps.status == "inactive"
    assert quality.backfill.share == pytest.approx(0.025)


class TestPredictions:
    def test_missing_table_is_not_available_yet(self, settings):
        lake = FakeLake()
        lake.missing = {"predictions"}
        response = asyncio.run(make_service(settings, lake=lake).predictions())
        assert response.status == "inactive"
        assert response.reason == "ML predictions not available yet"

    def test_contract_columns(self, settings):
        now = datetime.now(UTC)
        lake = FakeLake()
        lake.prediction_rows = [
            {
                "product_id": "BTC-USD",
                "window_start": now,
                "horizon_minutes": 5,
                "model_run_id": "a1b2c3",
                "target": "volatility",
                "probability": 0.57,
                "predicted_label": 1,
                "is_filled": 0,
                "created_at": now,
            }
        ]
        response = asyncio.run(make_service(settings, lake=lake).predictions())
        assert [(p.product_id, p.score, p.model_version) for p in response.predictions] == [("BTC-USD", 0.57, "a1b2c3")]
        assert (response.predictions[0].target, response.predictions[0].horizon_minutes) == ("volatility", 5)

    def test_tolerates_schema_variations(self, settings):
        now = datetime.now(UTC)
        lake = FakeLake()
        lake.prediction_rows = [
            {"product_id": "BTC-USD", "probability": "0.61", "model": 4, "created_at": now.replace(tzinfo=None)},
            {"product_id": "ETH-USD", "score": None, "window_start": now},
            {"score": 0.5},  # no product: skipped
            {"product_id": "SOL-USD", "score": "not-a-number", "created_at": now},
        ]
        response = asyncio.run(make_service(settings, lake=lake).predictions())
        assert response.status == "ok"
        assert [(p.product_id, p.score, p.model_version) for p in response.predictions] == [
            ("BTC-USD", 0.61, "4"),
            ("ETH-USD", None, None),
            ("SOL-USD", None, None),
        ]
        assert response.predictions[0].created_at.tzinfo is not None

    def test_stale_predictions(self, settings):
        lake = FakeLake()
        lake.prediction_rows = [
            {"product_id": "BTC-USD", "score": 0.5, "created_at": datetime.now(UTC) - timedelta(days=1)}
        ]
        assert asyncio.run(make_service(settings, lake=lake).predictions()).status == "stale"


class TestSnapshotMode:
    SNAPSHOT_AT = datetime(2026, 10, 22, 8, 10, tzinfo=UTC)

    def service(self, lake=None):
        settings = Settings(data_source="real", snapshot_at=self.SNAPSHOT_AT, source_timeout_seconds=0.5)
        return make_service(settings, kafka=NotDeployedKafka(), lake=lake)

    def test_freshness_is_measured_from_the_snapshot_and_kafka_is_not_deployed(self):
        # Data as of 4 minutes before the snapshot, read days later: still fresh
        lake = FakeLake(now=self.SNAPSHOT_AT - timedelta(minutes=4))
        response = asyncio.run(self.service(lake).pipeline())
        by_id = stages(response)
        assert response.generated_at == self.SNAPSHOT_AT
        for stage in ("bronze", "silver", "gold"):
            assert by_id[stage].status == "ok", by_id[stage].reason
        assert tables(response)["gold"].freshness_seconds == 240
        for stage in ("producer", "kafka"):
            assert by_id[stage].status == "inactive"
            assert "not deployed" in by_id[stage].reason
        assert response.lag.status == "inactive" and response.lag.total_lag is None

    def test_old_data_is_still_stale_relative_to_the_snapshot(self):
        lake = FakeLake(now=self.SNAPSHOT_AT - timedelta(hours=2))
        assert stages(asyncio.run(self.service(lake).pipeline()))["gold"].status == "stale"

    def test_predictions_written_after_the_snapshot_are_aged_by_their_minute(self):
        lake = FakeLake(now=self.SNAPSHOT_AT)
        lake.prediction_rows = [
            {
                "product_id": "BTC-USD",
                "window_start": self.SNAPSHOT_AT - timedelta(minutes=24),
                "probability": 0.8,
                "created_at": self.SNAPSHOT_AT + timedelta(days=2),  # scored later from the snapshot's data
            }
        ]
        response = asyncio.run(self.service(lake).predictions())
        assert response.reason == "newest prediction is 24m 0s old"

    def test_market_window_ends_at_the_snapshot(self):
        lake = FakeLake(now=self.SNAPSHOT_AT)
        seen = []
        lake.ohlcv = lambda product_id, since: seen.append(since) or []
        asyncio.run(self.service(lake).market("BTC-USD", 60))
        assert seen == [self.SNAPSHOT_AT - timedelta(minutes=60)]

    def test_meta_exposes_the_snapshot(self):
        assert self.service().meta().snapshot_at == self.SNAPSHOT_AT
