from datetime import UTC, datetime, timedelta

import pytest
from crypto_api.health import (
    IngestRateTracker,
    compute_lag,
    format_duration,
    freshness_seconds,
    freshness_verdict,
    kafka_ingest_verdict,
)
from crypto_api.sources.base import PartitionWatermarks as PW


class TestComputeLag:
    def test_lag_is_high_watermark_minus_next_bronze_offset(self):
        lag = compute_lag([PW(0, 0, 100), PW(1, 0, 50)], {0: 89, 1: 49})
        assert [(p.partition, p.bronze_next_offset, p.lag) for p in lag] == [(0, 90, 10), (1, 50, 0)]

    def test_partition_never_ingested_counts_all_retained_messages(self):
        [lag] = compute_lag([PW(2, 40, 100)], {})
        assert lag.bronze_next_offset is None
        assert lag.lag == 60

    def test_bronze_behind_retention_counts_only_retained_messages(self):
        # Offsets below the low watermark were deleted by retention; they can't be lag anymore
        [lag] = compute_lag([PW(0, 500, 600)], {0: 99})
        assert lag.lag == 100

    def test_lag_never_negative(self):
        # Bronze can briefly see offsets newer than a cached watermark read
        [lag] = compute_lag([PW(0, 0, 100)], {0: 150})
        assert lag.lag == 0

    def test_empty_topic(self):
        assert compute_lag([], {0: 5}) == []


class TestFreshness:
    def test_freshness_seconds(self):
        now = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
        assert freshness_seconds(now - timedelta(seconds=42), now) == 42
        assert freshness_seconds(None, now) is None
        assert freshness_seconds(now + timedelta(seconds=5), now) == 0  # clock skew clamps to 0

    @pytest.mark.parametrize(
        ("age", "status"),
        [(0, "ok"), (299.9, "ok"), (300, "ok"), (300.1, "stale"), (None, "stale")],
    )
    def test_threshold_boundaries(self, age, status):
        assert freshness_verdict(age, stale_after=300).status == status

    def test_reason_mentions_age_and_threshold(self):
        verdict = freshness_verdict(600, stale_after=300)
        assert "10m 0s" in verdict.reason and "5m 0s" in verdict.reason


@pytest.mark.parametrize(
    ("seconds", "text"),
    [(0, "0s"), (59.6, "1m 0s"), (61, "1m 1s"), (3600, "1h 0m"), (90_061, "1d 1h")],
)
def test_format_duration(seconds, text):
    assert format_duration(seconds) == text


class TestIngestRateTracker:
    def test_rate_from_watermark_deltas(self):
        tracker = IngestRateTracker()
        tracker.observe(0, 1_000)
        assert tracker.rate() is None  # one sample isn't a rate
        tracker.observe(10, 1_050)
        assert tracker.rate() == pytest.approx(5.0)

    def test_window_drops_old_samples(self):
        tracker = IngestRateTracker(window_seconds=60)
        tracker.observe(0, 0)
        tracker.observe(100, 1_000)  # slow early period
        tracker.observe(110, 1_200)
        assert tracker.rate() == pytest.approx(20.0)

    def test_stall_reported_only_after_threshold(self):
        tracker = IngestRateTracker()
        tracker.observe(0, 500)
        tracker.observe(60, 500)
        assert kafka_ingest_verdict(tracker, 60, stale_after=120).status == "ok"
        tracker.observe(130, 500)
        verdict = kafka_ingest_verdict(tracker, 130, stale_after=120)
        assert verdict.status == "stale"
        assert "2m 10s" in verdict.reason

    def test_new_messages_clear_stall(self):
        tracker = IngestRateTracker()
        tracker.observe(0, 500)
        tracker.observe(200, 500)
        tracker.observe(205, 510)
        assert kafka_ingest_verdict(tracker, 205, stale_after=120).status == "ok"

    def test_watermarks_going_backwards_reset_tracking(self):
        tracker = IngestRateTracker()
        tracker.observe(0, 5_000)
        tracker.observe(10, 5_100)
        tracker.observe(20, 10)  # topic recreated
        assert tracker.rate() is None
        assert kafka_ingest_verdict(tracker, 20, stale_after=120).reason == "reachable; measuring ingest rate"
