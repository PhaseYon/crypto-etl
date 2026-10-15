"""DeltaLakeSource against real Delta tables on the local filesystem (no network)."""

from datetime import UTC, datetime, timedelta

import pytest
from crypto_api.sources.base import TABLE_SPECS, TableMissingError
from crypto_api.sources.lake import DeltaLakeSource
from lake_fixtures import utc_minute, write_bronze, write_gaps, write_gold, write_quarantine, write_silver

NOW = datetime.now(UTC).replace(microsecond=0)


@pytest.fixture
def root(tmp_path):
    return str(tmp_path)


@pytest.fixture
def lake(root):
    return DeltaLakeSource(root, {})


def test_missing_table_raises(lake):
    with pytest.raises(TableMissingError):
        lake.table_snapshot(TABLE_SPECS["gold"])


def test_snapshot_uses_log_stats(root, lake):
    write_bronze(root, [(0, 1), (0, 2)], NOW - timedelta(minutes=5))
    write_bronze(root, [(1, 7)], NOW)
    snap = lake.table_snapshot(TABLE_SPECS["bronze"])
    assert snap.version == 1
    assert snap.row_count == 3
    assert snap.newest_at == NOW
    assert snap.last_commit_at is not None and snap.last_commit_at.tzinfo is not None


def test_bronze_offsets_from_single_partition_files(root, lake):
    write_bronze(root, [(0, 10), (0, 11)], NOW)
    write_bronze(root, [(1, 3)], NOW)
    write_bronze(root, [(0, 12)], NOW)
    assert lake.bronze_max_offsets() == {0: 12, 1: 3}


def test_bronze_offsets_with_mixed_partition_files(root, lake):
    write_bronze(root, [(0, 5), (1, 9), (2, 1)], NOW)  # one file spanning partitions
    write_bronze(root, [(0, 6)], NOW)
    assert lake.bronze_max_offsets() == {0: 6, 1: 9, 2: 1}


def test_ohlcv_filters_product_and_time(root, lake):
    write_gold(
        root,
        {"BTC-USD": [utc_minute(m, NOW) for m in (90, 30, 2, 1)], "ETH-USD": [utc_minute(1, NOW)]},
        NOW,
    )
    candles = lake.ohlcv("BTC-USD", NOW - timedelta(minutes=45))
    assert [c.window_start for c in candles] == [utc_minute(m, NOW) for m in (30, 2, 1)]
    assert candles[0].trade_count == 12


def test_ohlcv_across_day_partitions(root, lake):
    yesterday = utc_minute(0, NOW) - timedelta(days=1)
    write_gold(root, {"BTC-USD": [yesterday, utc_minute(1, NOW)]}, NOW)
    assert len(lake.ohlcv("BTC-USD", yesterday - timedelta(minutes=1))) == 2


def test_quarantine_reasons_and_recent(root, lake):
    write_quarantine(root, ["bad side", "missing price", "bad side"], NOW)
    data = lake.quarantine(recent_limit=2)
    assert data.total == 3
    assert data.reasons == {"bad side": 2, "missing price": 1}
    assert [r.quarantined_at for r in data.recent] == [NOW, NOW - timedelta(minutes=1)]
    assert len(data.recent[0].value) == 300  # previews are truncated


def test_latest_gaps_keeps_only_newest_scan(root, lake):
    write_gaps(
        root,
        [
            ("BTC-USD", 1, 5, NOW - timedelta(hours=2)),  # older scan
            ("SOL-USD", 50, 59, NOW - timedelta(seconds=10)),
            ("BTC-USD", 20, 29, NOW),
        ],
    )
    gaps = lake.latest_gaps()
    assert [(g.product_id, g.gap_start_id) for g in gaps] == [("BTC-USD", 20), ("SOL-USD", 50)]
    assert gaps[0].missing_count == 10


def test_backfill_counts(root, lake):
    write_silver(root, ["ws"] * 8 + ["rest_backfill"] * 2, NOW)
    counts = lake.backfill_counts(NOW.date())
    assert (counts.total, counts.backfilled) == (10, 2)
    assert lake.backfill_counts(NOW.date() + timedelta(days=1)).total == 0
