import asyncio
import json

import pytest
from crypto_producer.backfill import run_backfill, take_in_range
from crypto_producer.gaps import BackfillJob
from fakes import make_fetch


async def backfill(job, fetch, max_trades=100_000, retries=2):
    emitted = []
    ok = await run_backfill(
        job, fetch, emitted.append, max_trades=max_trades, retries=retries, page_pause=0, retry_delay=0
    )
    return ok, emitted


@pytest.mark.asyncio
async def test_fills_the_exact_range_across_pages():
    fetch = make_fetch(range(1, 10_000), page_size=1000)
    ok, emitted = await backfill(BackfillJob("BTC-USD", lo=100, hi=2600), fetch)
    assert ok
    assert sorted(r["trade_id"] for r in emitted) == list(range(101, 2600))
    assert fetch.calls == [2600, 1600, 600]


@pytest.mark.asyncio
async def test_records_are_normalized_like_live_matches():
    _, emitted = await backfill(BackfillJob("ETH-USD", lo=1, hi=3), make_fetch(range(1, 10)))
    assert emitted == [
        {
            "type": "match",
            "product_id": "ETH-USD",
            "source": "rest_backfill",
            "trade_id": 2,
            "side": "buy",
            "size": "1",
            "price": "10",
            "time": "t",
        }
    ]
    json.dumps(emitted)  # must be serializable for Kafka


@pytest.mark.asyncio
async def test_stops_at_lo_without_emitting_older_trades():
    _, emitted = await backfill(BackfillJob("BTC-USD", lo=500, hi=510), make_fetch(range(1, 1000)))
    assert min(r["trade_id"] for r in emitted) == 501


@pytest.mark.asyncio
async def test_empty_page_reports_incomplete():
    job = BackfillJob("BTC-USD", lo=100, hi=200)
    ok, emitted = await backfill(job, make_fetch(range(150, 1000)))  # nothing below 150 exists
    assert not ok
    assert len(emitted) == 50
    assert job.unfetched() == (101, 149)


@pytest.mark.asyncio
async def test_gap_over_the_cap_is_skipped_without_fetching():
    fetch = make_fetch(range(1, 1000))
    ok, emitted = await backfill(BackfillJob("BTC-USD", lo=0, hi=500), fetch, max_trades=100)
    assert not ok and emitted == [] and fetch.calls == []


@pytest.mark.asyncio
async def test_transient_fetch_errors_are_retried():
    fetch = make_fetch(range(1, 1000), fail_times=2)
    ok, emitted = await backfill(BackfillJob("BTC-USD", lo=10, hi=20), fetch, retries=2)
    assert ok and len(emitted) == 9
    assert len(fetch.calls) == 3


@pytest.mark.asyncio
async def test_persistent_fetch_errors_fail_and_keep_progress():
    fetch = make_fetch(range(1, 1000), fail_times=99)
    job = BackfillJob("BTC-USD", lo=10, hi=20)
    ok, emitted = await backfill(job, fetch, retries=2)
    assert not ok and emitted == []
    assert job.unfetched() == (11, 19)


@pytest.mark.asyncio
async def test_cancellation_leaves_accurate_progress():
    fetch = make_fetch(range(1, 10_000), page_size=100, delay=0.05)
    job = BackfillJob("BTC-USD", lo=0, hi=1001)
    task = asyncio.create_task(run_backfill(job, fetch, lambda r: None, max_trades=10_000, page_pause=0))
    await asyncio.sleep(0.18)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    first, last = job.unfetched()
    assert first == 1 and 1 <= last < 1000
    assert last == job.cursor - 1


def test_take_in_range_stops_at_first_old_trade():
    page = [{"trade_id": i} for i in (9, 8, 7, 6)]
    assert [t["trade_id"] for t in take_in_range(page, lo=7)] == [9, 8]
