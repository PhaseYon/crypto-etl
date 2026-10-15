import asyncio
import logging

import pytest
from crypto_producer.app import Backfills, HealthFile
from crypto_producer.config import Settings
from crypto_producer.gaps import BackfillJob
from crypto_producer.kafka_io import DeliveryReporter
from fakes import FakeProducer, make_fetch


def make_backfills(fetch) -> tuple[Backfills, FakeProducer]:
    producer = FakeProducer()
    backfills = Backfills(Settings(), producer, DeliveryReporter())
    backfills._fetch = fetch
    return backfills, producer


@pytest.mark.asyncio
async def test_drain_waits_for_a_backfill_that_finishes_in_time(caplog):
    backfills, producer = make_backfills(make_fetch(range(1, 1000), delay=0.05))
    backfills.start(BackfillJob("BTC-USD", lo=10, hi=20))
    await backfills.drain(timeout=5)
    assert len(producer.messages) == 9
    assert backfills.running == {}
    assert "cut off" not in caplog.text


@pytest.mark.asyncio
async def test_drain_cancels_a_slow_backfill_and_logs_the_unfetched_range(caplog):
    caplog.set_level(logging.ERROR)
    # 100 trades per page, 0.2s per page: 1000 trades can't finish in 0.5s
    backfills, producer = make_backfills(make_fetch(range(1, 10_000), page_size=100, delay=0.2))
    backfills.start(BackfillJob("BTC-USD", lo=0, hi=1001))
    await backfills.drain(timeout=0.5)
    await asyncio.sleep(0)
    assert backfills.running == {}
    produced = sorted(int(v.split('"trade_id": ')[1].split(",")[0]) for _, _, v in producer.messages)
    first_missing, last_missing = 1, produced[0] - 1
    assert f"BTC-USD trade_id {first_missing}..{last_missing} not fetched" in caplog.text


@pytest.mark.asyncio
async def test_drain_with_nothing_running_returns_immediately():
    backfills, _ = make_backfills(make_fetch(range(1, 2)))
    await asyncio.wait_for(backfills.drain(timeout=10), timeout=0.1)


def test_health_file_is_touched_at_most_every_interval(tmp_path):
    path = tmp_path / "healthy"
    health = HealthFile(str(path), min_interval=60)
    health.touch()
    assert path.exists()
    path.unlink()
    health.touch()  # within the interval: no write
    assert not path.exists()


def test_health_file_disabled_when_no_path():
    HealthFile("").touch()  # must not raise
