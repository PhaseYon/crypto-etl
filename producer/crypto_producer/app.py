"""Producer entrypoint: wire the stream, backfills and Kafka together, and shut down gracefully."""

import asyncio
import contextlib
import json
import logging
import os
import signal
import time
from pathlib import Path

from confluent_kafka import Producer

from .backfill import run_backfill
from .coinbase import fetch_trades_before
from .config import Settings
from .gaps import BackfillJob
from .kafka_io import DeliveryReporter, load_last_trade_ids, make_producer, produce
from .stream import TradeStream

log = logging.getLogger(__name__)


class Backfills:
    """Running backfill tasks, keyed to their jobs so a shutdown can report what's unfinished."""

    def __init__(self, settings: Settings, producer: Producer, reporter: DeliveryReporter):
        self.settings = settings
        self.producer = producer
        self.reporter = reporter
        self.running: dict[asyncio.Task, BackfillJob] = {}

    def _emit(self, record: dict):
        produce(self.producer, self.settings.topic, record["product_id"], json.dumps(record), self.reporter)

    def _fetch(self, product: str, before_id: int) -> list[dict]:
        return fetch_trades_before(self.settings.rest_url, product, before_id, self.settings.rest_timeout)

    def start(self, job: BackfillJob):
        coro = run_backfill(
            job,
            self._fetch,
            self._emit,
            max_trades=self.settings.max_backfill_trades,
            retries=self.settings.rest_retries,
        )
        task = asyncio.create_task(coro)
        self.running[task] = job
        task.add_done_callback(lambda t: self.running.pop(t, None))

    async def drain(self, timeout: float):
        """Give running backfills up to `timeout` seconds, then cancel them and log what they didn't fetch."""
        if not self.running:
            return
        jobs = dict(self.running)
        log.info("waiting up to %.0fs for %d running backfill(s)", timeout, len(jobs))
        _, pending = await asyncio.wait(jobs, timeout=timeout)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in pending:
            if gap := jobs[task].unfetched():
                log.error("backfill cut off by shutdown: %s trade_id %d..%d not fetched", jobs[task].product, *gap)


class HealthFile:
    """Touches a file (at most every few seconds) while the feed is alive, for container healthchecks."""

    def __init__(self, path: str, min_interval: float = 5.0):
        self.path = Path(path) if path else None
        self.min_interval = min_interval
        # None = never touched. Not 0.0: monotonic time counts from boot, so on a freshly
        # booted host (e.g. a CI VM) `now - 0.0` can be under the interval and skip the first write.
        self._last: float | None = None

    def touch(self):
        now = time.monotonic()
        if self.path and (self._last is None or now - self._last >= self.min_interval):
            self._last = now
            self.path.touch()


async def poll_loop(producer: Producer, interval: float):
    """Serve delivery callbacks even when no trades arrive (quiet markets, during shutdown waits)."""
    while True:
        producer.poll(0)
        await asyncio.sleep(interval)


async def main(settings: Settings):
    producer = make_producer(settings)
    reporter = DeliveryReporter()
    backfills = Backfills(settings, producer, reporter)
    health = HealthFile(settings.health_file)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def on_signal():
        if stop.is_set():
            log.warning("second signal received, exiting immediately without flushing")
            os._exit(130)
        log.info("shutdown requested (send again to force exit)")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, on_signal)

    # Resume from what's already in Kafka so a gap across restarts is backfilled.
    # In a thread so signals are still handled while Kafka is slow to answer.
    last_trade_id = await asyncio.to_thread(load_last_trade_ids, settings)
    log.info("resuming from last trade_ids in Kafka: %s", last_trade_id or "none found")

    def emit_raw(product: str, raw: str):
        produce(producer, settings.topic, product, raw, reporter)

    stream = TradeStream(settings, last_trade_id, emit_raw, backfills.start, on_alive=health.touch)
    tasks = [asyncio.create_task(stream.run()), asyncio.create_task(poll_loop(producer, settings.poll_interval))]
    await stop.wait()

    # 1. Stop reading new trades
    tasks[0].cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await tasks[0]
    # 2. Let in-flight backfills finish, within a deadline
    await backfills.drain(settings.shutdown_backfill_timeout)
    # 3. Deliver what's queued. In a thread so a second signal can still force exit.
    tasks[1].cancel()
    log.info("flushing %d pending messages", len(producer))
    undelivered = await asyncio.to_thread(producer.flush, settings.shutdown_flush_timeout)
    if undelivered:
        log.error("shutdown: %d messages were not delivered", undelivered)
    log.info("stopped: %d delivered, %d failed", reporter.delivered, reporter.failed)


def run():
    settings = Settings.from_env()
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(main(settings))
