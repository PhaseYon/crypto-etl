"""Stream trades from the Coinbase Exchange WebSocket into Kafka.

Subscribes to the public `matches` channel and publishes each trade, unmodified,
to a Kafka topic keyed by product_id (so each pair's trades stay ordered).

The WebSocket is a live feed with no replay, so trades missed while disconnected
are detected by a jump in trade_id and backfilled from the REST API. On startup
the last trade_id per product is read back from Kafka, so gaps across producer
restarts are caught too.
"""

import asyncio
import contextlib
import json
import logging
import os
import random
import signal
import time
import urllib.request
from dataclasses import dataclass

import websockets
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

COINBASE_WS_URL = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
COINBASE_REST_URL = os.getenv("COINBASE_REST_URL", "https://api.exchange.coinbase.com")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw.trades")
PRODUCTS = os.getenv("PRODUCTS", "BTC-USD,ETH-USD,SOL-USD").split(",")
MAX_BACKFILL_TRADES = int(os.getenv("MAX_BACKFILL_TRADES", "100000"))
STARTUP_SCAN_MESSAGES = int(os.getenv("STARTUP_SCAN_MESSAGES", "5000"))
# Shutdown budget: backfill wait + flush must fit inside Docker's stop_grace_period
SHUTDOWN_BACKFILL_TIMEOUT = float(os.getenv("SHUTDOWN_BACKFILL_TIMEOUT", "5"))
SHUTDOWN_FLUSH_TIMEOUT = float(os.getenv("SHUTDOWN_FLUSH_TIMEOUT", "10"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

delivered = 0


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,   # no duplicates from producer retries
        "acks": "all",                # wait for the broker to persist each write
        "linger.ms": 50,              # batch small messages for up to 50ms
        "compression.type": "lz4",
    })


def on_delivery(err, msg):
    global delivered
    if err:
        log.error("delivery failed for key=%s: %s", msg.key(), err)
        return
    delivered += 1
    if delivered % 100 == 0:
        log.info("delivered %d trades (last: %s partition=%d offset=%d)",
                 delivered, msg.key().decode(), msg.partition(), msg.offset())


def load_last_trade_ids() -> dict[str, int]:
    """Highest trade_id per product found in the tail of each partition of TOPIC."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "producer-startup-scan",
        "enable.auto.commit": False,
    })
    last: dict[str, int] = {}
    try:
        partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        remaining = {}  # partition -> offset of its last message
        assignments = []
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
            if high > low:
                remaining[p] = high - 1
                assignments.append(TopicPartition(TOPIC, p, max(low, high - STARTUP_SCAN_MESSAGES)))
        consumer.assign(assignments)

        # Short polls under an overall deadline: a long consume() timeout would wait for a
        # full batch even when only a few messages are left
        deadline = time.monotonic() + 15
        while remaining:
            if time.monotonic() > deadline:
                log.warning("startup scan timed out, %d partitions unread", len(remaining))
                break
            for msg in consumer.consume(num_messages=500, timeout=0.2):
                if msg.error():
                    continue
                trade = json.loads(msg.value())
                product = trade.get("product_id")
                if product in PRODUCTS:
                    last[product] = max(last.get(product, 0), trade["trade_id"])
                if msg.offset() >= remaining.get(msg.partition(), float("inf")):
                    del remaining[msg.partition()]
    except KafkaException as e:
        log.warning("could not read last trade_ids from Kafka (%s), starting without them", e)
    finally:
        consumer.close()
    return last


def fetch_trades_before(product: str, trade_id: int) -> list[dict]:
    """Up to 1000 trades older than trade_id, newest first. Blocking HTTP call."""
    url = f"{COINBASE_REST_URL}/products/{product}/trades?limit=1000&after={trade_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "crypto-etl-producer"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.load(resp)


@dataclass
class BackfillJob:
    """Progress of one backfill, so a cut-off job can report what it didn't fetch."""

    product: str
    lo: int  # last trade_id before the gap
    hi: int  # first trade_id after the gap
    cursor: int  # cursor..hi-1 has been produced (pages are fetched newest-first)

    def unfetched(self) -> tuple[int, int] | None:
        """Inclusive trade_id range not yet produced, or None if complete."""
        return (self.lo + 1, self.cursor - 1) if self.cursor > self.lo + 1 else None


async def backfill(producer: Producer, job: BackfillJob):
    """Produce the trades with lo < trade_id < hi, fetched from the REST API."""
    product, lo, hi = job.product, job.lo, job.hi
    missing = hi - lo - 1
    if missing > MAX_BACKFILL_TRADES:
        log.error("gap too large to backfill: %s trade_id %d..%d (%d trades)", product, lo + 1, hi - 1, missing)
        return
    log.warning("gap detected: %s missing %d trades (%d..%d), backfilling", product, missing, lo + 1, hi - 1)

    sent = 0
    try:
        while job.cursor > lo + 1:
            # Blocking HTTP runs in a worker thread so the live stream keeps flowing
            trades = await asyncio.to_thread(fetch_trades_before, product, job.cursor)
            if not trades:
                break
            for trade in trades:
                if trade["trade_id"] <= lo:
                    break
                # REST trades lack these fields; add them so they parse like live matches
                record = {"type": "match", "product_id": product, "source": "rest_backfill", **trade}
                producer.produce(TOPIC, key=product, value=json.dumps(record), on_delivery=on_delivery)
                sent += 1
            producer.poll(0)
            job.cursor = max(trades[-1]["trade_id"], lo + 1)
            await asyncio.sleep(0.25)  # stay well under the public REST rate limit
    except Exception:
        log.exception("backfill failed for %s after %d of %d trades (%d..%d)", product, sent, missing, lo + 1, hi - 1)
        return
    log.info("backfill done: %s sent %d of %d missing trades", product, sent, missing)


async def stream_trades(
    producer: Producer, last_trade_id: dict[str, int], backfills: dict[asyncio.Task, BackfillJob]
):
    """Stream live trades until cancelled."""
    subscribe = {"type": "subscribe", "product_ids": PRODUCTS, "channels": ["matches"]}
    backoff = 1

    while True:
        try:
            async with websockets.connect(COINBASE_WS_URL, ping_interval=20) as ws:
                # Coinbase drops connections that don't subscribe within ~5s
                await ws.send(json.dumps(subscribe))
                log.info("connected, subscribed to %s", PRODUCTS)
                backoff = 1

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg["type"] == "match":
                        product, trade_id = msg["product_id"], msg["trade_id"]
                        prev = last_trade_id.get(product)
                        # trade_id increments by 1 per product, so a jump means missed trades
                        if prev is not None and trade_id > prev + 1:
                            job = BackfillJob(product, lo=prev, hi=trade_id, cursor=trade_id)
                            task = asyncio.create_task(backfill(producer, job))
                            backfills[task] = job
                            task.add_done_callback(lambda t: backfills.pop(t, None))
                        last_trade_id[product] = max(prev or 0, trade_id)

                        producer.produce(TOPIC, key=product, value=raw, on_delivery=on_delivery)
                        producer.poll(0)  # serve delivery callbacks without blocking
                    elif msg["type"] == "error":
                        log.error("coinbase error: %s", msg)
            log.warning("connection closed by server, reconnecting")
        except Exception as e:
            delay = backoff + random.uniform(0, 1)  # jitter
            if isinstance(e, (websockets.WebSocketException, OSError)):
                log.warning("connection lost (%s: %s), reconnecting in %.1fs", type(e).__name__, e, delay)
            else:
                log.exception("unexpected error, reconnecting in %.1fs", delay)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, 60)


async def wait_for_backfills(backfills: dict[asyncio.Task, BackfillJob], timeout: float):
    """Give running backfills up to `timeout` seconds, then cancel them and log what they didn't fetch."""
    if not backfills:
        return
    jobs = dict(backfills)
    log.info("waiting up to %.0fs for %d running backfill(s)", timeout, len(jobs))
    _, pending = await asyncio.wait(jobs, timeout=timeout)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in pending:
        if gap := jobs[task].unfetched():
            log.error("backfill cut off by shutdown: %s trade_id %d..%d not fetched", jobs[task].product, *gap)


async def main():
    producer = make_producer()
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
    last_trade_id = await asyncio.to_thread(load_last_trade_ids)
    log.info("resuming from last trade_ids in Kafka: %s", last_trade_id or "none found")

    backfills: dict[asyncio.Task, BackfillJob] = {}
    stream = asyncio.create_task(stream_trades(producer, last_trade_id, backfills))
    await stop.wait()

    # 1. Stop reading new trades
    stream.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await stream
    # 2. Let in-flight backfills finish, within a deadline
    await wait_for_backfills(backfills, SHUTDOWN_BACKFILL_TIMEOUT)
    # 3. Deliver what's queued. In a thread so a second signal can still force exit.
    log.info("flushing %d pending messages", len(producer))
    undelivered = await asyncio.to_thread(producer.flush, SHUTDOWN_FLUSH_TIMEOUT)
    if undelivered:
        log.error("shutdown: %d messages were not delivered", undelivered)


if __name__ == "__main__":
    asyncio.run(main())
