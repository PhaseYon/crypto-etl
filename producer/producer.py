"""Stream trades from the Coinbase Exchange WebSocket into Kafka.

Subscribes to the public `matches` channel and publishes each trade, unmodified,
to a Kafka topic keyed by product_id (so each pair's trades stay ordered).

The WebSocket is a live feed with no replay, so trades missed while disconnected
are detected by a jump in trade_id and backfilled from the REST API. On startup
the last trade_id per product is read back from Kafka, so gaps across producer
restarts are caught too.
"""

import asyncio
import json
import logging
import os
import random
import signal
import urllib.request

import websockets
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

COINBASE_WS_URL = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
COINBASE_REST_URL = os.getenv("COINBASE_REST_URL", "https://api.exchange.coinbase.com")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw.trades")
PRODUCTS = os.getenv("PRODUCTS", "BTC-USD,ETH-USD,SOL-USD").split(",")
MAX_BACKFILL_TRADES = int(os.getenv("MAX_BACKFILL_TRADES", "100000"))
STARTUP_SCAN_MESSAGES = int(os.getenv("STARTUP_SCAN_MESSAGES", "5000"))

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

        while remaining:
            msgs = consumer.consume(num_messages=500, timeout=5)
            if not msgs:
                log.warning("startup scan timed out, %d partitions unread", len(remaining))
                break
            for msg in msgs:
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


async def backfill(producer: Producer, product: str, lo: int, hi: int):
    """Produce the trades with lo < trade_id < hi, fetched from the REST API."""
    missing = hi - lo - 1
    if missing > MAX_BACKFILL_TRADES:
        log.error("gap too large to backfill: %s trade_id %d..%d (%d trades)", product, lo + 1, hi - 1, missing)
        return
    log.warning("gap detected: %s missing %d trades (%d..%d), backfilling", product, missing, lo + 1, hi - 1)

    sent = 0
    cursor = hi
    try:
        while cursor > lo + 1:
            # Blocking HTTP runs in a worker thread so the live stream keeps flowing
            trades = await asyncio.to_thread(fetch_trades_before, product, cursor)
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
            cursor = trades[-1]["trade_id"]
            await asyncio.sleep(0.25)  # stay well under the public REST rate limit
    except Exception:
        log.exception("backfill failed for %s after %d of %d trades (%d..%d)", product, sent, missing, lo + 1, hi - 1)
        return
    log.info("backfill done: %s sent %d of %d missing trades", product, sent, missing)


async def stream_trades(producer: Producer, stop: asyncio.Event, last_trade_id: dict[str, int]):
    subscribe = {"type": "subscribe", "product_ids": PRODUCTS, "channels": ["matches"]}
    backoff = 1
    backfills: set[asyncio.Task] = set()

    while not stop.is_set():
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
                            task = asyncio.create_task(backfill(producer, product, prev, trade_id))
                            backfills.add(task)
                            task.add_done_callback(backfills.discard)
                        last_trade_id[product] = max(prev or 0, trade_id)

                        producer.produce(TOPIC, key=product, value=raw, on_delivery=on_delivery)
                        producer.poll(0)  # serve delivery callbacks without blocking
                    elif msg["type"] == "error":
                        log.error("coinbase error: %s", msg)
                    if stop.is_set():
                        break
            log.warning("connection closed by server, reconnecting")
        except Exception as e:
            delay = backoff + random.uniform(0, 1)  # jitter
            if isinstance(e, (websockets.WebSocketException, OSError)):
                log.warning("connection lost (%s: %s), reconnecting in %.1fs", type(e).__name__, e, delay)
            else:
                log.exception("unexpected error, reconnecting in %.1fs", delay)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, 60)


async def main():
    producer = make_producer()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    # Resume from what's already in Kafka so a gap across restarts is backfilled
    last_trade_id = load_last_trade_ids()
    log.info("resuming from last trade_ids in Kafka: %s", last_trade_id or "none found")

    task = asyncio.create_task(stream_trades(producer, stop, last_trade_id))
    await stop.wait()
    task.cancel()
    log.info("shutting down, flushing %d pending messages", len(producer))
    producer.flush(10)


if __name__ == "__main__":
    asyncio.run(main())
