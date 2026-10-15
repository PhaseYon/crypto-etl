"""Kafka producer setup, delivery tracking, and reading the last trade_ids back on startup."""

import json
import logging
import time
from collections.abc import Iterable

from confluent_kafka import Consumer, KafkaException, Message, Producer, TopicPartition

from .config import Settings

log = logging.getLogger(__name__)


def make_producer(settings: Settings) -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,  # no duplicates from producer retries
            "acks": "all",  # wait for the broker to persist each write
            "linger.ms": 50,  # batch small messages for up to 50ms
            "compression.type": "lz4",
        }
    )


class DeliveryReporter:
    """Delivery callback that counts confirmed and failed messages."""

    def __init__(self, log_every: int = 1000):
        self.delivered = 0
        self.failed = 0
        self.log_every = log_every

    def __call__(self, err, msg: Message):
        if err:
            self.failed += 1
            log.error("delivery failed for key=%s: %s", msg.key(), err)
            return
        self.delivered += 1
        if self.delivered % self.log_every == 0:
            log.info(
                "delivered %d trades (last: %s partition=%d offset=%d)",
                self.delivered,
                msg.key().decode(),
                msg.partition(),
                msg.offset(),
            )


def produce(producer: Producer, topic: str, key: str, value: str | bytes, on_delivery, block_timeout: float = 30.0):
    """produce(), waiting for queue space instead of failing when the local queue is full."""
    deadline = time.monotonic() + block_timeout
    while True:
        try:
            producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
            return
        except BufferError:
            if time.monotonic() > deadline:
                raise
            log.warning("local producer queue full (%d messages), waiting for Kafka", len(producer))
            producer.poll(0.5)


def max_trade_ids(values: Iterable[bytes | str], products: Iterable[str]) -> dict[str, int]:
    """Highest trade_id per product among raw message values. Skips unparseable messages."""
    wanted = set(products)
    last: dict[str, int] = {}
    for value in values:
        try:
            trade = json.loads(value)
            product, trade_id = trade["product_id"], int(trade["trade_id"])
        except (ValueError, KeyError, TypeError):
            continue
        if product in wanted:
            last[product] = max(last.get(product, trade_id), trade_id)
    return last


def load_last_trade_ids(settings: Settings) -> dict[str, int]:
    """Highest trade_id per product in the tail of each partition. Empty if Kafka is unreachable."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": "producer-startup-scan",
            "enable.auto.commit": False,
        }
    )
    deadline = time.monotonic() + settings.startup_scan_timeout
    try:
        while True:
            try:
                return max_trade_ids(_read_tails(consumer, settings, deadline), settings.products)
            except KafkaException as e:
                # Retry transient errors (e.g. a partition briefly without a leader) while time remains
                if time.monotonic() + 0.5 >= deadline:
                    log.warning("could not read last trade_ids from Kafka (%s), starting without them", e)
                    return {}
                log.info("startup scan failed (%s), retrying", e)
                time.sleep(0.5)
    finally:
        consumer.close()


def _read_tails(consumer: Consumer, settings: Settings, deadline: float) -> list[bytes]:
    topic = settings.topic

    def time_left() -> float:
        return max(deadline - time.monotonic(), 0.1)

    metadata = consumer.list_topics(topic, timeout=time_left()).topics[topic]
    if metadata.error is not None:
        raise KafkaException(metadata.error)

    remaining = {}  # partition -> offset of its last message
    assignments = []
    for p in metadata.partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=time_left())
        if high > low:
            remaining[p] = high - 1
            assignments.append(TopicPartition(topic, p, max(low, high - settings.startup_scan_messages)))
    consumer.assign(assignments)

    values = []
    # Short polls under the overall deadline: a long consume() timeout would wait for a
    # full batch even when only a few messages are left
    while remaining:
        if time.monotonic() > deadline:
            log.warning("startup scan timed out, %d partitions unread", len(remaining))
            break
        for msg in consumer.consume(num_messages=500, timeout=0.2):
            if msg.error():
                continue
            values.append(msg.value())
            if msg.offset() >= remaining.get(msg.partition(), float("inf")):
                del remaining[msg.partition()]
    return values
