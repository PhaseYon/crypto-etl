"""Test helpers for producing to a throwaway Kafka topic."""

from __future__ import annotations

import json
import uuid

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def create_topic(bootstrap: str, partitions: int = 3) -> str:
    name = f"test.lake.{uuid.uuid4().hex[:10]}"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for future in admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)]).values():
        future.result(timeout=30)
    return name


def delete_topic(bootstrap: str, name: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for future in admin.delete_topics([name]).values():
        future.result(timeout=30)


def produce(bootstrap: str, topic: str, messages: list[dict | str]) -> None:
    """Send messages keyed by product_id (raw strings are sent as-is, e.g. malformed JSON)."""
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})
    errors: list[str] = []

    def on_delivery(err, msg):
        if err:
            errors.append(str(err))

    for message in messages:
        if isinstance(message, str):
            producer.produce(topic, value=message, on_delivery=on_delivery)
        else:
            producer.produce(topic, key=message.get("product_id"), value=json.dumps(message), on_delivery=on_delivery)
    remaining = producer.flush(30)
    assert remaining == 0 and not errors, f"produce failed: {errors or remaining}"


def trade(product_id: str, trade_id: int, price: str, size: str, time: str, side: str = "buy", **extra) -> dict:
    """A Coinbase `match` message as the producer forwards it."""
    return {
        "type": "match",
        "trade_id": trade_id,
        "product_id": product_id,
        "price": price,
        "size": size,
        "side": side,
        "time": time,
        **extra,
    }
