"""Fixtures for integration tests: a real Kafka, a unique topic per test, and the producer as a subprocess.

Point KAFKA_TEST_BOOTSTRAP at a running broker (default localhost:9092), e.g.:
    KAFKA_HOST_PORT=19092 docker compose up -d kafka kafka-init
    KAFKA_TEST_BOOTSTRAP=localhost:19092 pytest producer/tests -m integration
"""

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable

import pytest
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_TEST_BOOTSTRAP = os.getenv("KAFKA_TEST_BOOTSTRAP", "localhost:9092")


@pytest.fixture(autouse=True)
def no_real_kafka_in_unit_tests(request, monkeypatch):
    """Unit tests must never reach a real broker: default settings point at the main stack's raw.trades."""
    if request.node.get_closest_marker("integration"):
        return

    def refuse(*args, **kwargs):
        raise RuntimeError("unit test tried to create a real Kafka producer; inject a fake instead")

    for module in ("crypto_producer.kafka_io", "crypto_producer.backfill", "crypto_producer.app"):
        monkeypatch.setattr(f"{module}.make_producer", refuse)


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    admin = AdminClient({"bootstrap.servers": KAFKA_TEST_BOOTSTRAP})
    try:
        admin.list_topics(timeout=5)
    except Exception as e:
        pytest.skip(f"Kafka not reachable at {KAFKA_TEST_BOOTSTRAP} ({e}); set KAFKA_TEST_BOOTSTRAP")
    return KAFKA_TEST_BOOTSTRAP


@pytest.fixture
def topic(kafka_bootstrap):
    """A fresh 3-partition topic, deleted after the test."""
    name = f"it-producer-{uuid.uuid4().hex[:12]}"
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])[name].result(timeout=10)
    assert wait_for(lambda: _partitions_ready(kafka_bootstrap, name), timeout=15), f"topic {name} never got leaders"
    yield name
    admin.delete_topics([name])[name].result(timeout=10)


def _partitions_ready(bootstrap: str, topic: str) -> bool:
    """A new topic briefly has no partition leaders; watermark queries fail until it does."""
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "it-ready-check"})
    try:
        for p in consumer.list_topics(topic, timeout=5).topics[topic].partitions:
            consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
        return True
    except KafkaException:
        return False
    finally:
        consumer.close()


def read_topic(bootstrap: str, topic: str, attempts: int = 10) -> list[tuple[str, dict]]:
    """Every message currently in the topic as (key, parsed value).

    Retries transient leader errors, which a single-node KRaft broker reports briefly
    around topic creation and deletion.
    """
    for attempt in range(attempts):
        try:
            return _read_topic_once(bootstrap, topic)
        except KafkaException:
            if attempt == attempts - 1:
                raise
            time.sleep(0.5)
    raise AssertionError("unreachable")


def _read_topic_once(bootstrap: str, topic: str) -> list[tuple[str, dict]]:
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"it-{uuid.uuid4().hex}"})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        ends = {}
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if high > low:
                ends[p] = high - 1
        consumer.assign([TopicPartition(topic, p, 0) for p in ends])
        out = []
        deadline = time.monotonic() + 15
        while ends and time.monotonic() < deadline:
            for msg in consumer.consume(500, timeout=0.2):
                if msg.error():
                    continue
                out.append((msg.key().decode(), json.loads(msg.value())))
                if msg.offset() >= ends.get(msg.partition(), float("inf")):
                    del ends[msg.partition()]
        return out
    finally:
        consumer.close()


def wait_for(predicate: Callable[[], bool], timeout: float = 20, interval: float = 0.3) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class ProducerProcess:
    """`python -m crypto_producer` as a subprocess, so signals and shutdown are real."""

    def __init__(self, env: dict[str, str]):
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "crypto_producer"],
            env={**os.environ, **env},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.output = ""

    def stop(self, sig: int = signal.SIGINT, timeout: float = 30) -> int:
        if self.proc.poll() is None:
            self.proc.send_signal(sig)
        self.output, _ = self.proc.communicate(timeout=timeout)
        return self.proc.returncode


@pytest.fixture
def run_producer():
    """Start producers with the given env; any still running are killed at teardown."""
    started: list[ProducerProcess] = []

    def start(**env: str) -> ProducerProcess:
        defaults = {"PRODUCTS": "BTC-USD,ETH-USD", "SHUTDOWN_FLUSH_TIMEOUT": "5", "LOG_LEVEL": "INFO"}
        p = ProducerProcess({**defaults, **env})
        started.append(p)
        return p

    yield start
    for p in started:
        if p.proc.poll() is None:
            p.proc.kill()
            p.proc.communicate()
