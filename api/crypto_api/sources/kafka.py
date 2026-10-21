"""Kafka metadata via confluent-kafka. Reads watermarks and topic config only, never message data."""

import threading

from confluent_kafka import Consumer, KafkaException, TopicPartition

# confluent_kafka.admin documents these as public but doesn't re-export them in a way type checkers accept
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType  # pyright: ignore[reportPrivateImportUsage]

from .base import PartitionWatermarks


class TopicUnavailableError(KafkaException):
    """The topic doesn't exist (yet) or its metadata reports an error."""


class KafkaMetadataSource:
    def __init__(self, bootstrap: str, topic: str, timeout_seconds: float):
        self._topic = topic
        self._timeout = timeout_seconds
        common = {
            "bootstrap.servers": bootstrap,
            "socket.timeout.ms": int(timeout_seconds * 1000),
            # Quiet librdkafka's own reconnect logging; errors surface through calls
            "log_level": 3,
        }
        # group.id is required by the client but nothing is ever committed
        self._consumer = Consumer({**common, "group.id": "crypto-etl-dashboard", "enable.auto.commit": False})
        self._admin = AdminClient(common)
        # Consumer handles aren't safe for concurrent use from the worker threads
        self._lock = threading.Lock()

    def watermarks(self) -> list[PartitionWatermarks]:
        with self._lock:
            metadata = self._consumer.list_topics(self._topic, timeout=self._timeout)
            topic = metadata.topics.get(self._topic)
            if topic is None or topic.error is not None:
                reason = topic.error.str() if topic is not None and topic.error is not None else "not found"
                raise TopicUnavailableError(f"topic {self._topic!r} not available: {reason}")
            result = []
            for partition in sorted(topic.partitions):
                low, high = self._consumer.get_watermark_offsets(
                    TopicPartition(self._topic, partition), timeout=self._timeout
                )
                result.append(PartitionWatermarks(partition, low, high))
            return result

    def retention_ms(self) -> int | None:
        resource = ConfigResource(ResourceType.TOPIC, self._topic)
        futures = self._admin.describe_configs([resource], request_timeout=self._timeout)
        configs = futures[resource].result(timeout=self._timeout)
        entry = configs.get("retention.ms")
        if entry is None or entry.value is None:
            return None
        value = int(entry.value)
        return None if value < 0 else value  # -1 means unlimited

    def close(self) -> None:
        with self._lock:
            self._consumer.close()
