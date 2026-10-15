from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import replace

import pytest

from lake.config import LakeConfig

from .kafka_helpers import create_topic, delete_topic


@pytest.fixture
def pipeline_config(s3_config: LakeConfig) -> Iterator[LakeConfig]:
    """Unique Kafka topic + unique lake prefix in the object store, for one test."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
    topic = create_topic(bootstrap)
    yield replace(s3_config, kafka_topic=topic, kafka_bootstrap=bootstrap)
    delete_topic(bootstrap, topic)
