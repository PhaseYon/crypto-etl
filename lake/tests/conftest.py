from __future__ import annotations

import os
import shutil
import tempfile
import uuid
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from lake.config import LakeConfig
from lake.spark import build_spark


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One local Spark for the whole run (JVM startup is the slow part)."""
    config = LakeConfig.from_env({**os.environ, "SPARK_SHUFFLE_PARTITIONS": "2", "SPARK_DRIVER_MEMORY": "1g"})
    session = build_spark("lake-tests", config, master="local[2]", extra={"spark.ui.enabled": "false"})
    yield session
    session.stop()


@pytest.fixture
def local_config() -> Iterator[LakeConfig]:
    """Config whose tables and checkpoints live in a fresh local temp directory."""
    root = tempfile.mkdtemp(prefix="lake-test-")
    yield LakeConfig.from_env({"LAKE_ROOT": root, "KAFKA_BOOTSTRAP": "unused:9092"})
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def s3_config() -> LakeConfig:
    """Config pointing at a unique prefix in the real bucket (integration tests)."""
    env = dict(os.environ)
    bucket = env.get("LAKE_BUCKET", "crypto-lake")
    env["LAKE_ROOT"] = f"s3a://{bucket}/_tests/{uuid.uuid4().hex[:12]}"
    return LakeConfig.from_env(env)
