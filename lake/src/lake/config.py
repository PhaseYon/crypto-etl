"""Lake configuration, read from environment variables (see docs/CONTRACTS.md)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


def _bool(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class LakeConfig:
    kafka_bootstrap: str
    kafka_topic: str
    # Root of all tables and checkpoints: s3a://<bucket> in Docker/cloud, a local path in tests
    lake_root: str
    s3_endpoint: str | None
    aws_region: str
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    # Streaming tuning
    bronze_trigger: str
    silver_trigger: str
    gold_trigger: str
    max_offsets_per_trigger: int
    fail_on_data_loss: bool
    shuffle_partitions: int
    driver_memory: str
    spark_master: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> LakeConfig:
        env = os.environ if env is None else env
        bucket = env.get("LAKE_BUCKET", "crypto-lake")
        return cls(
            kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", "kafka:29092"),
            kafka_topic=env.get("KAFKA_TOPIC", "raw.trades"),
            lake_root=env.get("LAKE_ROOT") or f"s3a://{bucket}",
            s3_endpoint=env.get("S3_ENDPOINT") or None,
            aws_region=env.get("AWS_REGION", "us-east-1"),
            aws_access_key_id=env.get("AWS_ACCESS_KEY_ID") or None,
            aws_secret_access_key=env.get("AWS_SECRET_ACCESS_KEY") or None,
            bronze_trigger=env.get("BRONZE_TRIGGER", "10 seconds"),
            silver_trigger=env.get("SILVER_TRIGGER", "30 seconds"),
            gold_trigger=env.get("GOLD_TRIGGER", "30 seconds"),
            max_offsets_per_trigger=int(env.get("MAX_OFFSETS_PER_TRIGGER", "50000")),
            fail_on_data_loss=_bool(env.get("FAIL_ON_DATA_LOSS", "true")),
            shuffle_partitions=int(env.get("SPARK_SHUFFLE_PARTITIONS", "8")),
            driver_memory=env.get("SPARK_DRIVER_MEMORY", "1g"),
            spark_master=env.get("SPARK_MASTER", "local[*]"),
        )

    def path(self, relative: str) -> str:
        return f"{self.lake_root.rstrip('/')}/{relative}"

    def checkpoint(self, job_name: str) -> str:
        return self.path(f"_checkpoints/{job_name}")

    @property
    def bronze_trades(self) -> str:
        return self.path("bronze/trades")

    @property
    def silver_trades(self) -> str:
        return self.path("silver/trades")

    @property
    def silver_quarantine(self) -> str:
        return self.path("silver/trades_quarantine")

    @property
    def gold_ohlcv_1m(self) -> str:
        return self.path("gold/ohlcv_1m")

    @property
    def ops_trade_gaps(self) -> str:
        return self.path("ops/trade_gaps")
