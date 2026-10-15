"""Storage locations and credentials, read from the environment (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class StorageConfig:
    """Where the lake lives and how to authenticate to it.

    `s3_endpoint` empty means real AWS S3; set it (e.g. http://minio:9000) for MinIO.
    """

    bucket: str = "crypto-lake"
    s3_endpoint: str = ""
    region: str = "us-east-1"
    access_key_id: str | None = None
    secret_access_key: str | None = field(default=None, repr=False)

    @classmethod
    def from_env(cls) -> StorageConfig:
        return cls(
            bucket=os.getenv("LAKE_BUCKET", "crypto-lake"),
            s3_endpoint=os.getenv("S3_ENDPOINT", ""),
            region=os.getenv("AWS_REGION", "us-east-1"),
            access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or None,
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or None,
        )

    def table_uri(self, path: str) -> str:
        """URI of a lake table, e.g. table_uri("gold/ohlcv_1m") -> s3://crypto-lake/gold/ohlcv_1m."""
        return f"s3://{self.bucket}/{path.strip('/')}"

    def storage_options(self) -> dict[str, str]:
        """delta-rs object store options. Empty for real AWS with ambient credentials."""
        opts: dict[str, str] = {"AWS_REGION": self.region}
        if self.access_key_id and self.secret_access_key:
            opts["AWS_ACCESS_KEY_ID"] = self.access_key_id
            opts["AWS_SECRET_ACCESS_KEY"] = self.secret_access_key
        if self.s3_endpoint:
            # MinIO: custom endpoint, path-style addressing, plain HTTP allowed locally,
            # and conditional puts so concurrent Delta commits are safe without DynamoDB.
            opts["AWS_ENDPOINT_URL"] = self.s3_endpoint
            opts["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "false"
            opts["AWS_CONDITIONAL_PUT"] = "etag"
            if self.s3_endpoint.startswith("http://"):
                opts["AWS_ALLOW_HTTP"] = "true"
        return opts


def storage_options_for(uri: str, config: StorageConfig | None = None) -> dict[str, str] | None:
    """Options for `uri`: S3 options for s3:// URIs, None for local paths."""
    if uri.startswith(("s3://", "s3a://")):
        return (config or StorageConfig.from_env()).storage_options()
    return None


def normalize_uri(uri: str) -> str:
    """delta-rs speaks s3://, Spark speaks s3a://; accept both."""
    return "s3://" + uri[len("s3a://") :] if uri.startswith("s3a://") else uri


GOLD_OHLCV_PATH = "gold/ohlcv_1m"
PREDICTIONS_PATH = "ml/predictions"
DEFAULT_TRACKING_URI = "sqlite:///mlruns/mlflow.db"
DEFAULT_ARTIFACT_ROOT = "mlruns/artifacts"


def mlflow_tracking_uri() -> str:
    return os.getenv("MLFLOW_TRACKING_URI") or DEFAULT_TRACKING_URI


def mlflow_artifact_root() -> str:
    return os.getenv("MLFLOW_ARTIFACT_ROOT") or DEFAULT_ARTIFACT_ROOT
