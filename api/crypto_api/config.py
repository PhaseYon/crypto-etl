"""Settings loaded from environment variables (see .env.example and docs/CONTRACTS.md)."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    data_source: Literal["real", "mock"] = "real"
    mock_scenario: Literal["healthy", "degraded"] = "healthy"

    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "raw.trades"

    lake_bucket: str = "crypto-lake"
    # Overrides s3://{lake_bucket} as the lake root, e.g. a local path in tests
    lake_uri: str | None = None
    s3_endpoint: str | None = None
    aws_region: str = "us-east-1"
    aws_access_key_id: SecretStr | None = None
    aws_secret_access_key: SecretStr | None = None

    products_csv: str = Field(default="BTC-USD,ETH-USD,SOL-USD", alias="PRODUCTS")

    # Every call to Kafka or the lake is bounded by this timeout
    source_timeout_seconds: float = 4.0
    # Source reads are cached so many open dashboards don't multiply the load
    cache_ttl_seconds: float = 3.0
    poll_interval_ms: int = 5000

    # Health thresholds (seconds unless noted)
    kafka_stale_seconds: float = 120
    bronze_stale_seconds: float = 300
    silver_stale_seconds: float = 300
    gold_stale_seconds: float = 300
    predictions_stale_seconds: float = 7200
    lag_stale_messages: int = 10_000

    backfill_lookback_days: int = 1
    cors_origins_csv: str = Field(default="", alias="CORS_ORIGINS")

    @property
    def products(self) -> list[str]:
        return [p.strip() for p in self.products_csv.split(",") if p.strip()]

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.cors_origins_csv.split(",") if o.strip()]

    @property
    def lake_root(self) -> str:
        return (self.lake_uri or f"s3://{self.lake_bucket}").rstrip("/")

    def storage_options(self) -> dict[str, str]:
        """object_store options for delta-rs; empty S3_ENDPOINT means real AWS S3."""
        if not self.lake_root.startswith("s3://"):
            return {}
        options = {
            "AWS_REGION": self.aws_region,
            # Bound every request so a hung object store can't stall the API
            "timeout": f"{max(1, int(self.source_timeout_seconds))}s",
            "connect_timeout": "3s",
        }
        if self.aws_access_key_id and self.aws_secret_access_key:
            options["AWS_ACCESS_KEY_ID"] = self.aws_access_key_id.get_secret_value()
            options["AWS_SECRET_ACCESS_KEY"] = self.aws_secret_access_key.get_secret_value()
        if self.s3_endpoint:
            options["AWS_ENDPOINT_URL"] = self.s3_endpoint
            options["AWS_ALLOW_HTTP"] = "true" if self.s3_endpoint.startswith("http://") else "false"
            # MinIO and most S3-compatible stores need path-style URLs
            options["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "false"
        return options


@lru_cache
def get_settings() -> Settings:
    return Settings()
