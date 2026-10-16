"""Settings loaded from environment variables. No secrets: Coinbase market data is public."""

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "localhost:9092"
    topic: str = "raw.trades"
    products: tuple[str, ...] = ("BTC-USD", "ETH-USD", "SOL-USD")
    ws_url: str = "wss://ws-feed.exchange.coinbase.com"
    rest_url: str = "https://api.exchange.coinbase.com"
    rest_timeout: float = 15.0
    rest_retries: int = 3
    max_backfill_trades: int = 500_000
    startup_scan_messages: int = 5000
    startup_scan_timeout: float = 15.0
    # Shutdown budget: backfill wait + flush must fit inside Docker's stop_grace_period
    shutdown_backfill_timeout: float = 5.0
    shutdown_flush_timeout: float = 10.0
    poll_interval: float = 1.0
    health_file: str = ""  # touched while the feed is alive; empty disables it
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> "Settings":
        """Build settings from env vars, raising ValueError naming the bad variable."""
        d = cls()

        def get(name: str, default, cast=str):
            raw = env.get(name)
            if raw is None or raw.strip() == "":
                return default
            try:
                value = cast(raw.strip())
            except ValueError:
                raise ValueError(f"{name}={raw!r} is not a valid {cast.__name__}") from None
            if isinstance(value, int | float) and value < 0:
                raise ValueError(f"{name} must be >= 0, got {raw!r}")
            return value

        products = tuple(p.strip() for p in env.get("PRODUCTS", "").split(",") if p.strip()) or d.products
        return cls(
            kafka_bootstrap=get("KAFKA_BOOTSTRAP", d.kafka_bootstrap),
            topic=get("KAFKA_TOPIC", d.topic),
            products=products,
            ws_url=get("COINBASE_WS_URL", d.ws_url),
            rest_url=get("COINBASE_REST_URL", d.rest_url).rstrip("/"),
            rest_timeout=get("REST_TIMEOUT", d.rest_timeout, float),
            rest_retries=get("REST_RETRIES", d.rest_retries, int),
            max_backfill_trades=get("MAX_BACKFILL_TRADES", d.max_backfill_trades, int),
            startup_scan_messages=get("STARTUP_SCAN_MESSAGES", d.startup_scan_messages, int),
            startup_scan_timeout=get("STARTUP_SCAN_TIMEOUT", d.startup_scan_timeout, float),
            shutdown_backfill_timeout=get("SHUTDOWN_BACKFILL_TIMEOUT", d.shutdown_backfill_timeout, float),
            shutdown_flush_timeout=get("SHUTDOWN_FLUSH_TIMEOUT", d.shutdown_flush_timeout, float),
            poll_interval=get("POLL_INTERVAL", d.poll_interval, float),
            health_file=get("HEALTH_FILE", d.health_file),
            log_level=get("LOG_LEVEL", d.log_level).upper(),
        )
