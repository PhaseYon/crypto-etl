"""Synthetic gold/ohlcv_1m data for tests and local demos.

Simulates individual trades (random-walk prices, Poisson arrivals, lognormal sizes)
and aggregates them into minute candles, so the output looks like real gold data:
minutes without trades simply have no row.
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake

from crypto_ml.config import StorageConfig, normalize_uri, storage_options_for
from crypto_ml.schema import OHLCV_ARROW_SCHEMA, OHLCV_DELTA_TYPES, OHLCV_PARTITION_COLUMN

log = logging.getLogger(__name__)

DEFAULT_PRICES = {"BTC-USD": 84_000.0, "ETH-USD": 2_700.0, "SOL-USD": 120.0}


def generate_ohlcv(
    products: list[str],
    start: datetime,
    minutes: int,
    seed: int = 0,
    trades_per_minute: float = 8.0,
    per_trade_vol: float = 0.0004,
    drop_rate: float = 0.0,
) -> pd.DataFrame:
    """Candles for `products` over `minutes` minutes from `start` (UTC, minute-aligned).

    `drop_rate` additionally removes that fraction of candles at random, on top of the
    minutes that naturally have no trades.
    """
    rng = np.random.default_rng(seed)
    start_ts = pd.Timestamp(start).tz_convert("UTC") if pd.Timestamp(start).tzinfo else pd.Timestamp(start, tz="UTC")
    start_ts = start_ts.floor("min")
    frames = []
    for product in products:
        n_per_minute = rng.poisson(trades_per_minute, size=minutes)
        minute_idx = np.repeat(np.arange(minutes), n_per_minute)
        n = len(minute_idx)
        if n == 0:
            continue
        base = DEFAULT_PRICES.get(product, 100.0)
        price = base * np.exp(np.cumsum(rng.normal(0.0, per_trade_vol, size=n)))
        size = rng.lognormal(mean=np.log(base**-0.5), sigma=1.0, size=n)
        is_buy = rng.random(n) < 0.5
        trades = pd.DataFrame({
            "minute": minute_idx, "price": price, "size": size,
            "notional": price * size, "buy_size": np.where(is_buy, size, 0.0),
        })
        g = trades.groupby("minute", sort=True)
        candles = pd.DataFrame({
            "open": g["price"].first(), "high": g["price"].max(), "low": g["price"].min(),
            "close": g["price"].last(), "volume": g["size"].sum(), "quote_volume": g["notional"].sum(),
            "trade_count": g["price"].size().astype("int64"), "buy_volume": g["buy_size"].sum(),
        })
        candles["sell_volume"] = candles["volume"] - candles["buy_volume"]
        candles["vwap"] = candles["quote_volume"] / candles["volume"]
        candles["window_start"] = start_ts + pd.to_timedelta(candles.index.to_numpy(), unit="min")
        candles["product_id"] = product
        frames.append(candles.reset_index(drop=True))

    if not frames:
        return empty_ohlcv()
    df = pd.concat(frames, ignore_index=True)
    if drop_rate > 0:
        df = df[rng.random(len(df)) >= drop_rate]
    df["window_end"] = df["window_start"] + pd.Timedelta(minutes=1)
    df["updated_at"] = df["window_end"] + pd.Timedelta(seconds=30)
    df["window_date"] = df["window_start"].dt.date
    return df[list(OHLCV_DELTA_TYPES)].sort_values(["product_id", "window_start"]).reset_index(drop=True)


def empty_ohlcv() -> pd.DataFrame:
    return OHLCV_ARROW_SCHEMA.empty_table().to_pandas()


def to_arrow(df: pd.DataFrame) -> pa.Table:
    """Contract-typed Arrow table (rejects frames that don't fit the schema)."""
    return pa.Table.from_pandas(df[list(OHLCV_DELTA_TYPES)], schema=OHLCV_ARROW_SCHEMA, preserve_index=False)


def write_ohlcv_delta(df: pd.DataFrame, uri: str, mode: str = "overwrite",
                      storage_options: dict[str, str] | None = None) -> None:
    uri = normalize_uri(uri)
    if storage_options is None:
        storage_options = storage_options_for(uri)
    write_deltalake(uri, to_arrow(df), mode=mode, partition_by=[OHLCV_PARTITION_COLUMN],
                    storage_options=storage_options)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Write synthetic gold/ohlcv_1m data as a Delta table.")
    parser.add_argument("--uri", help="Table URI or local path (default: s3://$LAKE_BUCKET/gold/ohlcv_1m)")
    parser.add_argument("--products", default="BTC-USD,ETH-USD,SOL-USD")
    parser.add_argument("--start", default="2026-01-01T00:00:00Z")
    parser.add_argument("--days", type=float, default=3.0)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    uri = args.uri or StorageConfig.from_env().table_uri("gold/ohlcv_1m")
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00")).astimezone(UTC)
    df = generate_ohlcv(args.products.split(","), start, int(args.days * 1440), seed=args.seed)
    write_ohlcv_delta(df, uri, mode=args.mode)
    log.info("wrote %d synthetic candles to %s", len(df), uri)


if __name__ == "__main__":
    main()
