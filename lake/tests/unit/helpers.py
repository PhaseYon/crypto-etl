"""Builders for test DataFrames in the bronze/silver schemas."""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession

from lake.schemas import BRONZE_TRADES, SILVER_TRADES

INGESTED = dt.datetime(2026, 10, 15, 12, 0, 0)


def trade_json(
    trade_id=1,
    product_id="BTC-USD",
    price="84000.5",
    size="0.01",
    side="buy",
    time="2026-10-15T09:14:26.443591Z",
    **extra,
) -> str:
    message = {"type": "match", "trade_id": trade_id, "product_id": product_id, "price": price, "size": size}
    return json.dumps({**message, "side": side, "time": time, **extra})


def bronze_df(spark: SparkSession, values: list[str | None], partition: int = 0, start_offset: int = 0) -> DataFrame:
    rows = [
        ("BTC-USD", value, "raw.trades", partition, start_offset + i, INGESTED, INGESTED, INGESTED.date())
        for i, value in enumerate(values)
    ]
    return spark.createDataFrame(rows, BRONZE_TRADES)


def silver_df(spark: SparkSession, trades: list[tuple]) -> DataFrame:
    """trades: (product_id, trade_id, price, size, side, trade_time[, source])."""
    rows = []
    for i, (product_id, trade_id, price, size, side, trade_time, *rest) in enumerate(trades):
        source = rest[0] if rest else "ws"
        lineage = (0, i, INGESTED)
        rows.append(
            ("coinbase", product_id, trade_id, Decimal(price), Decimal(size), side, trade_time, source)
            + lineage
            + (trade_time.date(),)
        )
    return spark.createDataFrame(rows, SILVER_TRADES)
