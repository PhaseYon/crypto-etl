"""Writes small contract-shaped Delta tables (docs/CONTRACTS.md) for tests."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
from deltalake import write_deltalake

TS = pa.timestamp("us", tz="UTC")


def write_bronze(root: str, rows: list[tuple[int, int]], now: datetime, options: dict | None = None) -> None:
    """rows: (kafka_partition, kafka_offset). One write per call = one commit."""
    table = pa.table(
        {
            "kafka_key": pa.array(["BTC-USD"] * len(rows)),
            "value": pa.array(["{}"] * len(rows)),
            "topic": pa.array(["raw.trades"] * len(rows)),
            "kafka_partition": pa.array([p for p, _ in rows], pa.int32()),
            "kafka_offset": pa.array([o for _, o in rows], pa.int64()),
            "kafka_timestamp": pa.array([now] * len(rows), TS),
            "ingested_at": pa.array([now] * len(rows), TS),
            "ingest_date": pa.array([now.date()] * len(rows), pa.date32()),
        }
    )
    write_deltalake(
        f"{root}/bronze/trades", table, mode="append", partition_by=["ingest_date"], storage_options=options
    )


def write_silver(root: str, sources: list[str], now: datetime, options: dict | None = None) -> None:
    n = len(sources)
    table = pa.table(
        {
            "exchange": pa.array(["coinbase"] * n),
            "product_id": pa.array(["BTC-USD"] * n),
            "trade_id": pa.array(list(range(n)), pa.int64()),
            "price": pa.array([Decimal("84000.5")] * n, pa.decimal128(38, 18)),
            "size": pa.array([Decimal("0.01")] * n, pa.decimal128(38, 18)),
            "side": pa.array(["buy"] * n),
            "trade_time": pa.array([now] * n, TS),
            "source": pa.array(sources),
            "kafka_partition": pa.array([0] * n, pa.int32()),
            "kafka_offset": pa.array(list(range(n)), pa.int64()),
            "ingested_at": pa.array([now] * n, TS),
            "trade_date": pa.array([now.date()] * n, pa.date32()),
        }
    )
    write_deltalake(f"{root}/silver/trades", table, mode="append", partition_by=["trade_date"], storage_options=options)


def write_gold(
    root: str, product_minutes: dict[str, list[datetime]], now: datetime, options: dict | None = None
) -> None:
    rows = [(p, m) for p, minutes in product_minutes.items() for m in minutes]
    table = pa.table(
        {
            "product_id": pa.array([p for p, _ in rows]),
            "window_start": pa.array([m for _, m in rows], TS),
            "window_end": pa.array([m + timedelta(minutes=1) for _, m in rows], TS),
            "open": pa.array([100.0 + i for i in range(len(rows))]),
            "high": pa.array([102.0 + i for i in range(len(rows))]),
            "low": pa.array([99.0 + i for i in range(len(rows))]),
            "close": pa.array([101.0 + i for i in range(len(rows))]),
            "volume": pa.array([1.5] * len(rows)),
            "quote_volume": pa.array([150.0] * len(rows)),
            "vwap": pa.array([100.5] * len(rows)),
            "trade_count": pa.array([12] * len(rows), pa.int64()),
            "buy_volume": pa.array([0.7] * len(rows)),
            "sell_volume": pa.array([0.8] * len(rows)),
            "updated_at": pa.array([now] * len(rows), TS),
            "window_date": pa.array([m.date() for _, m in rows], pa.date32()),
        }
    )
    write_deltalake(
        f"{root}/gold/ohlcv_1m", table, mode="append", partition_by=["window_date"], storage_options=options
    )


def write_quarantine(root: str, errors: list[str], now: datetime, options: dict | None = None) -> None:
    n = len(errors)
    table = pa.table(
        {
            "value": pa.array([f'{{"bad": {i}}}' + "x" * 400 for i in range(n)]),
            "error": pa.array(errors),
            "kafka_partition": pa.array([0] * n, pa.int32()),
            "kafka_offset": pa.array(list(range(n)), pa.int64()),
            "quarantined_at": pa.array([now - timedelta(minutes=i) for i in range(n)], TS),
        }
    )
    write_deltalake(f"{root}/silver/trades_quarantine", table, mode="append", storage_options=options)


def write_gaps(root: str, rows: list[tuple[str, int, int, datetime]], options: dict | None = None) -> None:
    table = pa.table(
        {
            "product_id": pa.array([r[0] for r in rows]),
            "gap_start_id": pa.array([r[1] for r in rows], pa.int64()),
            "gap_end_id": pa.array([r[2] for r in rows], pa.int64()),
            "missing_count": pa.array([r[2] - r[1] + 1 for r in rows], pa.int64()),
            "detected_at": pa.array([r[3] for r in rows], TS),
        }
    )
    write_deltalake(f"{root}/ops/trade_gaps", table, mode="append", storage_options=options)


def utc_minute(minutes_ago: int, now: datetime | None = None) -> datetime:
    now = now or datetime.now(UTC)
    return now.replace(second=0, microsecond=0) - timedelta(minutes=minutes_ago)
