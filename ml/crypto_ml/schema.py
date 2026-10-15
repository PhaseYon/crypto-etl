"""The gold/ohlcv_1m contract (docs/CONTRACTS.md) and validation against it."""

from __future__ import annotations

import pyarrow as pa

# Column -> Delta primitive type, exactly as in docs/CONTRACTS.md.
OHLCV_DELTA_TYPES: dict[str, str] = {
    "product_id": "string",
    "window_start": "timestamp",
    "window_end": "timestamp",
    "open": "double",
    "high": "double",
    "low": "double",
    "close": "double",
    "volume": "double",
    "quote_volume": "double",
    "vwap": "double",
    "trade_count": "long",
    "buy_volume": "double",
    "sell_volume": "double",
    "updated_at": "timestamp",
    "window_date": "date",
}
OHLCV_PARTITION_COLUMN = "window_date"

_DELTA_TO_ARROW: dict[str, pa.DataType] = {
    "string": pa.string(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "double": pa.float64(),
    "long": pa.int64(),
    "date": pa.date32(),
}

OHLCV_ARROW_SCHEMA = pa.schema([(name, _DELTA_TO_ARROW[t]) for name, t in OHLCV_DELTA_TYPES.items()])


class SchemaDriftError(ValueError):
    """The table no longer matches the contract the ML code was written against."""


def validate_delta_types(actual: dict[str, str], expected: dict[str, str] = OHLCV_DELTA_TYPES) -> list[str]:
    """Problems found comparing a table's column types to the contract. Extra columns are allowed."""
    problems = []
    for name, want in expected.items():
        got = actual.get(name)
        if got is None:
            problems.append(f"missing column '{name}' (expected {want})")
        elif got != want:
            problems.append(f"column '{name}' has type {got}, expected {want}")
    return problems
