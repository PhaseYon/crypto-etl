"""Interfaces the services read pipeline state through, plus the plain data they return.

Real implementations talk to Kafka and the Delta lake; the mock implementation generates
contract-shaped data. All methods are blocking and are called from worker threads with a timeout.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol


class TableMissingError(Exception):
    """The Delta table hasn't been created yet."""


@dataclass(frozen=True)
class TableSpec:
    id: str
    label: str
    path: str  # relative to the lake root
    freshness_column: str
    # Missing optional tables report "inactive" rather than "down"
    optional: bool = False


# Lake tables from docs/CONTRACTS.md
TABLE_SPECS: dict[str, TableSpec] = {
    spec.id: spec
    for spec in (
        TableSpec("bronze", "Bronze trades", "bronze/trades", "ingested_at"),
        TableSpec("silver", "Silver trades", "silver/trades", "ingested_at"),
        TableSpec("quarantine", "Quarantine", "silver/trades_quarantine", "quarantined_at", optional=True),
        TableSpec("gold", "Gold OHLCV 1m", "gold/ohlcv_1m", "updated_at"),
        TableSpec("trade_gaps", "Trade gaps", "ops/trade_gaps", "detected_at", optional=True),
        TableSpec("predictions", "ML predictions", "ml/predictions", "created_at", optional=True),
    )
}


@dataclass(frozen=True)
class PartitionWatermarks:
    partition: int
    low: int
    high: int


@dataclass(frozen=True)
class TableSnapshot:
    version: int
    last_commit_at: datetime | None
    row_count: int | None
    newest_at: datetime | None  # max of the table's freshness column


@dataclass(frozen=True)
class CandleRow:
    window_start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None
    trade_count: int


@dataclass(frozen=True)
class QuarantineRow:
    error: str
    quarantined_at: datetime | None
    kafka_partition: int | None
    kafka_offset: int | None
    value: str


@dataclass(frozen=True)
class QuarantineData:
    total: int
    reasons: dict[str, int]
    recent: list[QuarantineRow] = field(default_factory=list)


@dataclass(frozen=True)
class GapRow:
    product_id: str
    gap_start_id: int
    gap_end_id: int
    missing_count: int
    detected_at: datetime | None


@dataclass(frozen=True)
class BackfillCounts:
    total: int
    backfilled: int


class KafkaSource(Protocol):
    def watermarks(self) -> list[PartitionWatermarks]: ...

    def retention_ms(self) -> int | None: ...


class LakeSource(Protocol):
    def table_snapshot(self, spec: TableSpec) -> TableSnapshot:
        """Raises TableMissingError if the table doesn't exist."""
        ...

    def bronze_max_offsets(self) -> dict[int, int]:
        """Highest kafka_offset per Kafka partition ingested into bronze."""
        ...

    def ohlcv(self, product_id: str, since: datetime) -> list[CandleRow]: ...

    def quarantine(self, recent_limit: int) -> QuarantineData: ...

    def latest_gaps(self) -> list[GapRow]: ...

    def backfill_counts(self, since: date) -> BackfillCounts: ...

    def predictions(self, limit: int) -> list[dict[str, Any]]: ...
