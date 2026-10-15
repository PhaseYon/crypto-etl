import sys
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crypto_api.config import Settings  # noqa: E402
from crypto_api.sources.base import (  # noqa: E402
    BackfillCounts,
    CandleRow,
    GapRow,
    PartitionWatermarks,
    QuarantineData,
    TableMissingError,
    TableSnapshot,
    TableSpec,
)


class FakeKafka:
    def __init__(self, marks: list[PartitionWatermarks] | None = None, error: Exception | None = None):
        self.marks = marks if marks is not None else [PartitionWatermarks(0, 0, 100), PartitionWatermarks(1, 0, 50)]
        self.error = error
        self.calls = 0

    def watermarks(self) -> list[PartitionWatermarks]:
        self.calls += 1
        if self.error:
            raise self.error
        return self.marks

    def retention_ms(self) -> int | None:
        if self.error:
            raise self.error
        return 168 * 3_600_000


class FakeLake:
    """Configurable lake: per-method errors, missing tables, and canned data."""

    def __init__(self, now: datetime | None = None):
        self.now = now or datetime.now(UTC)
        self.error: Exception | None = None
        self.missing: set[str] = set()
        self.snapshots: dict[str, TableSnapshot] = {}
        self.offsets: dict[int, int] = {0: 98, 1: 49}
        self.candles: list[CandleRow] = []
        self.prediction_rows: list[dict[str, Any]] = []
        self.hook: Callable[[], None] | None = None

    def _guard(self, table_id: str) -> None:
        if self.hook:
            self.hook()
        if self.error:
            raise self.error
        if table_id in self.missing:
            raise TableMissingError(table_id)

    def table_snapshot(self, spec: TableSpec) -> TableSnapshot:
        self._guard(spec.id)
        return self.snapshots.get(spec.id, TableSnapshot(7, self.now, 1_000, self.now))

    def bronze_max_offsets(self) -> dict[int, int]:
        self._guard("bronze")
        return self.offsets

    def ohlcv(self, product_id: str, since: datetime) -> list[CandleRow]:
        self._guard("gold")
        return self.candles

    def quarantine(self, recent_limit: int) -> QuarantineData:
        self._guard("quarantine")
        return QuarantineData(total=3, reasons={"bad side": 2, "missing price": 1})

    def latest_gaps(self) -> list[GapRow]:
        self._guard("trade_gaps")
        return [GapRow("BTC-USD", 10, 19, 10, self.now)]

    def backfill_counts(self, since: date) -> BackfillCounts:
        self._guard("silver")
        return BackfillCounts(total=1_000, backfilled=25)

    def predictions(self, limit: int) -> list[dict[str, Any]]:
        self._guard("predictions")
        return self.prediction_rows


@pytest.fixture
def settings() -> Settings:
    return Settings(data_source="real", source_timeout_seconds=0.5, cache_ttl_seconds=0)
