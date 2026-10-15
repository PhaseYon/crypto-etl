"""Deterministic, contract-shaped fake data so the dashboard works before the lake exists.

Values are derived from the wall clock, so repeated polls see a pipeline that is moving:
watermarks grow, candles extend, freshness stays low.
"""

import math
import random
import time
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from typing import Any

from .base import (
    BackfillCounts,
    CandleRow,
    GapRow,
    PartitionWatermarks,
    QuarantineData,
    QuarantineRow,
    TableSnapshot,
    TableSpec,
)

EPOCH = 1_767_225_600  # 2026-01-01T00:00:00Z
PARTITION_RATES = (1.4, 0.6, 2.1)  # messages per second per Kafka partition
RETENTION_SECONDS = 168 * 3600
BRONZE_LAG = (3, 0, 7)  # messages bronze trails Kafka by, per partition
BASE_PRICES = {"BTC-USD": 84_000.0, "ETH-USD": 2_700.0, "SOL-USD": 120.0}
MINUTE_VOLATILITY = 0.0009


class MockUnavailableError(ConnectionError):
    pass


def _now() -> float:
    return time.time()


class MockKafkaSource:
    def watermarks(self) -> list[PartitionWatermarks]:
        elapsed = _now() - EPOCH
        result = []
        for partition, rate in enumerate(PARTITION_RATES):
            high = int(rate * elapsed)
            low = max(0, high - int(rate * RETENTION_SECONDS))
            result.append(PartitionWatermarks(partition, low, high))
        return result

    def retention_ms(self) -> int | None:
        return RETENTION_SECONDS * 1000


class MockLakeSource:
    def __init__(self, products: list[str], degraded: bool = False):
        self._products = products
        self._degraded = degraded

    def _check(self) -> None:
        if self._degraded:
            raise MockUnavailableError("mock scenario 'degraded': object store unreachable")

    def table_snapshot(self, spec: TableSpec) -> TableSnapshot:
        self._check()
        now = datetime.fromtimestamp(_now(), tz=UTC)
        total_rate = sum(PARTITION_RATES)
        elapsed = _now() - EPOCH
        profiles: dict[str, tuple[int, float, int]] = {
            # id: (rows, seconds since newest row, commits per hour)
            "bronze": (int(total_rate * elapsed), 4, 360),
            "silver": (int(total_rate * elapsed * 0.998), 9, 360),
            "quarantine": (37, 1_260, 2),
            "gold": (int(elapsed / 60) * len(self._products), 14, 360),
            "trade_gaps": (1, 1_200, 4),
            "predictions": (len(self._products) * 30, 50, 60),
        }
        rows, age, commits_per_hour = profiles.get(spec.id, (0, 60, 1))
        version = int(elapsed / 3600 * commits_per_hour)
        return TableSnapshot(
            version=version,
            last_commit_at=now - timedelta(seconds=min(age, 3600 / commits_per_hour)),
            row_count=rows,
            newest_at=now - timedelta(seconds=age),
        )

    def bronze_max_offsets(self) -> dict[int, int]:
        self._check()
        marks = MockKafkaSource().watermarks()
        return {m.partition: m.high - 1 - lag for m, lag in zip(marks, BRONZE_LAG, strict=True)}

    def ohlcv(self, product_id: str, since: datetime) -> list[CandleRow]:
        self._check()
        now_minute = int(_now() // 60)
        first_minute = int(since.timestamp() // 60)
        rows: list[CandleRow] = []
        for day in range(first_minute // 1440, now_minute // 1440 + 1):
            for minute, candle in _day_candles(product_id, day):
                if first_minute <= minute <= now_minute:
                    rows.append(candle)
        return rows

    def quarantine(self, recent_limit: int) -> QuarantineData:
        self._check()
        now = datetime.fromtimestamp(_now(), tz=UTC)
        reasons = {"unparseable time": 22, "missing price": 12, "invalid side": 3}
        recent = [
            QuarantineRow(
                error=error,
                quarantined_at=now - timedelta(minutes=21 + 37 * i),
                kafka_partition=i % 3,
                kafka_offset=1_000_000 + 7_919 * i,
                value=value,
            )
            for i, (error, value) in enumerate(
                [
                    ("unparseable time", '{"type":"match","trade_id":1,"time":"not-a-time","price":"1"}'),
                    ("missing price", '{"type":"match","trade_id":2,"product_id":"ETH-USD","size":"0.1"}'),
                    ("invalid side", '{"type":"match","trade_id":3,"side":"hold","price":"2"}'),
                ]
                * 3
            )
        ]
        return QuarantineData(total=sum(reasons.values()), reasons=reasons, recent=recent[:recent_limit])

    def latest_gaps(self) -> list[GapRow]:
        self._check()
        detected = datetime.fromtimestamp(_now(), tz=UTC) - timedelta(minutes=20)
        return [GapRow("SOL-USD", 357_825_623, 357_825_682, 60, detected)]

    def backfill_counts(self, since: date) -> BackfillCounts:
        self._check()
        days = max(1, (datetime.fromtimestamp(_now(), tz=UTC).date() - since).days)
        total = int(sum(PARTITION_RATES) * 86_400 * days)
        return BackfillCounts(total=total, backfilled=int(total * 0.004))

    def predictions(self, limit: int) -> list[dict[str, Any]]:
        self._check()
        now_minute = int(_now() // 60)
        rows = []
        for minute in range(now_minute, now_minute - 30, -1):
            for product in self._products:
                rng = random.Random(f"pred:{product}:{minute}")
                rows.append(
                    {
                        "product_id": product,
                        "window_start": datetime.fromtimestamp(minute * 60, tz=UTC),
                        "score": round(0.5 + 0.12 * math.sin(minute / 7) + rng.uniform(-0.05, 0.05), 4),
                        "model_version": "3",
                        "created_at": datetime.fromtimestamp(minute * 60 + 10, tz=UTC),
                    }
                )
        return rows[:limit]


@lru_cache(maxsize=64)
def _day_candles(product_id: str, day: int) -> tuple[tuple[int, CandleRow], ...]:
    """One UTC day of 1-minute candles, deterministic per (product, day)."""
    rng = random.Random(f"ohlcv:{product_id}:{day}")
    base = BASE_PRICES.get(product_id, 100.0)
    price = base * math.exp(0.03 * math.sin(day / 3))  # day anchor drifts slowly
    candles = []
    for i in range(1440):
        minute = day * 1440 + i
        if rng.random() < 0.02:
            continue  # minutes without trades have no row (see contract)
        open_ = price
        close = open_ * math.exp(rng.gauss(0, MINUTE_VOLATILITY))
        high = max(open_, close) * (1 + abs(rng.gauss(0, MINUTE_VOLATILITY / 2)))
        low = min(open_, close) * (1 - abs(rng.gauss(0, MINUTE_VOLATILITY / 2)))
        volume = abs(rng.gauss(4, 2)) * 1000 / base * 10
        candles.append(
            (
                minute,
                CandleRow(
                    window_start=datetime.fromtimestamp(minute * 60, tz=UTC),
                    open=round(open_, 4),
                    high=round(high, 4),
                    low=round(low, 4),
                    close=round(close, 4),
                    volume=round(volume, 6),
                    vwap=round((open_ + close + high + low) / 4, 4),
                    trade_count=max(1, int(rng.gauss(120, 40))),
                ),
            )
        )
        price = close
    return tuple(candles)
