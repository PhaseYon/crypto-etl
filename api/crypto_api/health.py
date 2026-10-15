"""Pure health logic: lag, freshness, ingest rate. No I/O, so it's unit-testable in isolation."""

from collections import deque
from dataclasses import dataclass
from datetime import datetime

from .models import Health, PartitionLag
from .sources.base import PartitionWatermarks


def compute_lag(watermarks: list[PartitionWatermarks], bronze_max_offsets: dict[int, int]) -> list[PartitionLag]:
    """Messages in Kafka not yet in bronze, per partition.

    Spark doesn't commit offsets to Kafka, so the bronze table's own kafka_offset column is the
    bookmark: lag = high watermark - (max offset in bronze + 1). A partition bronze has never
    read counts everything still retained (high - low).
    """
    result = []
    for mark in watermarks:
        max_offset = bronze_max_offsets.get(mark.partition)
        if max_offset is None:
            next_offset = None
            lag = mark.high - mark.low
        else:
            next_offset = max_offset + 1
            lag = mark.high - max(next_offset, mark.low)
        result.append(
            PartitionLag(
                partition=mark.partition,
                high_watermark=mark.high,
                bronze_next_offset=next_offset,
                lag=max(0, lag),
            )
        )
    return result


def freshness_seconds(newest_at: datetime | None, now: datetime) -> float | None:
    if newest_at is None:
        return None
    return max(0.0, (now - newest_at).total_seconds())


@dataclass(frozen=True)
class Verdict:
    status: Health
    reason: str


def freshness_verdict(age: float | None, stale_after: float, what: str = "newest row") -> Verdict:
    if age is None:
        return Verdict("stale", f"no timestamp to measure freshness of the {what}")
    if age > stale_after:
        return Verdict("stale", f"{what} is {format_duration(age)} old (threshold {format_duration(stale_after)})")
    return Verdict("ok", f"{what} is {format_duration(age)} old")


def format_duration(seconds: float) -> str:
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    if seconds < 86_400:
        return f"{seconds // 3600}h {seconds % 3600 // 60}m"
    return f"{seconds // 86_400}d {seconds % 86_400 // 3600}h"


class IngestRateTracker:
    """Ingest rate and stall detection from successive high-watermark samples.

    Kafka has no built-in "messages per second", so the API samples the sum of high
    watermarks on each (cached) read and derives the rate over a sliding window.
    """

    def __init__(self, window_seconds: float = 60.0):
        self._window = window_seconds
        self._samples: deque[tuple[float, int]] = deque()
        self._last_change_at: float | None = None
        self._first_sample_at: float | None = None

    def observe(self, at: float, total_high: int) -> None:
        if self._samples and total_high < self._samples[-1][1]:
            # Watermarks went backwards: topic recreated. Start over.
            self._samples.clear()
            self._last_change_at = None
            self._first_sample_at = None
        if self._first_sample_at is None:
            self._first_sample_at = at
        if not self._samples or total_high != self._samples[-1][1]:
            self._last_change_at = at
        self._samples.append((at, total_high))
        while len(self._samples) > 2 and at - self._samples[0][0] > self._window:
            self._samples.popleft()

    def rate(self) -> float | None:
        if len(self._samples) < 2:
            return None
        (t0, v0), (t1, v1) = self._samples[0], self._samples[-1]
        if t1 <= t0:
            return None
        return (v1 - v0) / (t1 - t0)

    def seconds_since_change(self, now: float) -> float | None:
        """None until there's been enough observation time to judge a stall."""
        if self._last_change_at is None or self._first_sample_at is None:
            return None
        return now - self._last_change_at

    def observed_for(self, now: float) -> float:
        return 0.0 if self._first_sample_at is None else now - self._first_sample_at


def kafka_ingest_verdict(tracker: IngestRateTracker, now: float, stale_after: float) -> Verdict:
    since_change = tracker.seconds_since_change(now)
    if since_change is None:
        return Verdict("ok", "reachable; measuring ingest rate")
    if since_change > stale_after and tracker.observed_for(now) >= stale_after:
        return Verdict("stale", f"no new messages for {format_duration(since_change)}")
    rate = tracker.rate()
    if rate is None:
        return Verdict("ok", "reachable; measuring ingest rate")
    return Verdict("ok", f"ingesting {rate:.1f} msg/s")
