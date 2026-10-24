"""Assembles API responses from source outcomes. Each part degrades on its own."""

import asyncio
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from . import __version__
from .config import Settings
from .health import (
    IngestRateTracker,
    Verdict,
    compute_lag,
    format_duration,
    freshness_seconds,
    freshness_verdict,
    kafka_ingest_verdict,
)
from .models import (
    BackfillShare,
    Candle,
    GapsSummary,
    Health,
    KafkaDetail,
    LagDetail,
    MarketResponse,
    MetaResponse,
    Metric,
    PartitionOffsets,
    PipelineResponse,
    Prediction,
    PredictionsResponse,
    QualityResponse,
    QuarantineReason,
    QuarantineRecord,
    QuarantineSummary,
    Stage,
    TableStatus,
    Thresholds,
    TradeGap,
)
from .runner import Outcome, SourceRunner
from .sources.base import TABLE_SPECS, KafkaSource, LakeSource, PartitionWatermarks, TableSnapshot

SEVERITY: dict[Health, int] = {"inactive": 0, "ok": 1, "stale": 2, "down": 3}
# probability and model_run_id are the contract columns (docs/CONTRACTS.md); the rest are tolerated fallbacks
SCORE_COLUMNS = ("probability", "p_up", "score", "prob_up", "prediction", "y_pred")
VERSION_COLUMNS = ("model_run_id", "model_version", "model")
NOT_DEPLOYED = "not deployed: serving a snapshot of the lake"
RECENT_QUARANTINE = 10
PREDICTIONS_LIMIT = 200


def worst(*verdicts: Verdict) -> Verdict:
    return max(verdicts, key=lambda v: SEVERITY[v.status])


class PipelineService:
    def __init__(self, settings: Settings, kafka: KafkaSource, lake: LakeSource, runner: SourceRunner):
        self._s = settings
        self._kafka = kafka
        self._lake = lake
        self._runner = runner
        self._tracker = IngestRateTracker()

    def _now(self) -> datetime:
        return self._s.snapshot_at or datetime.now(UTC)

    # ---- source reads (cached, timed out, never raising) ----

    def _load_watermarks(self) -> list[PartitionWatermarks]:
        marks = self._kafka.watermarks()
        # Sample only on real reads (not cache hits) so the rate reflects actual time between reads
        self._tracker.observe(time.time(), sum(m.high for m in marks))
        return marks

    async def _watermarks(self) -> Outcome:
        return await self._runner.call("kafka.watermarks", self._load_watermarks)

    async def _retention(self) -> Outcome:
        return await self._runner.call("kafka.retention", self._kafka.retention_ms, ttl=60)

    async def _snapshot(self, table_id: str) -> Outcome:
        return await self._runner.call(f"lake.snapshot.{table_id}", self._lake.table_snapshot, TABLE_SPECS[table_id])

    # ---- endpoints ----

    def meta(self) -> MetaResponse:
        s = self._s
        return MetaResponse(
            version=__version__,
            data_source=s.data_source,
            snapshot_at=s.snapshot_at,
            products=s.products,
            poll_interval_ms=s.poll_interval_ms,
            thresholds=Thresholds(
                kafka_stale_seconds=s.kafka_stale_seconds,
                bronze_stale_seconds=s.bronze_stale_seconds,
                silver_stale_seconds=s.silver_stale_seconds,
                gold_stale_seconds=s.gold_stale_seconds,
                predictions_stale_seconds=s.predictions_stale_seconds,
                lag_stale_messages=s.lag_stale_messages,
            ),
        )

    async def pipeline(self) -> PipelineResponse:
        table_ids = list(TABLE_SPECS)
        if self._s.snapshot_at is not None:
            # No Kafka in a snapshot: don't even try to reach it
            snapshots = await asyncio.gather(*(self._snapshot(t) for t in table_ids))
            kafka = self._not_deployed_kafka()
            lag = LagDetail(status="inactive", error=None, total_lag=None, partitions=[])
        else:
            marks, retention, offsets, *snapshots = await asyncio.gather(
                self._watermarks(),
                self._retention(),
                self._runner.call("lake.bronze_offsets", self._lake.bronze_max_offsets),
                *(self._snapshot(t) for t in table_ids),
            )
            kafka = self._kafka_detail(marks, retention)
            lag = self._lag_detail(marks, offsets)
        now = self._now()
        tables = {t: self._table_status(t, outcome, now) for t, outcome in zip(table_ids, snapshots, strict=True)}
        return PipelineResponse(
            generated_at=now,
            stages=self._stages(kafka, lag, tables),
            kafka=kafka,
            lag=lag,
            tables=list(tables.values()),
        )

    async def market(self, product_id: str, minutes: int) -> MarketResponse:
        since = self._now() - timedelta(minutes=minutes)
        # Bucket the start to the minute so the cache key is stable across polls
        since = since.replace(second=0, microsecond=0)
        outcome = await self._runner.call(
            f"lake.ohlcv.{product_id}.{minutes}.{since.isoformat()}", self._lake.ohlcv, product_id, since
        )
        base = {"product_id": product_id, "minutes": minutes}
        if outcome.missing:
            return MarketResponse(status="inactive", error="gold/ohlcv_1m doesn't exist yet", candles=[], **base)
        if outcome.error:
            return MarketResponse(status="down", error=outcome.error, candles=[], **base)
        candles = [
            Candle(
                time=c.window_start,
                open=c.open,
                high=c.high,
                low=c.low,
                close=c.close,
                volume=c.volume,
                vwap=c.vwap,
                trade_count=c.trade_count,
            )
            for c in outcome.value
        ]
        return MarketResponse(status="ok", error=None, candles=candles, **base)

    async def quality(self) -> QualityResponse:
        since = (self._now() - timedelta(days=self._s.backfill_lookback_days)).date()
        quarantine, gaps, backfill = await asyncio.gather(
            self._runner.call("lake.quarantine", self._lake.quarantine, RECENT_QUARANTINE),
            self._runner.call("lake.gaps", self._lake.latest_gaps),
            self._runner.call(f"lake.backfill.{since.isoformat()}", self._lake.backfill_counts, since),
        )
        return QualityResponse(
            generated_at=self._now(),
            quarantine=_quarantine_summary(quarantine),
            gaps=_gaps_summary(gaps),
            backfill=_backfill_share(backfill, self._s.backfill_lookback_days),
        )

    async def predictions(self) -> PredictionsResponse:
        outcome = await self._runner.call("lake.predictions", self._lake.predictions, PREDICTIONS_LIMIT)
        if outcome.missing:
            return PredictionsResponse(
                status="inactive", error=None, reason="ML predictions not available yet", predictions=[]
            )
        if outcome.error:
            return PredictionsResponse(
                status="down", error=outcome.error, reason="couldn't read ml/predictions", predictions=[]
            )
        predictions = [p for p in (_to_prediction(row) for row in outcome.value) if p is not None]
        if not predictions:
            return PredictionsResponse(
                status="inactive", error=None, reason="ml/predictions has no rows yet", predictions=[]
            )
        newest = max(
            (p.created_at or p.window_start for p in predictions if p.created_at or p.window_start), default=None
        )
        verdict = freshness_verdict(
            freshness_seconds(newest, self._now()), self._s.predictions_stale_seconds, "newest prediction"
        )
        return PredictionsResponse(status=verdict.status, error=None, reason=verdict.reason, predictions=predictions)

    # ---- assembly ----

    def _not_deployed_kafka(self) -> KafkaDetail:
        return KafkaDetail(
            status="inactive",
            error=None,
            topic=self._s.kafka_topic,
            partitions=[],
            retained_messages=None,
            ingest_rate_per_second=None,
            retention_hours=None,
        )

    def _kafka_detail(self, marks: Outcome, retention: Outcome) -> KafkaDetail:
        retention_hours = retention.value / 3_600_000 if retention.ok and retention.value is not None else None
        if not marks.ok:
            return KafkaDetail(
                status="down",
                error=marks.error,
                topic=self._s.kafka_topic,
                partitions=[],
                retained_messages=None,
                ingest_rate_per_second=None,
                retention_hours=retention_hours,
            )
        verdict = kafka_ingest_verdict(self._tracker, time.time(), self._s.kafka_stale_seconds)
        return KafkaDetail(
            status=verdict.status,
            error=None,
            topic=self._s.kafka_topic,
            partitions=[
                PartitionOffsets(partition=m.partition, low_watermark=m.low, high_watermark=m.high) for m in marks.value
            ],
            retained_messages=sum(m.high - m.low for m in marks.value),
            ingest_rate_per_second=self._tracker.rate(),
            retention_hours=retention_hours,
        )

    def _lag_detail(self, marks: Outcome, offsets: Outcome) -> LagDetail:
        if not marks.ok:
            return LagDetail(status="down", error=f"Kafka unreachable: {marks.error}", total_lag=None, partitions=[])
        if offsets.error:
            return LagDetail(status="down", error=f"bronze unreadable: {offsets.error}", total_lag=None, partitions=[])
        # A missing bronze table means nothing has been ingested: the whole retained topic is lag
        partitions = compute_lag(marks.value, {} if offsets.missing else offsets.value)
        total = sum(p.lag for p in partitions)
        status: Health = "stale" if total > self._s.lag_stale_messages else "ok"
        return LagDetail(status=status, error=None, total_lag=total, partitions=partitions)

    def _table_status(self, table_id: str, outcome: Outcome, now: datetime) -> TableStatus:
        spec = TABLE_SPECS[table_id]
        base: dict[str, Any] = {"id": table_id, "label": spec.label, "path": spec.path}
        if outcome.missing:
            status: Health = "inactive" if spec.optional else "down"
            return TableStatus(
                **base,
                status=status,
                reason="not created yet",
                exists=False,
                version=None,
                last_commit_at=None,
                row_count=None,
                freshness_seconds=None,
            )
        if outcome.error:
            return TableStatus(
                **base,
                status="down",
                reason=outcome.error,
                exists=False,
                version=None,
                last_commit_at=None,
                row_count=None,
                freshness_seconds=None,
            )
        snap: TableSnapshot = outcome.value
        age = freshness_seconds(snap.newest_at, now)
        verdict = self._table_verdict(table_id, snap, age)
        return TableStatus(
            **base,
            status=verdict.status,
            reason=verdict.reason,
            exists=True,
            version=snap.version,
            last_commit_at=snap.last_commit_at,
            row_count=snap.row_count,
            freshness_seconds=age,
        )

    def _table_verdict(self, table_id: str, snap: TableSnapshot, age: float | None) -> Verdict:
        thresholds = {
            "bronze": self._s.bronze_stale_seconds,
            "silver": self._s.silver_stale_seconds,
            "gold": self._s.gold_stale_seconds,
            "predictions": self._s.predictions_stale_seconds,
        }
        if table_id in thresholds:
            return freshness_verdict(age, thresholds[table_id])
        # Quarantine and gap tables are quality signals, not freshness-critical
        if snap.row_count is None:
            rows = "row count unknown"
        else:
            rows = f"{snap.row_count:,} row{'' if snap.row_count == 1 else 's'}"
        when = "" if age is None else f", newest {format_duration(age)} ago"
        return Verdict("ok", f"{rows}{when}")

    def _stages(self, kafka: KafkaDetail, lag: LagDetail, tables: dict[str, TableStatus]) -> list[Stage]:
        def table_metrics(t: TableStatus) -> list[Metric]:
            return [
                Metric(label="Rows", value=t.row_count, unit="count"),
                Metric(label="Freshness", value=t.freshness_seconds, unit="seconds"),
                Metric(label="Delta version", value=t.version, unit="version"),
            ]

        if kafka.status == "inactive":
            producer = Verdict("inactive", NOT_DEPLOYED)
        elif kafka.status == "down":
            producer = Verdict("inactive", "unknown: Kafka is unreachable")
        else:
            producer = Verdict(kafka.status, f"inferred from Kafka ingest: {kafka_reason(kafka)}")

        bronze = tables["bronze"]
        if lag.error:
            lag_reason = lag.error
        elif lag.total_lag is None:
            lag_reason = NOT_DEPLOYED
        else:
            lag_reason = f"{lag.total_lag:,} messages behind Kafka (threshold {self._s.lag_stale_messages:,})"
        lag_verdict = Verdict(lag.status, lag_reason)
        bronze_verdict = worst(Verdict(bronze.status, bronze.reason), lag_verdict)
        if lag.status == "inactive":
            bronze_verdict = Verdict(bronze.status, bronze.reason)
        elif bronze.status == "ok" and lag.status == "ok":
            bronze_verdict = Verdict("ok", f"{bronze.reason}; {lag_verdict.reason}")

        silver, gold, quarantine = tables["silver"], tables["gold"], tables["quarantine"]
        predictions = tables["predictions"]
        ml_reason = (
            "ML predictions not available yet"
            if not predictions.exists and predictions.status == "inactive"
            else predictions.reason
        )
        return [
            Stage(
                id="coinbase",
                label="Coinbase WebSocket",
                status="inactive",
                reason="external source; not monitored directly",
                metrics=[],
            ),
            Stage(
                id="producer",
                label="Producer",
                status=producer.status,
                reason=producer.reason,
                metrics=[Metric(label="Ingest rate", value=kafka.ingest_rate_per_second, unit="per_second")],
            ),
            Stage(
                id="kafka",
                label=f"Kafka {kafka.topic}",
                status=kafka.status,
                reason=kafka.error or kafka_reason(kafka),
                metrics=[
                    Metric(label="Retained messages", value=kafka.retained_messages, unit="count"),
                    Metric(label="Ingest rate", value=kafka.ingest_rate_per_second, unit="per_second"),
                    Metric(label="Retention", value=kafka.retention_hours, unit="hours"),
                ],
            ),
            Stage(
                id="bronze",
                label="Bronze",
                status=bronze_verdict.status,
                reason=bronze_verdict.reason,
                metrics=[*table_metrics(bronze)[:2], Metric(label="Lag", value=lag.total_lag, unit="count")],
            ),
            Stage(
                id="silver",
                label="Silver",
                status=silver.status,
                reason=silver.reason,
                metrics=[
                    *table_metrics(silver)[:2],
                    Metric(label="Quarantined", value=quarantine.row_count, unit="count"),
                ],
            ),
            Stage(id="gold", label="Gold", status=gold.status, reason=gold.reason, metrics=table_metrics(gold)[:2]),
            Stage(
                id="ml",
                label="ML",
                status=predictions.status,
                reason=ml_reason,
                metrics=table_metrics(predictions)[:2],
            ),
        ]


def kafka_reason(kafka: KafkaDetail) -> str:
    if kafka.status == "inactive":
        return NOT_DEPLOYED
    if kafka.status == "stale":
        return "no new messages within the threshold"
    if kafka.ingest_rate_per_second is None:
        return "reachable; measuring ingest rate"
    return f"ingesting {kafka.ingest_rate_per_second:.1f} msg/s"


def _quarantine_summary(outcome: Outcome) -> QuarantineSummary:
    if outcome.missing:
        return QuarantineSummary(status="inactive", error=None, total=0, reasons=[], recent=[])
    if outcome.error:
        return QuarantineSummary(status="down", error=outcome.error, total=None, reasons=[], recent=[])
    data = outcome.value
    return QuarantineSummary(
        status="ok",
        error=None,
        total=data.total,
        reasons=[
            QuarantineReason(error=error, count=count)
            for error, count in sorted(data.reasons.items(), key=lambda item: (-item[1], item[0]))
        ],
        recent=[
            QuarantineRecord(
                error=r.error,
                quarantined_at=r.quarantined_at,
                kafka_partition=r.kafka_partition,
                kafka_offset=r.kafka_offset,
                value_preview=r.value,
            )
            for r in data.recent
        ],
    )


def _gaps_summary(outcome: Outcome) -> GapsSummary:
    if outcome.missing:
        return GapsSummary(status="inactive", error=None, detected_at=None, gaps=[])
    if outcome.error:
        return GapsSummary(status="down", error=outcome.error, detected_at=None, gaps=[])
    gaps = [
        TradeGap(
            product_id=g.product_id,
            gap_start_id=g.gap_start_id,
            gap_end_id=g.gap_end_id,
            missing_count=g.missing_count,
            detected_at=g.detected_at,
        )
        for g in outcome.value
    ]
    detected = max((g.detected_at for g in gaps if g.detected_at), default=None)
    return GapsSummary(status="ok", error=None, detected_at=detected, gaps=gaps)


def _backfill_share(outcome: Outcome, lookback_days: int) -> BackfillShare:
    base = {"lookback_days": lookback_days}
    if outcome.missing:
        return BackfillShare(status="inactive", error=None, total_rows=None, backfilled_rows=None, share=None, **base)
    if outcome.error:
        return BackfillShare(
            status="down", error=outcome.error, total_rows=None, backfilled_rows=None, share=None, **base
        )
    counts = outcome.value
    share = counts.backfilled / counts.total if counts.total else None
    return BackfillShare(
        status="ok", error=None, total_rows=counts.total, backfilled_rows=counts.backfilled, share=share, **base
    )


def _to_prediction(row: dict[str, Any]) -> Prediction | None:
    """Maps an ml/predictions row, tolerating missing or differently named columns."""
    product = row.get("product_id")
    if not product:
        return None
    score = next((row[c] for c in SCORE_COLUMNS if row.get(c) is not None), None)
    try:
        score = float(score) if score is not None else None
    except (TypeError, ValueError):
        score = None
    version = next((row[c] for c in VERSION_COLUMNS if row.get(c) is not None), None)
    target = row.get("target")
    horizon = row.get("horizon_minutes")
    return Prediction(
        product_id=str(product),
        window_start=_utc(row.get("window_start")),
        score=score,
        target=str(target) if target else None,
        horizon_minutes=horizon if isinstance(horizon, int) and not isinstance(horizon, bool) else None,
        model_version=None if version is None else str(version),
        created_at=_utc(row.get("created_at")),
    )


def _utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
