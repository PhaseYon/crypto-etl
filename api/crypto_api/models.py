"""Response models. They define the OpenAPI schema the UI's TypeScript types are generated from."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

# ok: healthy · stale: reachable but behind its threshold · down: unreachable or missing
# inactive: not monitored or not created yet (e.g. ML predictions before the first run)
Health = Literal["ok", "stale", "down", "inactive"]

StageId = Literal["coinbase", "producer", "kafka", "bronze", "silver", "gold", "ml"]
TableId = Literal["bronze", "silver", "quarantine", "gold", "trade_gaps", "predictions"]


class Metric(BaseModel):
    label: str
    value: float | None = Field(description="null when unknown")
    unit: Literal["count", "per_second", "seconds", "hours", "ratio", "version"]


class Stage(BaseModel):
    id: StageId
    label: str
    status: Health
    reason: str = Field(description="Why the stage has this status; shown in the UI")
    metrics: list[Metric]


class PartitionOffsets(BaseModel):
    partition: int
    low_watermark: int
    high_watermark: int


class PartitionLag(BaseModel):
    partition: int
    high_watermark: int
    bronze_next_offset: int | None = Field(description="max kafka_offset in bronze + 1; null if none ingested")
    lag: int


class KafkaDetail(BaseModel):
    status: Health
    error: str | None
    topic: str
    partitions: list[PartitionOffsets]
    retained_messages: int | None
    ingest_rate_per_second: float | None = Field(description="From high-watermark deltas; null until two samples")
    retention_hours: float | None


class LagDetail(BaseModel):
    status: Health
    error: str | None
    total_lag: int | None
    partitions: list[PartitionLag]


class TableStatus(BaseModel):
    id: TableId
    label: str
    path: str
    status: Health
    reason: str
    exists: bool
    version: int | None
    last_commit_at: datetime | None
    row_count: int | None
    freshness_seconds: float | None = Field(description="now minus the newest event/ingest timestamp in the table")


class Thresholds(BaseModel):
    kafka_stale_seconds: float
    bronze_stale_seconds: float
    silver_stale_seconds: float
    gold_stale_seconds: float
    predictions_stale_seconds: float
    lag_stale_messages: int


class PipelineResponse(BaseModel):
    generated_at: datetime
    stages: list[Stage]
    kafka: KafkaDetail
    lag: LagDetail
    tables: list[TableStatus]


class Candle(BaseModel):
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None
    trade_count: int


class MarketResponse(BaseModel):
    status: Health
    error: str | None
    product_id: str
    minutes: int
    candles: list[Candle]


class QuarantineReason(BaseModel):
    error: str
    count: int


class QuarantineRecord(BaseModel):
    error: str
    quarantined_at: datetime | None
    kafka_partition: int | None
    kafka_offset: int | None
    value_preview: str = Field(description="First characters of the raw value")


class QuarantineSummary(BaseModel):
    status: Health
    error: str | None
    total: int | None
    reasons: list[QuarantineReason]
    recent: list[QuarantineRecord]


class TradeGap(BaseModel):
    product_id: str
    gap_start_id: int
    gap_end_id: int
    missing_count: int
    detected_at: datetime | None


class GapsSummary(BaseModel):
    status: Health
    error: str | None
    detected_at: datetime | None = Field(description="Time of the latest data-quality scan shown")
    gaps: list[TradeGap]


class BackfillShare(BaseModel):
    status: Health
    error: str | None
    lookback_days: int
    total_rows: int | None
    backfilled_rows: int | None
    share: float | None


class QualityResponse(BaseModel):
    generated_at: datetime
    quarantine: QuarantineSummary
    gaps: GapsSummary
    backfill: BackfillShare


class Prediction(BaseModel):
    product_id: str
    window_start: datetime | None
    score: float | None
    model_version: str | None
    created_at: datetime | None


class PredictionsResponse(BaseModel):
    status: Health
    error: str | None
    reason: str
    predictions: list[Prediction]


class MetaResponse(BaseModel):
    version: str
    data_source: Literal["real", "mock"]
    products: list[str]
    poll_interval_ms: int
    thresholds: Thresholds


class LivenessResponse(BaseModel):
    status: Literal["ok"]
