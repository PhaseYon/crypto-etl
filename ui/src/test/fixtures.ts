import type {
  Candle,
  MarketResponse,
  MetaResponse,
  PipelineResponse,
  PredictionsResponse,
  QualityResponse,
  Stage,
} from "../api/types";

export const NOW = "2026-10-15T12:00:00Z";

export const meta: MetaResponse = {
  version: "0.1.0",
  data_source: "mock",
  snapshot_at: null,
  products: ["BTC-USD", "ETH-USD", "SOL-USD"],
  poll_interval_ms: 5000,
  thresholds: {
    kafka_stale_seconds: 120,
    bronze_stale_seconds: 300,
    silver_stale_seconds: 300,
    gold_stale_seconds: 300,
    predictions_stale_seconds: 7200,
    lag_stale_messages: 10000,
  },
};

const stage = (id: Stage["id"], label: string, status: Stage["status"], reason: string): Stage => ({
  id,
  label,
  status,
  reason,
  metrics: [
    { label: "Rows", value: 12_345, unit: "count" },
    { label: "Freshness", value: 4, unit: "seconds" },
  ],
});

export function pipeline(overrides: Partial<PipelineResponse> = {}): PipelineResponse {
  return {
    generated_at: NOW,
    stages: [
      {
        ...stage("coinbase", "Coinbase WebSocket", "inactive", "external source; not monitored directly"),
        metrics: [],
      },
      stage("producer", "Producer", "ok", "inferred from Kafka ingest: ingesting 4.1 msg/s"),
      stage("kafka", "Kafka raw.trades", "ok", "ingesting 4.1 msg/s"),
      stage("bronze", "Bronze", "ok", "newest row is 4s old"),
      stage("silver", "Silver", "stale", "newest row is 9m 0s old (threshold 5m 0s)"),
      stage("gold", "Gold", "down", "OSError: connection refused"),
      stage("ml", "ML", "inactive", "ML predictions not available yet"),
    ],
    kafka: {
      status: "ok",
      error: null,
      topic: "raw.trades",
      partitions: [
        { partition: 0, low_watermark: 100, high_watermark: 2_100 },
        { partition: 1, low_watermark: 0, high_watermark: 950 },
      ],
      retained_messages: 2_950,
      ingest_rate_per_second: 4.1,
      retention_hours: 168,
    },
    lag: {
      status: "ok",
      error: null,
      total_lag: 7,
      partitions: [
        { partition: 0, high_watermark: 2_100, bronze_next_offset: 2_093, lag: 7 },
        { partition: 1, high_watermark: 950, bronze_next_offset: 950, lag: 0 },
      ],
    },
    tables: [
      {
        id: "bronze",
        label: "Bronze trades",
        path: "bronze/trades",
        status: "ok",
        reason: "newest row is 4s old",
        exists: true,
        version: 42,
        last_commit_at: NOW,
        row_count: 120_000,
        freshness_seconds: 4,
      },
      {
        id: "predictions",
        label: "ML predictions",
        path: "ml/predictions",
        status: "inactive",
        reason: "not created yet",
        exists: false,
        version: null,
        last_commit_at: null,
        row_count: null,
        freshness_seconds: null,
      },
    ],
    ...overrides,
  };
}

export function candles(count: number, start = Date.parse(NOW) - count * 60_000): Candle[] {
  return Array.from({ length: count }, (_, i) => {
    const open = 100 + i;
    return {
      time: new Date(start + i * 60_000).toISOString(),
      open,
      high: open + 2,
      low: open - 1,
      close: open + (i % 2 === 0 ? 1 : -0.5),
      volume: 3 + i,
      vwap: open + 0.5,
      trade_count: 10 + i,
    };
  });
}

export function market(overrides: Partial<MarketResponse> = {}): MarketResponse {
  return {
    status: "ok",
    error: null,
    product_id: "BTC-USD",
    minutes: 360,
    candles: candles(5),
    ...overrides,
  };
}

export function quality(overrides: Partial<QualityResponse> = {}): QualityResponse {
  return {
    generated_at: NOW,
    quarantine: {
      status: "ok",
      error: null,
      total: 37,
      reasons: [
        { error: "unparseable time", count: 22 },
        { error: "missing price", count: 12 },
        { error: "invalid side", count: 3 },
      ],
      recent: [
        {
          error: "unparseable time",
          quarantined_at: NOW,
          kafka_partition: 0,
          kafka_offset: 1234,
          value_preview: '{"type":"match","time":"nope"}',
        },
      ],
    },
    gaps: {
      status: "ok",
      error: null,
      detected_at: NOW,
      gaps: [
        { product_id: "SOL-USD", gap_start_id: 100, gap_end_id: 159, missing_count: 60, detected_at: NOW },
      ],
    },
    backfill: {
      status: "ok",
      error: null,
      lookback_days: 1,
      total_rows: 100_000,
      backfilled_rows: 400,
      share: 0.004,
    },
    ...overrides,
  };
}

export function predictions(overrides: Partial<PredictionsResponse> = {}): PredictionsResponse {
  const rows = ["BTC-USD", "ETH-USD"].flatMap((product, p) =>
    [0, 1, 2].map((i) => ({
      product_id: product,
      window_start: new Date(Date.parse(NOW) - i * 60_000).toISOString(),
      score: 0.5 + p * 0.1 - i * 0.01,
      target: "direction",
      horizon_minutes: 5,
      model_version: "3",
      created_at: new Date(Date.parse(NOW) - i * 60_000 + 10_000).toISOString(),
    })),
  );
  return {
    status: "ok",
    error: null,
    reason: "newest prediction is 50s old",
    predictions: rows,
    ...overrides,
  };
}
