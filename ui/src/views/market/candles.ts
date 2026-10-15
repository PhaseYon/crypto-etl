import type { CandlestickData, HistogramData, UTCTimestamp } from "lightweight-charts";

import type { Candle } from "../../api/types";

export const RANGES = [
  { minutes: 60, label: "1h" },
  { minutes: 360, label: "6h" },
  { minutes: 1440, label: "24h" },
  { minutes: 10_080, label: "7d" },
] as const;

export function toTimestamp(iso: string): UTCTimestamp {
  return Math.floor(Date.parse(iso) / 1000) as UTCTimestamp;
}

/** Chart rows sorted by time with duplicates dropped (the chart library rejects both). */
export function toCandleData(candles: Candle[]): CandlestickData<UTCTimestamp>[] {
  const byTime = new Map<number, CandlestickData<UTCTimestamp>>();
  for (const c of candles) {
    const time = toTimestamp(c.time);
    if (Number.isNaN(time)) continue;
    byTime.set(time, { time, open: c.open, high: c.high, low: c.low, close: c.close });
  }
  return [...byTime.values()].sort((a, b) => a.time - b.time);
}

export function toVolumeData(candles: Candle[]): HistogramData<UTCTimestamp>[] {
  const byTime = new Map<number, HistogramData<UTCTimestamp>>();
  for (const c of candles) {
    const time = toTimestamp(c.time);
    if (Number.isNaN(time)) continue;
    byTime.set(time, { time, value: c.volume });
  }
  return [...byTime.values()].sort((a, b) => a.time - b.time);
}

export type MarketSummary = {
  last: number;
  change: number;
  high: number;
  low: number;
  volume: number;
  trades: number;
};

export function summarize(candles: Candle[]): MarketSummary | null {
  const first = candles[0];
  const last = candles[candles.length - 1];
  if (!first || !last) return null;
  let high = -Infinity;
  let low = Infinity;
  let volume = 0;
  let trades = 0;
  for (const c of candles) {
    high = Math.max(high, c.high);
    low = Math.min(low, c.low);
    volume += c.volume;
    trades += c.trade_count;
  }
  return {
    last: last.close,
    change: first.open === 0 ? 0 : last.close / first.open - 1,
    high,
    low,
    volume,
    trades,
  };
}
