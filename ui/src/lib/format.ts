import type { Health, Metric } from "../api/types";

const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });
const integer = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

/** 1,284 · 12.9K · 4.2M. Exact below 10,000 so small counts aren't rounded away. */
export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return Math.abs(value) < 10_000 ? integer.format(value) : compact.format(value);
}

/** Trade volumes are often fractional (0.7356 BTC): keep 4 significant digits below 1,000. */
export function formatVolume(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  if (Math.abs(value) >= 1000) return compact.format(value);
  return value.toLocaleString("en-US", { maximumSignificantDigits: 4 });
}

export function formatRate(perSecond: number | null | undefined): string {
  if (perSecond === null || perSecond === undefined || !Number.isFinite(perSecond)) return "—";
  return `${perSecond < 10 ? perSecond.toFixed(1) : integer.format(perSecond)}/s`;
}

export function formatDuration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || !Number.isFinite(seconds)) return "—";
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86_400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86_400)}d ${Math.floor((s % 86_400) / 3600)}h`;
}

export function formatPercent(ratio: number | null | undefined, digits = 1): string {
  if (ratio === null || ratio === undefined || !Number.isFinite(ratio)) return "—";
  return `${(ratio * 100).toFixed(digits)}%`;
}

export function formatSignedPercent(ratio: number | null | undefined): string {
  if (ratio === null || ratio === undefined || !Number.isFinite(ratio)) return "—";
  const sign = ratio > 0 ? "+" : ratio < 0 ? "−" : "";
  return `${sign}${Math.abs(ratio * 100).toFixed(2)}%`;
}

/** Price precision follows magnitude: 84,123.45 · 2,701.30 · 119.87 · 0.004512 */
export function formatPrice(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  const digits = Math.abs(value) >= 1 ? 2 : 6;
  return value.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

export function formatMetric(metric: Metric): string {
  switch (metric.unit) {
    case "count":
      return formatCount(metric.value);
    case "per_second":
      return formatRate(metric.value);
    case "seconds":
      return formatDuration(metric.value);
    case "hours":
      return metric.value === null ? "—" : `${formatCount(metric.value)}h`;
    case "ratio":
      return formatPercent(metric.value);
    case "version":
      return metric.value === null ? "—" : `v${integer.format(metric.value)}`;
  }
}

export function formatTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

export function formatAgo(iso: string | null | undefined, now: number = Date.now()): string {
  if (!iso) return "—";
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "—";
  return `${formatDuration((now - then) / 1000)} ago`;
}

export const HEALTH_LABEL: Record<Health, string> = {
  ok: "OK",
  stale: "Stale",
  down: "Down",
  inactive: "Inactive",
};
