import { keepPreviousData, useQuery } from "@tanstack/react-query";

import { apiGet } from "./client";

export const DEFAULT_POLL_MS = 5_000;
export const MAX_POLL_MS = 60_000;

/** Poll at the base interval while healthy; back off exponentially while requests fail. */
export function pollInterval(baseMs: number, consecutiveFailures: number): number {
  if (consecutiveFailures <= 0) return baseMs;
  return Math.min(baseMs * 2 ** consecutiveFailures, MAX_POLL_MS);
}

function polling(baseMs: number) {
  return (query: { state: { fetchFailureCount: number } }) =>
    pollInterval(baseMs, query.state.fetchFailureCount);
}

export function useMeta() {
  return useQuery({
    queryKey: ["meta"],
    queryFn: ({ signal }) => apiGet("/api/meta", undefined, signal),
    staleTime: 5 * 60_000,
  });
}

function usePollMs(): number {
  return useMeta().data?.poll_interval_ms ?? DEFAULT_POLL_MS;
}

export function usePipeline() {
  const pollMs = usePollMs();
  return useQuery({
    queryKey: ["pipeline"],
    queryFn: ({ signal }) => apiGet("/api/pipeline", undefined, signal),
    refetchInterval: polling(pollMs),
  });
}

export function useMarket(product: string | undefined, minutes: number) {
  const pollMs = usePollMs();
  return useQuery({
    queryKey: ["market", product, minutes],
    queryFn: ({ signal }) => apiGet("/api/market/ohlcv", { product: product ?? "", minutes }, signal),
    enabled: product !== undefined,
    // Keep the previous chart on screen while a new product/range loads
    placeholderData: keepPreviousData,
    refetchInterval: polling(pollMs * 3),
  });
}

export function useQuality() {
  const pollMs = usePollMs();
  return useQuery({
    queryKey: ["quality"],
    queryFn: ({ signal }) => apiGet("/api/quality", undefined, signal),
    refetchInterval: polling(pollMs * 3),
  });
}

export function usePredictions() {
  const pollMs = usePollMs();
  return useQuery({
    queryKey: ["predictions"],
    queryFn: ({ signal }) => apiGet("/api/ml/predictions", undefined, signal),
    refetchInterval: polling(pollMs * 6),
  });
}
