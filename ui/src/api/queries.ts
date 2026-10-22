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

/** When the API serves a frozen lake snapshot, its timestamp; null for a live pipeline. */
export function useSnapshotAt(): string | null {
  return useMeta().data?.snapshot_at ?? null;
}

/** Poll every `multiplier` × the API's interval; a snapshot never changes, so it's fetched once. */
function usePolling(multiplier = 1) {
  const meta = useMeta().data;
  if (meta?.snapshot_at) return false as const;
  return polling((meta?.poll_interval_ms ?? DEFAULT_POLL_MS) * multiplier);
}

export function usePipeline() {
  const refetchInterval = usePolling();
  return useQuery({
    queryKey: ["pipeline"],
    queryFn: ({ signal }) => apiGet("/api/pipeline", undefined, signal),
    refetchInterval,
  });
}

export function useMarket(product: string | undefined, minutes: number) {
  const refetchInterval = usePolling(3);
  return useQuery({
    queryKey: ["market", product, minutes],
    queryFn: ({ signal }) => apiGet("/api/market/ohlcv", { product: product ?? "", minutes }, signal),
    enabled: product !== undefined,
    // Keep the previous chart on screen while a new product/range loads
    placeholderData: keepPreviousData,
    refetchInterval,
  });
}

export function useQuality() {
  const refetchInterval = usePolling(3);
  return useQuery({
    queryKey: ["quality"],
    queryFn: ({ signal }) => apiGet("/api/quality", undefined, signal),
    refetchInterval,
  });
}

export function usePredictions() {
  const refetchInterval = usePolling(6);
  return useQuery({
    queryKey: ["predictions"],
    queryFn: ({ signal }) => apiGet("/api/ml/predictions", undefined, signal),
    refetchInterval,
  });
}
