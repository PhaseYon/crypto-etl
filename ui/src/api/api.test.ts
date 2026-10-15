import { describe, expect, it, vi } from "vitest";

import { stubApi } from "../test/render";
import { ApiError, apiGet } from "./client";
import { MAX_POLL_MS, pollInterval } from "./queries";

describe("pollInterval", () => {
  it("polls at the base rate while healthy", () => {
    expect(pollInterval(5_000, 0)).toBe(5_000);
  });

  it("backs off exponentially on consecutive failures, capped", () => {
    expect(pollInterval(5_000, 1)).toBe(10_000);
    expect(pollInterval(5_000, 2)).toBe(20_000);
    expect(pollInterval(5_000, 10)).toBe(MAX_POLL_MS);
  });
});

describe("apiGet", () => {
  it("sends query parameters and returns the JSON body", async () => {
    const fetchMock = stubApi({ "/api/market/ohlcv": { status: "ok" } });
    await expect(apiGet("/api/market/ohlcv", { product: "BTC-USD", minutes: 60 })).resolves.toEqual({
      status: "ok",
    });
    const url = new URL(fetchMock.mock.calls[0]?.[0] as string);
    expect(url.searchParams.get("product")).toBe("BTC-USD");
    expect(url.searchParams.get("minutes")).toBe("60");
  });

  it("turns HTTP errors into ApiError with the status", async () => {
    stubApi({ "/api/pipeline": 503 });
    const error = await apiGet("/api/pipeline").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ApiError);
    expect((error as ApiError).status).toBe(503);
  });

  it("reports network failures as unreachable", async () => {
    stubApi({ "/api/pipeline": new TypeError("Failed to fetch") });
    await expect(apiGet("/api/pipeline")).rejects.toThrow("API unreachable: Failed to fetch");
  });

  it("times out hung requests", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        (_url: string, init: RequestInit) =>
          new Promise((_, reject) => {
            init.signal?.addEventListener("abort", () => {
              reject(new DOMException("aborted", "AbortError"));
            });
          }),
      ),
    );
    const original = AbortSignal.timeout.bind(AbortSignal);
    vi.spyOn(AbortSignal, "timeout").mockImplementation(() => original(10));
    await expect(apiGet("/api/pipeline")).rejects.toThrow("didn't respond");
  });
});
