import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ReactElement } from "react";
import { vi } from "vitest";

export function renderWithClient(ui: ReactElement) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchInterval: false, gcTime: Infinity } },
  });
  return { client, ...render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>) };
}

type Route = Record<string, unknown>;

/** Stubs fetch: each key is a path prefix; values are JSON bodies, Errors (network failure) or numbers (HTTP status). */
export function stubApi(routes: Route) {
  const fetchMock = vi.fn((input: RequestInfo | URL) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const match = Object.keys(routes)
      .sort((a, b) => b.length - a.length)
      .find((prefix) => url.pathname.startsWith(prefix));
    if (match === undefined) return Promise.reject(new TypeError(`no stub for ${url.pathname}`));
    const raw = routes[match];
    const value = typeof raw === "function" ? (raw as () => unknown)() : raw;
    if (value instanceof Error) return Promise.reject(value);
    if (typeof value === "number") return Promise.resolve(new Response("error", { status: value }));
    return Promise.resolve(
      new Response(JSON.stringify(value), { status: 200, headers: { "Content-Type": "application/json" } }),
    );
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}
