import type { paths } from "./schema";

type GetPath = {
  [P in keyof paths]: paths[P]["get"] extends { responses: unknown } ? P : never;
}[keyof paths];
type Operation<P extends GetPath> = paths[P]["get"];
type JsonResponse<P extends GetPath> =
  Operation<P> extends {
    responses: { 200: { content: { "application/json": infer T } } };
  }
    ? T
    : never;
type Query<P extends GetPath> = Operation<P> extends { parameters: { query?: infer Q } } ? Q : never;

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number | null,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

const REQUEST_TIMEOUT_MS = 10_000;

/** Typed GET: the path picks both the query parameters and the response type from the OpenAPI schema. */
export async function apiGet<P extends GetPath>(
  path: P,
  query?: Query<P>,
  signal?: AbortSignal,
): Promise<JsonResponse<P>> {
  const url = new URL(path, window.location.origin);
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
        url.searchParams.set(key, String(value));
      }
    }
  }
  const timeout = AbortSignal.timeout(REQUEST_TIMEOUT_MS);
  let response: Response;
  try {
    response = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
    });
  } catch (error) {
    if (timeout.aborted) throw new ApiError(`API didn't respond within ${REQUEST_TIMEOUT_MS / 1000}s`, null);
    throw new ApiError(`API unreachable: ${error instanceof Error ? error.message : String(error)}`, null);
  }
  if (!response.ok) {
    throw new ApiError(`API returned ${response.status} ${response.statusText}`.trim(), response.status);
  }
  return (await response.json()) as JsonResponse<P>;
}
