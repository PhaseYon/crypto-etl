# Pipeline dashboard

A web dashboard that shows the ETL pipeline live: each stage's health, Kafka throughput and lag, Delta table
stats, 1-minute candles, data quality and ML predictions.

It has two parts:

| Part    | Path   | Stack                                                                                     |
| ------- | ------ | ----------------------------------------------------------------------------------------- |
| **API** | `api/` | FastAPI, read-only. Reads Kafka metadata (confluent-kafka) and Delta tables (delta-rs)    |
| **UI**  | `ui/`  | React 19 + TypeScript (strict), Vite, TanStack Query, lightweight-charts, served by nginx |

A browser can't talk to Kafka or read Delta tables, so the API gathers the state and the UI renders it.

```
browser ──► nginx (ui, :3000) ──/api──► dashboard-api (:8000) ──► Kafka (metadata only)
                                                            └──► Delta lake on S3 / MinIO (log + stats)
```

## Running it

### In Docker (recommended)

From the repo root:

```bash
docker compose up -d dashboard                               # real data: needs Kafka; lake optional
DASHBOARD_DATA_SOURCE=mock docker compose up -d dashboard    # demo data, no lake needed
```

Open http://localhost:3000. The dashboard only requires Kafka. Lake tables that don't exist yet (or an object store
that's down) show up as **Down** or **Not yet** on their own stages; nothing else breaks.

### Development

Node isn't required on the host: `ui/scripts/npm.sh` runs any npm command in the pinned Node image.

```bash
# API with hot reload (mock data), from the repo root
python3 -m venv .venv && .venv/bin/pip install -r api/requirements-dev.txt
cd api && DATA_SOURCE=mock ../.venv/bin/uvicorn crypto_api.main:app --reload --port 8000

# UI dev server with hot reload, proxying /api to the API above
ui/scripts/npm.sh npm ci
NODE_DOCKER_ARGS="-p 5173:5173 -e VITE_API_PROXY=http://host.docker.internal:8000" ui/scripts/npm.sh npm run dev
```

Open http://localhost:5173.

## Configuration

API (environment variables; the shared ones are documented in `.env.example` and `docs/CONTRACTS.md`):

| Variable                              | Default                        | Meaning                                                                    |
| ------------------------------------- | ------------------------------ | -------------------------------------------------------------------------- |
| `DATA_SOURCE`                         | `real`                         | `mock` serves deterministic demo data shaped like the contract             |
| `MOCK_SCENARIO`                       | `healthy`                      | `degraded` makes the mock object store unreachable (to see failure states) |
| `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`      | `localhost:9092`, `raw.trades` |                                                                            |
| `LAKE_BUCKET`, `S3_ENDPOINT`, `AWS_*` |                                | Same as the lake. Empty `S3_ENDPOINT` means real AWS S3                    |
| `LAKE_URI`                            | `s3://$LAKE_BUCKET`            | Override the lake root, e.g. a local path                                  |
| `PRODUCTS`                            | `BTC-USD,ETH-USD,SOL-USD`      | Products offered in the market view (others are rejected)                  |
| `SOURCE_TIMEOUT_SECONDS`              | `4`                            | Upper bound for every Kafka / object-store call                            |
| `CACHE_TTL_SECONDS`                   | `3`                            | How long source reads are shared between requests                          |
| `POLL_INTERVAL_MS`                    | `5000`                         | The UI's base refresh interval                                             |
| `CORS_ORIGINS`                        | empty                          | Only needed if the UI is served from another origin                        |

Compose (`ui/compose.yml`): `DASHBOARD_HOST_PORT` (3000), `DASHBOARD_API_HOST_PORT` (8000, bound to localhost),
`DASHBOARD_DATA_SOURCE` (`real`).

## Health model

Every stage and table reports one of four states, always shown with an icon and a label (never color alone), plus
the reason:

| State        | Meaning                                                                                      |
| ------------ | -------------------------------------------------------------------------------------------- |
| **OK**       | Reachable and within its freshness threshold                                                 |
| **Stale**    | Reachable, but behind its threshold                                                          |
| **Down**     | Unreachable, erroring, timed out, or a required table is missing                             |
| **Inactive** | Not monitored (Coinbase), or optional and not created yet (ML predictions, quarantine, gaps) |

Thresholds (API environment variables, also returned by `/api/meta`):

| Stage            | Stale when                                                           | Variable (default)                                          |
| ---------------- | -------------------------------------------------------------------- | ----------------------------------------------------------- |
| Kafka / producer | no new messages for this long (needs that long of observation first) | `KAFKA_STALE_SECONDS` (120)                                 |
| Bronze           | newest `ingested_at` older than, **or** Kafka→bronze lag above       | `BRONZE_STALE_SECONDS` (300), `LAG_STALE_MESSAGES` (10,000) |
| Silver           | newest `ingested_at` older than                                      | `SILVER_STALE_SECONDS` (300)                                |
| Gold             | newest `updated_at` older than                                       | `GOLD_STALE_SECONDS` (300)                                  |
| ML               | newest prediction older than                                         | `PREDICTIONS_STALE_SECONDS` (7,200)                         |

How the numbers are computed:

- **Ingest rate.** Kafka has no messages-per-second metric, so the API samples the sum of high watermarks on each
  (cached) read and divides the change by the time between samples over a 60s window. Shows "measuring" until two samples exist.
- **Kafka→bronze lag.** Spark doesn't commit offsets to Kafka, so the bronze table is the bookmark:
  `lag = high watermark − (max kafka_offset in bronze + 1)` per partition. A partition bronze has never read counts
  every retained message. The max offset comes from Delta file statistics (Spark writes one file per Kafka partition
  per batch). Only files spanning several partitions are scanned, and only their dates.
- **Row counts and freshness** come from the Delta log's per-file statistics (`num_records`, column max values), not
  from scanning data.
- **Producer** health is inferred from Kafka ingest; the producer exposes no metrics of its own.

## Resilience

- Every source call runs in a bounded thread pool with a timeout. One slow or dead source can't hang a request.
- Results, failures included, are cached for a few seconds, with concurrent requests sharing one read. Many open
  tabs (or a dead source) don't multiply the load.
- Each part of each response degrades on its own: with the object store down, Kafka stats still render, and the
  reverse. The API returns 200 with per-part status instead of failing the whole response.
- The UI polls at the API's interval and backs off exponentially (up to 60s) while requests fail. It keeps the last
  data on screen, dimmed, with a "refresh failing" notice, instead of blanking the page.
- nginx fails fast (3s connect timeout) when the API is down, so the UI's backoff takes over.
- The API container runs as non-root with a read-only filesystem. The UI is served by unprivileged nginx with a CSP.
  No secrets ever reach the frontend bundle.

## API

| Endpoint                                            | Returns                                                                                 |
| --------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `GET /api/livez`                                    | Liveness only                                                                           |
| `GET /api/meta`                                     | Data source, products, poll interval, thresholds                                        |
| `GET /api/pipeline`                                 | Stages (status, reason, metrics), Kafka partitions, lag per partition, every lake table |
| `GET /api/market/ohlcv?product=BTC-USD&minutes=360` | 1-minute candles from `gold/ohlcv_1m` (15 ≤ minutes ≤ 10,080)                           |
| `GET /api/quality`                                  | Quarantine reasons and recent records, latest trade_id gaps, backfilled share           |
| `GET /api/ml/predictions`                           | Latest predictions from `ml/predictions` (tolerant of schema differences)               |

Interactive docs: `/api/docs`. The OpenAPI schema is the single source of the UI's TypeScript types:

```bash
.venv/bin/python api/scripts/export_openapi.py   # writes ui/src/api/openapi.json
ui/scripts/npm.sh npm run gen:api                 # regenerates ui/src/api/schema.ts
```

A unit test fails if the committed snapshot is stale, and `npm run check:api` fails if the TS types are.

## Tests

```bash
# API unit tests (fast; uses real delta-rs against local temp tables, no network)
.venv/bin/python -m pytest api/tests

# API integration tests: real Kafka + a throwaway S3 store (RustFS)
docker compose up -d --wait kafka
docker compose --profile ui-test up -d --wait ui-test-s3
docker compose --profile ui-test up ui-test-s3-init
.venv/bin/python -m pytest api/tests -m integration     # UI_IT_* env vars override endpoints

# UI: generated-types check, typecheck, lint, format check, unit/component tests (Vitest + Testing Library)
ui/scripts/npm.sh npm run ci

# End-to-end (Playwright, desktop + phone) against the containerized dashboard in mock mode
ui/scripts/e2e.sh

# Screenshots of every view, light/dark/phone
node ui/scripts/screenshots.mjs <base-url> <out-dir>   # inside the Playwright image; see e2e.sh
```

## Design notes

- Charts follow the project's dataviz rules. Colors come from a validated palette (light and dark tuned
  separately). Candles use a blue/red diverging pair, not green/red. Volume sits in its own pane, so no plot has two
  y-axes. Lines are 2px. Every chart has a legend and a table view.
- Colors follow the entity: a product keeps its color in the ML chart however the data is filtered.
- Layout works down to phone width (verified by the e2e phone project).

## Assumptions about other components

These are handled defensively and should be pinned down in `docs/CONTRACTS.md`:

- **`ops/trade_gaps` semantics.** It's not specified whether each scan overwrites the table or appends to it. The API
  shows the latest scan: rows whose `detected_at` is within 60s of the newest. A `scan_id` column, or overwrite
  semantics, would make this exact.
- **`ml/predictions` schema.** Not in the contract yet. The API reads `product_id`, `window_start`, `created_at`,
  `model_version` (or `model`) and the first of `score` / `probability` / `prob_up` / `prediction` / `y_pred`. Rows
  without `product_id` are skipped.
- **Delta features.** Reads go through delta-rs. Tables using features it can't read (e.g. deletion vectors through
  the pyarrow path) report **Down** with the error, not a crash.
