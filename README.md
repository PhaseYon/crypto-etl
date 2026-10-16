# crypto-etl

Streaming ETL for crypto trades: live Coinbase trades → Kafka → Spark → a Delta Lake on S3-compatible
storage, feeding ML training and batch predictions, with a dashboard that visualizes the whole pipeline.

```
Coinbase WS ──► producer ──► Kafka raw.trades ──► bronze ──► silver ──► gold ──► ML ──► ml/predictions
                   ▲                               (Spark Structured Streaming,   │
                   │                                Delta Lake on S3/RustFS)      │
                   └──── backfill CLI ◄──── ops/trade_gaps ◄──────────────────────┘
                                         dashboard (React + FastAPI) reads every stage
```

| Component | What it does | Docs |
|---|---|---|
| [`producer/`](producer/README.md) | Coinbase WebSocket → Kafka. Reconnects with backoff, detects `trade_id` gaps and backfills them from the REST API (also across restarts), graceful shutdown, backfill CLI | [producer/README.md](producer/README.md) |
| [`lake/`](lake/README.md) | Spark jobs: bronze (raw), silver (parsed, validated, deduped by MERGE, bad records quarantined), gold (1-minute OHLCV, corrected by late data), trade-gap detection, compaction/vacuum | [lake/README.md](lake/README.md) |
| [`ml/`](ml/README.md) | Leakage-safe features and labels, time-based splits with embargo, baselines vs gradient boosting, MLflow tracking, batch inference | [ml/README.md](ml/README.md) |
| [`ui/`](ui/README.md), `api/` | React + TypeScript dashboard and read-only FastAPI backend: pipeline health, market candles, data quality, ML predictions | [ui/README.md](ui/README.md) |

Interfaces between components (Kafka message format, table schemas, config) are defined in
[docs/CONTRACTS.md](docs/CONTRACTS.md).

## Tech stack

Python 3.14 (producer, ML) · Kafka 4.1 (KRaft) · Spark 4.1 + Delta Lake 4.4 · RustFS (S3-compatible, local) / AWS S3 ·
delta-rs · scikit-learn · MLflow · FastAPI · React 19 + TypeScript + Vite + TanStack Query + lightweight-charts ·
Docker Compose · pytest · Vitest · Playwright · ruff · GitHub Actions

## Quick start

```bash
cp .env.example .env        # then replace the <PLACEHOLDER> credentials
docker compose up -d --build
```

| Service | URL |
|---|---|
| Dashboard | http://localhost:3000 |
| Dashboard API docs | http://localhost:8000/api/docs |
| Object store console | http://localhost:9001 (credentials from `.env`) |
| Kafka (from the host) | `localhost:9092` |

The full stack needs about 6–7 GB of RAM (raise WSL's limit in `.wslconfig` if needed). Trades appear in
Kafka within seconds, and the first gold candles within a couple of minutes.

On-demand jobs:

```bash
docker compose --profile batch run --rm lake-trade-gaps     # find missing trade_id ranges
lake/scripts/maintenance.sh                                  # OPTIMIZE + VACUUM (pauses streaming jobs)
docker compose --profile ml run --rm ml-train                # needs a few hours of candles to be meaningful
docker compose --profile ml run --rm ml-predict
```

## Configuration and secrets

All configuration is environment variables; [`.env.example`](.env.example) lists every one with placeholders.
`.env` is gitignored. In the cloud, inject the same variables from a secrets manager or KMS instead.
Coinbase market data is public, so no exchange API key is needed. To use AWS S3 instead of the local store,
set `S3_ENDPOINT=` (empty) plus real AWS credentials and region.

## Local development

One venv covers every Python component, for the editor and for running tests on the host
(component versions are kept aligned so they can share it):

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-local.txt
```

In VS Code, select `.venv/bin/python` as the interpreter. Spark itself only runs in its Docker image;
`pyspark` is in the venv just for imports and type hints in `lake/`.

## Tests

| Suite | Command |
|---|---|
| Producer unit / integration | `pytest producer/tests` / `KAFKA_TEST_BOOTSTRAP=localhost:9092 pytest producer/tests -m integration` |
| Lake unit / integration (in Docker) | `lake/scripts/test.sh unit` / `lake/scripts/test.sh integration` |
| ML unit / integration | `pytest ml` / see [ml/README.md](ml/README.md) |
| API unit / integration | `pytest api/tests` / `pytest api/tests -m integration` |
| UI checks + unit tests (in Docker) | `ui/scripts/npm.sh npm ci && ui/scripts/npm.sh npm run ci` |
| UI end-to-end (Playwright) | `ui/scripts/e2e.sh` |

Integration tests are excluded by default (`-m 'not integration'` in `pyproject.toml`). CI
([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs lint and unit tests for every component on
every push and PR, and integration tests on pushes to `main`.

## Reliability, in short

- **Producer:** reconnects with exponential backoff and jitter; backfills trade_id gaps from REST during
  runtime and across restarts; waits for in-flight backfills on shutdown; idempotent Kafka producer with `acks=all`.
- **Kafka:** data persisted on a named volume, 7-day retention, which is the replay window if Spark is down.
- **Lake:** checkpointed streaming jobs restart automatically and resume exactly where they stopped; silver
  and gold use MERGE, so replays don't duplicate and late backfilled trades correct candles instead of being dropped;
  malformed records are quarantined, not lost.
- **Gaps that slip through** are found by the trade-gap job and fed to the producer's backfill CLI.
- **Dashboard:** each stage degrades independently; one failing source shows as "down" without breaking the page.

Each component's README has its failure modes and recovery runbook.

## Known limitations

- Single-node everything (Kafka replication factor 1, Spark `local[*]`): fine for one machine, not HA.
- Not yet run against real AWS S3 (S3A config is unit-tested only).
- Short-horizon crypto price direction is close to unpredictable; the ML pipeline is infrastructure, and the
  baselines are there to keep results honest.
