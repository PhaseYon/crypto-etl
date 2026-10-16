# Producer (extract)

Streams every trade from the Coinbase Exchange WebSocket (`matches` channel) into the Kafka
topic `raw.trades`, keyed by `product_id`. Messages are forwarded unmodified; trades missed
while disconnected are backfilled from the Coinbase REST API. Message format: see
[`docs/CONTRACTS.md`](../docs/CONTRACTS.md).

No API keys needed: Coinbase market data is public.

## Run

**In Docker** (from the repo root):
```bash
docker compose up -d kafka kafka-init producer
docker compose logs -f producer
```

**Locally** (Kafka from compose, producer on the host):
```bash
python3 -m venv .venv
.venv/bin/pip install -e producer -r requirements-dev.txt -r producer/requirements-dev.txt
docker compose up -d kafka kafka-init
.venv/bin/python -m crypto_producer          # Ctrl+C once: graceful stop; twice: exit now
```

Watch trades arrive:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic raw.trades --property print.key=true
```

## Configuration

All settings are environment variables (validated at startup; a bad value exits with an error naming it).

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | `kafka:29092` inside Docker |
| `KAFKA_TOPIC` | `raw.trades` | |
| `PRODUCTS` | `BTC-USD,ETH-USD,SOL-USD` | Comma-separated Coinbase product ids |
| `COINBASE_WS_URL` | `wss://ws-feed.exchange.coinbase.com` | |
| `COINBASE_REST_URL` | `https://api.exchange.coinbase.com` | |
| `REST_TIMEOUT` | `15` | Seconds per REST request |
| `REST_RETRIES` | `3` | Retries per REST page (exponential backoff) |
| `MAX_BACKFILL_TRADES` | `500000` | Larger gaps are logged, not backfilled (about a day of BTC at ~200 trades/min; a full-size backfill takes a few minutes) |
| `STARTUP_SCAN_MESSAGES` | `5000` | Messages per partition read on startup to find the last trade_ids |
| `STARTUP_SCAN_TIMEOUT` | `15` | Overall deadline for that scan |
| `SHUTDOWN_BACKFILL_TIMEOUT` | `5` | How long shutdown waits for running backfills |
| `SHUTDOWN_FLUSH_TIMEOUT` | `10` | How long shutdown waits to deliver queued messages |
| `POLL_INTERVAL` | `1` | Seconds between delivery-callback polls |
| `HEALTH_FILE` | empty (Docker: `/tmp/producer-alive`) | Touched while the feed is alive |
| `LOG_LEVEL` | `INFO` | |

Keep `SHUTDOWN_BACKFILL_TIMEOUT + SHUTDOWN_FLUSH_TIMEOUT` (plus ~1s) below the compose
`stop_grace_period` (20s), or Docker SIGKILLs the producer mid-flush.

## Failure modes

| What fails | What happens | What you do |
|---|---|---|
| WebSocket drops / handshake fails / DNS | Reconnects with exponential backoff (1s→60s) plus jitter. Any `trade_id` jump on reconnect is backfilled from REST. | Nothing |
| Producer crashes or restarts | On startup it reads the last `trade_id` per product back from Kafka, so the gap across the restart is detected and backfilled. In Docker, `restart: unless-stopped` brings it back. | Nothing |
| REST error during a backfill | Each page is retried (`REST_RETRIES`); if it still fails, the log names the exact missing range. | Run the backfill CLI for that range |
| Gap larger than `MAX_BACKFILL_TRADES` (long outage) | Logged as `gap too large to backfill: <product> trade_id X..Y`; live streaming continues. | Run the backfill CLI (raise the cap if needed) |
| Shutdown while a backfill runs | Waits up to `SHUTDOWN_BACKFILL_TIMEOUT`, then logs `backfill cut off by shutdown: <product> trade_id X..Y not fetched`. | Run the backfill CLI for that range |
| Kafka briefly down | Messages wait in the local queue and are retried; `produce()` waits for queue space instead of failing. Messages not delivered within ~5 min are reported as `delivery failed`. | Check Kafka |
| Kafka down at startup | Logs a warning, starts without resume state (a restart gap won't be detected), keeps streaming into the local queue. | The lake's `ops/trade_gaps` will show the gap; backfill it |
| Feed silent (no trades and no heartbeats for 2 min) | Docker healthcheck turns unhealthy. | Check logs / connectivity |

Duplicates are possible (backfill overlap, reconnects): the lake dedupes on `(product_id, trade_id)`.
Backfilled trades arrive after newer live ones; consumers must use the trade's `time`.

## Backfill CLI

Fill explicit ranges into Kafka using the same backfill code (retries, cap, progress logging):

```bash
# One inclusive range
python -m crypto_producer.backfill --product BTC-USD --from-id 1103499338 --to-id 1103499400

# Ranges from the lake's ops/trade_gaps table, exported as CSV, JSON or JSON Lines
python -m crypto_producer.backfill --gaps-file trade_gaps.csv

# In Docker
docker compose run --rm --entrypoint python producer -m crypto_producer.backfill --product BTC-USD --from-id 1 --to-id 2
```

The gaps file needs `product_id`, `gap_start_id`, `gap_end_id` columns/keys (extra ones are ignored).
Exit codes: `0` every range filled and delivered, `1` some range incomplete, `2` bad input.

## Tests

```bash
# Unit tests (no network, no Kafka; ~1s)
.venv/bin/pytest producer/tests

# Integration tests: real Kafka + an offline fake Coinbase, unique topic per test
docker compose up -d kafka kafka-init
KAFKA_TEST_BOOTSTRAP=localhost:9092 .venv/bin/pytest producer/tests -m integration

# Lint
.venv/bin/ruff check producer && .venv/bin/ruff format --check producer
```

Unit tests are prevented from creating a real Kafka producer, so they can't write to a running stack.

## Layout

| Module | Responsibility |
|---|---|
| `config.py` | `Settings.from_env()` |
| `stream.py` | WebSocket loop, reconnect backoff |
| `gaps.py` | Gap detection, `BackfillJob` progress |
| `backfill.py` | REST pagination into Kafka; the CLI |
| `coinbase.py` | REST client, record normalization, subscribe message |
| `kafka_io.py` | Producer config, delivery reporting, startup tail scan |
| `app.py` | Wiring, signals, graceful shutdown, health file |
