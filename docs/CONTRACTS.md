# Data contracts

The interfaces between components. Each component may change its internals freely,
but changes to anything in this file must be coordinated.

```
Coinbase WS ──► producer ──► Kafka raw.trades ──► lake: bronze ──► silver ──► gold ──► ml
                   ▲                                                  │
                   └──── backfill CLI ◄──── ops/trade_gaps ◄──────────┘
```

## Configuration (environment variables)

| Variable | Used by | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP` | producer, lake | `kafka:29092` inside Docker, `localhost:${KAFKA_HOST_PORT}` from the host |
| `KAFKA_TOPIC` | producer, lake | Default `raw.trades` |
| `LAKE_BUCKET` | lake, ml | Default `crypto-lake` |
| `S3_ENDPOINT` | lake, ml | `http://minio:9000` locally; empty = real AWS S3 |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` | lake, ml | Object store credentials (MinIO root user/password locally) |

Secrets are never committed. `.env.example` lists every variable with placeholders.

## Kafka topic `raw.trades`

- Key: `product_id` (UTF-8), e.g. `BTC-USD`. Guarantees per-product ordering of live trades.
- Value: one JSON object per trade.
- Delivery: at-least-once. Duplicates are possible (reconnects, backfill overlap); consumers dedupe on `(product_id, trade_id)`.
- Ordering: backfilled trades arrive after newer live trades. Consumers must use `time`, not arrival order.

Live trade (Coinbase `match` message, forwarded unmodified):
```json
{"type": "match", "trade_id": 846389513, "maker_order_id": "…", "taker_order_id": "…",
 "side": "sell", "size": "0.09299177", "price": "2688.33", "product_id": "ETH-USD",
 "sequence": 103539470060, "time": "2026-10-01T09:14:26.443591Z"}
```

Backfilled trade (from the REST API): no order ids or `sequence`, plus `"source": "rest_backfill"`:
```json
{"type": "match", "product_id": "BTC-USD", "source": "rest_backfill", "trade_id": 1103499406,
 "side": "buy", "size": "0.00000010", "price": "83121.10000000", "time": "2026-10-07T17:54:00.501697Z"}
```

Field notes: `trade_id` increments by 1 per product (gaps = missing data). `price`/`size` are decimal strings.
`side` is the maker order's side. `time` is ISO-8601 UTC with microseconds. Missing `source` means `ws`.

## Lake (Delta Lake tables in `s3a://${LAKE_BUCKET}/`)

All tables are Delta. Streaming checkpoints live under `_checkpoints/<job_name>/`.

### `bronze/trades` (append-only, raw)
| Column | Type | Notes |
|---|---|---|
| `kafka_key` | string | |
| `value` | string | raw JSON, untouched |
| `topic` | string | |
| `kafka_partition` | int | |
| `kafka_offset` | long | |
| `kafka_timestamp` | timestamp | |
| `ingested_at` | timestamp | |
| `ingest_date` | date | partition column |

### `silver/trades` (cleaned, one row per trade)
| Column | Type | Notes |
|---|---|---|
| `exchange` | string | `coinbase` |
| `product_id` | string | |
| `trade_id` | long | |
| `price` | decimal(38,18) | |
| `size` | decimal(38,18) | |
| `side` | string | `buy` / `sell` (maker side) |
| `trade_time` | timestamp | UTC, event time |
| `source` | string | `ws` / `rest_backfill` |
| `kafka_partition` | int | lineage |
| `kafka_offset` | long | lineage |
| `ingested_at` | timestamp | |
| `trade_date` | date | partition column, from `trade_time` |

Unique key: `(exchange, product_id, trade_id)`, enforced by MERGE (upsert), **not** by watermark-based
dedupe: backfilled trades can be hours late and must not be dropped as late data.

### `silver/trades_quarantine`
Records that failed parsing/validation: `value` (raw), `error` (reason), `kafka_partition`, `kafka_offset`, `quarantined_at`.

### `gold/ohlcv_1m` (one row per product per minute that has trades)
| Column | Type |
|---|---|
| `product_id` | string |
| `window_start` | timestamp (UTC, minute-aligned) |
| `window_end` | timestamp |
| `open`, `high`, `low`, `close` | double |
| `volume` | double (base currency) |
| `quote_volume` | double (sum price*size) |
| `vwap` | double |
| `trade_count` | long |
| `buy_volume`, `sell_volume` | double (by maker side) |
| `updated_at` | timestamp |
| `window_date` | date, partition column |

Unique key: `(product_id, window_start)`. Late trades re-compute their affected minutes and MERGE, so
backfill corrects candles instead of being dropped. Minutes with no trades have no row (ml forward-fills).

### `ops/trade_gaps` (data quality)
`product_id`, `gap_start_id`, `gap_end_id` (inclusive), `missing_count`, `detected_at`.
Missing `trade_id` ranges found in `silver/trades`. Feed for the producer's backfill CLI.

## ML

Reads `gold/ohlcv_1m` only. Owns feature engineering, labels, training and model artifacts.
Experiments are tracked with MLflow (`MLFLOW_TRACKING_URI`).
