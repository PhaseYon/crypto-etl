# Lake: transform and load

Spark Structured Streaming jobs that move trades from Kafka into a Delta Lake on S3-compatible
storage, in three layers (medallion architecture). Table schemas and keys are defined in
[`docs/CONTRACTS.md`](../docs/CONTRACTS.md).

```
Kafka raw.trades
   │  lake-bronze   append raw JSON + Kafka coordinates           (exactly-once)
   ▼
bronze/trades
   │  lake-silver   parse, validate, dedupe, insert-only MERGE    (idempotent, late data kept)
   ├──────────────► silver/trades_quarantine   (invalid rows + reason)
   ▼
silver/trades ──── lake-trade-gaps (batch) ──► ops/trade_gaps ──► producer backfill CLI
   │  lake-gold     recompute touched minutes, MERGE               (idempotent, late data corrects)
   ▼
gold/ohlcv_1m  ──► ml
```

All tables and streaming checkpoints (`_checkpoints/<job>/`) live in the bucket `LAKE_BUCKET`.

## Stack

| Piece | Version | Why this one |
|---|---|---|
| Spark | 4.1.3 (Scala 2.13, Java 17, Python 3.10) | Official `apache/spark` image |
| Delta Lake | 4.4.1, artifact `delta-spark_4.1_2.13` | Delta's build for Spark 4.1 (Delta 4.1+ publishes one artifact per Spark minor version) |
| Kafka connector | `spark-sql-kafka-0-10_2.13` 4.1.3 | Must equal the Spark version |
| S3A | `hadoop-aws` 3.4.2 + AWS SDK v2 bundle 2.29.52 | Must equal Spark's bundled Hadoop (3.4.2) |
| Object store (local) | RustFS 1.0.1 | Apache-2.0, S3-compatible. MinIO no longer publishes Docker images, so RustFS stands in; the service keeps the name `minio`. |

Connector jars are resolved when the image is built (`lake/Dockerfile`), de-duplicated against the
jars Spark already ships, and registered in `spark-defaults.conf`. Containers never fetch from Maven
at runtime.

## Running

From the repo root (needs `.env`, copied from `.env.example`):

```bash
docker compose up -d lake-bronze lake-silver lake-gold   # also starts Kafka, object store, init jobs
docker compose logs -f lake-silver
docker compose stop lake-bronze lake-silver lake-gold     # clean stop; resumes from checkpoints

# Batch jobs
docker compose --profile batch run --rm lake-trade-gaps > gaps.jsonl   # JSON lines on stdout, logs on stderr
lake/scripts/maintenance.sh                                             # OPTIMIZE + VACUUM (see below)
```

The object store console is at `http://localhost:${MINIO_CONSOLE_HOST_PORT}` (credentials = the AWS
keys in `.env`).

Memory: each job runs Spark in local mode (`local[2]`, 768 MB driver, 1.28 GB container limit).
The three streaming jobs plus Kafka and the object store use about 6 GB, so give WSL at least
8 GB in `.wslconfig`. Tune with `LAKE_DRIVER_MEMORY` and `LAKE_JOB_MEM_LIMIT`.

## Configuration

Shared variables are in `docs/CONTRACTS.md` and `.env.example`. Lake-only settings (all optional):

| Variable | Default | Meaning |
|---|---|---|
| `LAKE_ROOT` | `s3a://${LAKE_BUCKET}` | Root for all tables and checkpoints (tests use a local path) |
| `BRONZE_TRIGGER` / `SILVER_TRIGGER` / `GOLD_TRIGGER` | `10 seconds` / `30 seconds` / `30 seconds` | Micro-batch interval per job |
| `MAX_OFFSETS_PER_TRIGGER` | `50000` | Upper bound on Kafka messages per bronze batch (bounds memory after a long outage) |
| `FAIL_ON_DATA_LOSS` | `true` | See "Kafka retention exceeded" below |
| `SPARK_MASTER` | `local[*]` (compose: `local[2]`) | Spark master |
| `SPARK_DRIVER_MEMORY` | `1g` (compose: `768m`) | Driver heap |
| `SPARK_SHUFFLE_PARTITIONS` | `8` | Shuffle and Delta snapshot partitions |
| `GAPS_LOOKBACK_HOURS` | `24` | Window scanned by `lake-trade-gaps` |
| `VACUUM_RETENTION_HOURS` | `168` | History kept by VACUUM |

## Guarantees per job

**Bronze** (`lake.jobs.bronze`): every Kafka message lands exactly once. Offsets live in the
checkpoint, and the Delta sink records each batch id, so a batch replayed after a crash is skipped.
The value is stored untouched, so silver can always be rebuilt from bronze.

**Silver** (`lake.jobs.silver`): one row per `(exchange, product_id, trade_id)`.
- Parsing uses an explicit schema and `try_*` conversions (Spark 4 runs in ANSI mode, where a plain
  cast raises), so a bad message is quarantined instead of failing the batch. Each quarantined row
  carries a reason (`invalid_json`, `non_positive_price`, `invalid_side`, ...).
- Duplicates within a batch are dropped deterministically (live WebSocket copy over REST backfill,
  then earliest Kafka offset). Duplicates across batches are dropped by an insert-only MERGE.
- **No watermark.** Backfilled trades can be hours late and must not be dropped as late data.
- Replays are no-ops: the MERGE only inserts missing keys, and quarantine is keyed by Kafka position.
- Because silver's writer only inserts, silver stays readable as a streaming source.

**Gold** (`lake.jobs.gold`): one candle per `(product_id, minute)` that has trades.
- For each batch of new silver rows, the job finds the minutes they touch, recomputes those minutes
  from **all** silver trades in them, and MERGEs the result. A late trade therefore corrects its
  candle (open, low, volume, ...), and a replayed batch produces identical candles.
- Open/close are ordered by `trade_time`, tie-broken by `trade_id`.
- Change data feed is not needed: the batch's own rows identify the affected minutes.

**Trade gaps** (`lake.jobs.trade_gaps`, batch): missing `trade_id` ranges per product within the
lookback, written to `ops/trade_gaps` (overwritten each run; Delta time travel keeps history) and
printed as JSON lines for the producer's backfill CLI. `--fail-on-gaps` exits 3 for alerting.
A gap that starts before the lookback window isn't seen, so run it more often than the lookback.

## Failure and recovery runbook

| Situation | What happens | What to do |
|---|---|---|
| A streaming job crashes (bug, OOM, JVM killed) | The process exits non-zero; Docker restarts it (`unless-stopped`); it resumes from its checkpoint. Verified by killing the JVM: exact counts afterwards. | Check `docker compose logs <job>`. If it crash-loops, fix the cause; the checkpoint keeps progress. |
| Object store down | S3A retries (7 attempts with backoff), then the batch fails, the job exits, Docker keeps restarting it. Kafka keeps buffering. | Restore the object store; jobs catch up on their own. |
| Kafka down | Bronze retries, then fails and restarts until Kafka is back. Silver and gold idle. | Restore Kafka. |
| **Kafka retention exceeded** (bronze down longer than 7 days, or topic recreated) | With `FAIL_ON_DATA_LOSS=true`, bronze refuses to start: the offsets in its checkpoint no longer exist. It fails loudly instead of silently skipping data. | Decide deliberately: accept the loss and set `FAIL_ON_DATA_LOSS=false` for one run (bronze resumes from the earliest available offset), then set it back. Then run `lake-trade-gaps` and backfill the missing ranges from the REST API. |
| Corrupted or deleted checkpoint | The job can't resume. | Stop the job and delete its checkpoint folder (`_checkpoints/<job>/`). **Bronze:** it restarts from the earliest Kafka offset; messages still in Kafka get appended again, so bronze gets duplicates (silver dedupes them, so silver and gold stay correct). **Silver/gold:** they reprocess their whole source table; MERGE makes that harmless, just slower. |
| A bug wrote wrong data to silver or gold | | Fix the code, stop the job, delete the table and its checkpoint, re-run `lake-init`, then start the job. It rebuilds from the layer below (bronze is the permanent raw record). |
| Gaps in `trade_id` | Reported by `lake-trade-gaps`. | Feed `gaps.jsonl` to the producer's backfill CLI. The backfilled trades flow through bronze → silver (kept, despite being late) → gold (candles corrected). |

## Maintenance

`lake/scripts/maintenance.sh` (daily, e.g. cron `0 3 * * *`):
1. stops the streaming writers (graceful; Kafka buffers meanwhile),
2. runs `OPTIMIZE` (compaction) then `VACUUM` (default 7-day retention) on every table,
3. restarts the writers, even if maintenance failed.

Why stop the writers: on S3, Delta's default log store is only safe with one process committing to
a table at a time. Between runs, auto-compaction keeps small files in check inside each writer.
VACUUM's retention also bounds time travel and how far a stopped downstream job may fall behind
before its source files are gone. Extra flags pass through: `maintenance.sh --skip-vacuum`.

## Switching to AWS S3

Change only environment variables:

```bash
S3_ENDPOINT=                  # empty: use AWS's own endpoint
AWS_REGION=eu-west-1
LAKE_BUCKET=my-crypto-lake    # create it once (with versioning and a lifecycle rule for _checkpoints if you like)
# Either keys...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
# ...or leave both empty to use the default AWS credentials chain (IAM role / instance profile)
```

Then drop the `minio` and `minio-init` services. Delta's default S3 log store assumes one writer
per table, which this design keeps. For multiple clusters writing the same table, configure
`S3DynamoDBLogStore`.

## Development

```bash
lake/scripts/test.sh unit                  # unit tests in the Spark image (~2 min)
lake/scripts/test.sh integration           # starts Kafka + object store, runs integration tests (~4 min)
lake/scripts/test.sh all -k gold           # everything; extra args go to pytest
ruff check lake && ruff format --check lake
```

Code layout: `src/lake/transforms.py` holds pure DataFrame functions (no I/O, unit-tested);
`tables.py` holds Delta writes (create, MERGE); `jobs/` holds one entry point per job
(`python -m lake.jobs.<name>`). Unit tests use a local Spark and local Delta tables. Integration
tests use a throwaway Kafka topic and a unique prefix in the bucket.
