"""Backfill missing trade_id ranges from the Coinbase REST API into Kafka.

Used by the live producer when it sees a gap, and as a CLI for ranges found later
(e.g. the lake's ops/trade_gaps table):

    python -m crypto_producer.backfill --product BTC-USD --from-id 100 --to-id 250
    python -m crypto_producer.backfill --gaps-file trade_gaps.csv
"""

import argparse
import asyncio
import csv
import json
import logging
import sys
from collections.abc import Callable
from pathlib import Path

from .coinbase import fetch_trades_before, to_backfill_record
from .config import Settings
from .gaps import BackfillJob
from .kafka_io import DeliveryReporter, make_producer, produce

log = logging.getLogger(__name__)

FetchPage = Callable[[str, int], list[dict]]  # (product, before_id) -> trades newest first
Emit = Callable[[dict], None]


def take_in_range(trades: list[dict], lo: int) -> list[dict]:
    """The leading trades of a newest-first page with trade_id > lo."""
    out = []
    for trade in trades:
        if trade["trade_id"] <= lo:
            break
        out.append(trade)
    return out


async def fetch_with_retries(fetch_page: FetchPage, product: str, before_id: int, retries: int, base_delay: float):
    """Call the blocking fetch in a worker thread, retrying with exponential backoff."""
    for attempt in range(retries + 1):
        try:
            return await asyncio.to_thread(fetch_page, product, before_id)
        except Exception as e:
            if attempt == retries:
                raise
            delay = base_delay * 2**attempt
            log.warning(
                "REST fetch failed for %s before %d (%s: %s), retry %d/%d in %.1fs",
                product,
                before_id,
                type(e).__name__,
                e,
                attempt + 1,
                retries,
                delay,
            )
            await asyncio.sleep(delay)


async def run_backfill(
    job: BackfillJob,
    fetch_page: FetchPage,
    emit: Emit,
    *,
    max_trades: int,
    retries: int = 3,
    page_pause: float = 0.25,
    retry_delay: float = 1.0,
) -> bool:
    """Emit every trade with job.lo < trade_id < job.hi. Returns True if the range is complete.

    Pages are fetched newest-first, and job.cursor tracks progress so a cancelled or failed
    job can report exactly which range is still missing.
    """
    first, last = job.lo + 1, job.hi - 1
    if job.missing > max_trades:
        log.error("gap too large to backfill: %s trade_id %d..%d (%d trades)", job.product, first, last, job.missing)
        return False
    log.warning("backfilling %s: %d trades (%d..%d)", job.product, job.missing, first, last)

    sent = 0
    try:
        while job.cursor > job.lo + 1:
            page = await fetch_with_retries(fetch_page, job.product, job.cursor, retries, retry_delay)
            if not page:
                break  # nothing older is available (e.g. beyond the exchange's history)
            for trade in take_in_range(page, job.lo):
                emit(to_backfill_record(job.product, trade))
                sent += 1
            job.cursor = max(page[-1]["trade_id"], job.lo + 1)
            if job.cursor > job.lo + 1:
                await asyncio.sleep(page_pause)  # stay well under the public REST rate limit
    except Exception:
        log.exception("backfill failed for %s after %d trades; still missing %s", job.product, sent, job.unfetched())
        return False

    if gap := job.unfetched():
        log.error("backfill incomplete for %s: REST returned no trades for %d..%d", job.product, *gap)
        return False
    log.info("backfill done: %s sent %d of %d missing trades", job.product, sent, job.missing)
    return True


def _read_rows(path: Path) -> list[dict]:
    text = path.read_text()
    suffix = path.suffix.lower()
    if suffix in (".jsonl", ".ndjson"):  # what Spark writes with df.write.json()
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    if suffix == ".json":
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    return list(csv.DictReader(text.splitlines()))


def jobs_from_gaps_file(path: Path) -> list[BackfillJob]:
    """Ranges shaped like ops/trade_gaps (product_id, gap_start_id, gap_end_id) from CSV, JSON or JSON Lines."""
    jobs = []
    for i, row in enumerate(_read_rows(path), start=1):
        try:
            jobs.append(BackfillJob.for_range(row["product_id"], int(row["gap_start_id"]), int(row["gap_end_id"])))
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"{path}: row {i} is invalid ({type(e).__name__}: {e}): {row}") from None
    return jobs


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m crypto_producer.backfill",
        description="Backfill missing trade_id ranges from the Coinbase REST API into Kafka.",
    )
    parser.add_argument("--product", help="e.g. BTC-USD")
    parser.add_argument("--from-id", type=int, help="first missing trade_id (inclusive)")
    parser.add_argument("--to-id", type=int, help="last missing trade_id (inclusive)")
    parser.add_argument("--gaps-file", type=Path, help="CSV/JSON/JSONL with product_id, gap_start_id, gap_end_id")
    args = parser.parse_args(argv)
    explicit = (args.product, args.from_id, args.to_id)
    if args.gaps_file is None and None in explicit:
        parser.error("give --product, --from-id and --to-id, or --gaps-file")
    if args.gaps_file is not None and any(v is not None for v in explicit):
        parser.error("--gaps-file can't be combined with --product/--from-id/--to-id")
    if args.gaps_file is None and args.from_id > args.to_id:
        parser.error("--from-id must be <= --to-id")
    return args


async def backfill_jobs(settings: Settings, jobs: list[BackfillJob], producer=None) -> bool:
    """Run jobs one after another into Kafka. True only if every range was filled and delivered."""
    if producer is None:  # not `or`: a Producer with an empty queue has len() == 0 and is falsy
        producer = make_producer(settings)
    reporter = DeliveryReporter()

    def emit(record: dict):
        produce(producer, settings.topic, record["product_id"], json.dumps(record), reporter)
        producer.poll(0)

    def fetch_page(product: str, before_id: int) -> list[dict]:
        return fetch_trades_before(settings.rest_url, product, before_id, settings.rest_timeout)

    ok = True
    for job in jobs:
        done = await run_backfill(
            job, fetch_page, emit, max_trades=settings.max_backfill_trades, retries=settings.rest_retries
        )
        if not done:
            ok = False
            if gap := job.unfetched():
                log.error("still missing: %s trade_id %d..%d", job.product, *gap)
    undelivered = producer.flush(settings.shutdown_flush_timeout)
    if undelivered or reporter.failed:
        log.error("%d messages undelivered, %d failed", undelivered, reporter.failed)
        ok = False
    log.info("backfill finished: %d range(s), %d trades delivered, ok=%s", len(jobs), reporter.delivered, ok)
    return ok


def main(argv: list[str] | None = None) -> int:
    """Exit codes: 0 all ranges filled, 1 some range incomplete or undelivered, 2 bad input."""
    args = parse_args(argv)
    settings = Settings.from_env()
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        if args.gaps_file is not None:
            jobs = jobs_from_gaps_file(args.gaps_file)
        else:
            jobs = [BackfillJob.for_range(args.product, args.from_id, args.to_id)]
    except (OSError, ValueError) as e:
        log.error("invalid input: %s", e)
        return 2
    return 0 if asyncio.run(backfill_jobs(settings, jobs)) else 1


if __name__ == "__main__":
    sys.exit(main())
