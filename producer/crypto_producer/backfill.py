"""Backfill missing trade_id ranges from the Coinbase REST API."""

import asyncio
import logging
from collections.abc import Callable

from .coinbase import to_backfill_record
from .gaps import BackfillJob

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
