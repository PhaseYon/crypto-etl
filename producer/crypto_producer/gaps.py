"""trade_id gap detection. trade_id increments by 1 per product, so a jump means missed trades."""

from dataclasses import dataclass


@dataclass
class BackfillJob:
    """A missing range lo < trade_id < hi, and how far backfilling it has got."""

    product: str
    lo: int  # last trade_id before the gap
    hi: int  # first trade_id after the gap
    cursor: int = 0  # cursor..hi-1 has been produced (pages are fetched newest-first)

    def __post_init__(self):
        if self.cursor == 0:
            self.cursor = self.hi

    @property
    def missing(self) -> int:
        return self.hi - self.lo - 1

    def unfetched(self) -> tuple[int, int] | None:
        """Inclusive trade_id range not yet produced, or None if complete."""
        return (self.lo + 1, self.cursor - 1) if self.cursor > self.lo + 1 else None

    @classmethod
    def for_range(cls, product: str, first_id: int, last_id: int) -> "BackfillJob":
        """Job covering the inclusive range first_id..last_id."""
        if first_id > last_id:
            raise ValueError(f"empty range {first_id}..{last_id}")
        return cls(product, lo=first_id - 1, hi=last_id + 1)


def observe_trade(last_trade_id: dict[str, int], product: str, trade_id: int) -> BackfillJob | None:
    """Record a live trade; return a job for the gap before it, if there is one."""
    prev = last_trade_id.get(product)
    last_trade_id[product] = trade_id if prev is None else max(prev, trade_id)
    if prev is not None and trade_id > prev + 1:
        return BackfillJob(product, lo=prev, hi=trade_id)
    return None
