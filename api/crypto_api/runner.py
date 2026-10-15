"""Calls blocking source methods in worker threads with a timeout, and caches the outcome briefly.

Failures are values (Outcome.error), not exceptions, so one broken source degrades only the
parts of a response that depend on it.
"""

import asyncio
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from .sources.base import TableMissingError

MAX_ERROR_CHARS = 300


@dataclass(frozen=True)
class Outcome:
    value: Any = None
    error: str | None = None
    missing: bool = False  # the Delta table doesn't exist yet

    @property
    def ok(self) -> bool:
        return self.error is None and not self.missing


def describe_error(exc: BaseException, timeout: float) -> str:
    if isinstance(exc, TimeoutError):
        return f"timed out after {timeout:g}s"
    message = str(exc).strip() or type(exc).__name__
    return f"{type(exc).__name__}: {message}"[:MAX_ERROR_CHARS]


class SourceRunner:
    def __init__(self, timeout_seconds: float, ttl_seconds: float, max_workers: int = 8):
        self._timeout = timeout_seconds
        self._ttl = ttl_seconds
        # Bounded so calls hung on a dead source can't spawn threads without limit
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="source")
        self._cache: dict[str, tuple[float, Outcome]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def call(self, key: str, fn: Callable[..., Any], *args: Any, ttl: float | None = None) -> Outcome:
        """Run fn(*args) at most once per TTL per key; concurrent callers share the result."""
        ttl = self._ttl if ttl is None else ttl
        cached = self._fresh(key)
        if cached is not None:
            return cached
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            cached = self._fresh(key)
            if cached is not None:
                return cached
            outcome = await self._run(fn, *args)
            # Failures are cached too, so a down source isn't hammered by every open tab
            self._cache[key] = (time.monotonic() + ttl, outcome)
            return outcome

    def _fresh(self, key: str) -> Outcome | None:
        entry = self._cache.get(key)
        if entry is not None and entry[0] > time.monotonic():
            return entry[1]
        return None

    async def _run(self, fn: Callable[..., Any], *args: Any) -> Outcome:
        loop = asyncio.get_running_loop()
        try:
            value = await asyncio.wait_for(loop.run_in_executor(self._executor, fn, *args), self._timeout)
        except TableMissingError as e:
            return Outcome(missing=True, error=None, value=str(e))
        except Exception as e:  # any source failure becomes a reported, degraded state
            return Outcome(error=describe_error(e, self._timeout))
        return Outcome(value=value)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)
