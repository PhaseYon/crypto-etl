import asyncio
import threading
import time

import pytest
from crypto_api.runner import SourceRunner, describe_error
from crypto_api.sources.base import TableMissingError


def run(coro):
    return asyncio.run(coro)


def test_value_is_returned():
    runner = SourceRunner(timeout_seconds=1, ttl_seconds=0)
    outcome = run(runner.call("k", lambda: 42))
    assert outcome.ok and outcome.value == 42


def test_exception_becomes_error_outcome():
    def boom():
        raise ConnectionError("refused")

    outcome = run(SourceRunner(1, 0).call("k", boom))
    assert not outcome.ok
    assert outcome.error == "ConnectionError: refused"


def test_timeout_becomes_error_outcome():
    release = threading.Event()
    runner = SourceRunner(timeout_seconds=0.1, ttl_seconds=0)
    started = time.monotonic()
    outcome = run(runner.call("k", release.wait, 5))
    release.set()
    assert outcome.error == "timed out after 0.1s"
    assert time.monotonic() - started < 1


def test_missing_table_is_flagged_not_an_error():
    def missing():
        raise TableMissingError("gold/ohlcv_1m")

    outcome = run(SourceRunner(1, 0).call("k", missing))
    assert outcome.missing and outcome.error is None and not outcome.ok


def test_results_cached_within_ttl():
    calls = []
    runner = SourceRunner(1, ttl_seconds=60)

    async def twice():
        await runner.call("k", lambda: calls.append(1) or len(calls))
        return await runner.call("k", lambda: calls.append(1) or len(calls))

    assert run(twice()).value == 1
    assert len(calls) == 1


def test_failures_are_cached_too():
    calls = []

    def failing():
        calls.append(1)
        raise OSError("down")

    runner = SourceRunner(1, ttl_seconds=60)

    async def twice():
        await runner.call("k", failing)
        return await runner.call("k", failing)

    assert run(twice()).error == "OSError: down"
    assert len(calls) == 1


def test_concurrent_callers_share_one_load():
    calls = []

    def slow():
        calls.append(1)
        time.sleep(0.05)
        return "v"

    runner = SourceRunner(1, ttl_seconds=60)

    async def many():
        return await asyncio.gather(*(runner.call("k", slow) for _ in range(10)))

    assert {o.value for o in run(many())} == {"v"}
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("exc", "expected"),
    [(TimeoutError(), "timed out after 2s"), (ValueError(""), "ValueError: ValueError"), (OSError("x" * 500), None)],
)
def test_describe_error(exc, expected):
    text = describe_error(exc, 2)
    if expected:
        assert text == expected
    assert len(text) <= 300
