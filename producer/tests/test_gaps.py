import pytest
from crypto_producer.gaps import BackfillJob, observe_trade


def test_first_trade_has_no_gap():
    last = {}
    assert observe_trade(last, "BTC-USD", 100) is None
    assert last == {"BTC-USD": 100}


def test_consecutive_trades_have_no_gap():
    last = {"BTC-USD": 100}
    assert observe_trade(last, "BTC-USD", 101) is None
    assert last["BTC-USD"] == 101


def test_jump_returns_job_for_the_missing_range():
    last = {"BTC-USD": 100}
    job = observe_trade(last, "BTC-USD", 250)
    assert (job.product, job.lo, job.hi, job.missing) == ("BTC-USD", 100, 250, 149)
    assert job.unfetched() == (101, 249)
    assert last["BTC-USD"] == 250


def test_old_or_duplicate_trade_does_not_move_last_back_or_open_a_gap():
    last = {"BTC-USD": 100}
    assert observe_trade(last, "BTC-USD", 90) is None
    assert observe_trade(last, "BTC-USD", 100) is None
    assert last["BTC-USD"] == 100


def test_products_are_tracked_independently():
    last = {"BTC-USD": 100, "ETH-USD": 5}
    assert observe_trade(last, "ETH-USD", 6) is None
    assert observe_trade(last, "BTC-USD", 102).unfetched() == (101, 101)


def test_unfetched_shrinks_as_cursor_moves_and_is_none_when_done():
    job = BackfillJob("BTC-USD", lo=100, hi=250)
    job.cursor = 180
    assert job.unfetched() == (101, 179)
    job.cursor = 101
    assert job.unfetched() is None


def test_for_range_is_inclusive():
    job = BackfillJob.for_range("BTC-USD", 101, 249)
    assert (job.lo, job.hi, job.missing) == (100, 250, 149)


def test_for_range_rejects_empty_range():
    with pytest.raises(ValueError):
        BackfillJob.for_range("BTC-USD", 10, 9)
