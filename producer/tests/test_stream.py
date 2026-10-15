import json

from crypto_producer.coinbase import subscribe_message
from crypto_producer.config import Settings
from crypto_producer.stream import MAX_BACKOFF, TradeStream, next_backoff, reconnect_delay


def test_reconnect_delay_adds_up_to_one_second_of_jitter():
    assert reconnect_delay(4, rand=lambda: 0.0) == 4
    assert reconnect_delay(4, rand=lambda: 0.999) < 5


def test_backoff_doubles_up_to_the_cap():
    delays = [1.0]
    for _ in range(10):
        delays.append(next_backoff(delays[-1]))
    assert delays[:5] == [1, 2, 4, 8, 16]
    assert max(delays) == MAX_BACKOFF


def make_stream(last=None):
    emitted, jobs, alive = [], [], []
    stream = TradeStream(
        Settings(),
        last if last is not None else {},
        lambda p, raw: emitted.append((p, raw)),
        jobs.append,
        on_alive=lambda: alive.append(1),
    )
    return stream, emitted, jobs, alive


def match(trade_id, product="BTC-USD"):
    return json.dumps({"type": "match", "product_id": product, "trade_id": trade_id})


def test_match_is_forwarded_unmodified_keyed_by_product():
    stream, emitted, jobs, alive = make_stream()
    stream.handle(match(1))
    assert emitted == [("BTC-USD", match(1))]
    assert jobs == [] and alive == [1]


def test_gap_starts_a_backfill_and_still_forwards_the_trade():
    stream, emitted, jobs, _ = make_stream({"BTC-USD": 1})
    stream.handle(match(5))
    assert [(j.lo, j.hi) for j in jobs] == [(1, 5)]
    assert len(emitted) == 1


def test_heartbeat_marks_alive_without_producing():
    stream, emitted, _, alive = make_stream()
    stream.handle(json.dumps({"type": "heartbeat", "product_id": "BTC-USD", "last_trade_id": 9}))
    assert emitted == [] and alive == [1]


def test_other_messages_are_not_produced():
    stream, emitted, jobs, alive = make_stream()
    stream.handle(json.dumps({"type": "subscriptions", "channels": []}))
    stream.handle(json.dumps({"type": "error", "message": "bad"}))
    assert emitted == [] and jobs == [] and alive == []


def test_subscribes_to_matches_and_heartbeat():
    msg = subscribe_message(("BTC-USD",))
    assert msg == {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["matches", "heartbeat"]}
