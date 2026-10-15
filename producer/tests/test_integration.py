"""End-to-end: the real producer process, a real Kafka, and a fake Coinbase (offline, deterministic)."""

import re
import signal

import pytest
from conftest import read_topic, wait_for
from fake_coinbase import FakeCoinbase, match

pytestmark = pytest.mark.integration


def env_for(fake: FakeCoinbase, bootstrap: str, topic: str, **extra) -> dict[str, str]:
    return {
        "COINBASE_WS_URL": fake.ws_url,
        "COINBASE_REST_URL": fake.rest_url,
        "KAFKA_BOOTSTRAP": bootstrap,
        "KAFKA_TOPIC": topic,
        **extra,
    }


def ids_by_product(messages) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {}
    for key, value in messages:
        assert key == value["product_id"], "message key must be the product_id"
        out.setdefault(key, []).append(value["trade_id"])
    return {k: sorted(v) for k, v in out.items()}


def test_live_trades_land_in_kafka_keyed_by_product(kafka_bootstrap, topic, run_producer):
    script = [match("BTC-USD", 1), match("ETH-USD", 7), match("BTC-USD", 2), match("BTC-USD", 3)]
    with FakeCoinbase(ws_script=script) as fake:
        p = run_producer(**env_for(fake, kafka_bootstrap, topic))
        assert wait_for(lambda: len(read_topic(kafka_bootstrap, topic)) == 4), "trades never reached Kafka"
        assert p.stop(signal.SIGTERM) == 0, p.output  # docker stop sends SIGTERM

    messages = read_topic(kafka_bootstrap, topic)
    assert ids_by_product(messages) == {"BTC-USD": [1, 2, 3], "ETH-USD": [7]}
    assert all("source" not in v for _, v in messages), "live trades are forwarded unmodified"
    assert fake.subscriptions[0]["channels"] == ["matches", "heartbeat"]


def test_gap_is_backfilled_from_rest(kafka_bootstrap, topic, run_producer):
    with FakeCoinbase(ws_script=[match("BTC-USD", 100), match("BTC-USD", 2600)]) as fake:
        p = run_producer(**env_for(fake, kafka_bootstrap, topic))
        assert wait_for(lambda: len(read_topic(kafka_bootstrap, topic)) == 2501, timeout=30), "backfill incomplete"
        assert p.stop() == 0, p.output

    messages = read_topic(kafka_bootstrap, topic)
    assert ids_by_product(messages) == {"BTC-USD": list(range(100, 2601))}
    backfilled = [v for _, v in messages if v.get("source") == "rest_backfill"]
    assert len(backfilled) == 2499
    assert {"type", "product_id", "trade_id", "price", "size", "side", "time"} <= backfilled[0].keys()
    assert "backfill done: BTC-USD sent 2499 of 2499" in p.output


def test_restart_resumes_from_kafka_and_backfills_the_gap(kafka_bootstrap, topic, run_producer):
    with FakeCoinbase(ws_script=[match("BTC-USD", 100), match("BTC-USD", 101)]) as fake:
        first = run_producer(**env_for(fake, kafka_bootstrap, topic))
        assert wait_for(lambda: len(read_topic(kafka_bootstrap, topic)) == 2)
        assert first.stop() == 0, first.output

    # While the producer was down, trades 102..109 happened
    with FakeCoinbase(ws_script=[match("BTC-USD", 110), match("BTC-USD", 111)]) as fake:
        second = run_producer(**env_for(fake, kafka_bootstrap, topic))
        assert wait_for(lambda: len(read_topic(kafka_bootstrap, topic)) == 12), "gap across restart not filled"
        assert second.stop() == 0, second.output

    assert "resuming from last trade_ids in Kafka: {'BTC-USD': 101}" in second.output
    assert ids_by_product(read_topic(kafka_bootstrap, topic)) == {"BTC-USD": list(range(100, 112))}


def test_shutdown_waits_for_backfill_then_reports_what_was_cut_off(kafka_bootstrap, topic, run_producer):
    # 5000 missing trades at ~1s per 1000-trade page: can't finish within a 1s shutdown budget
    with FakeCoinbase(ws_script=[match("BTC-USD", 100), match("BTC-USD", 5101)], rest_page_delay=1.0) as fake:
        p = run_producer(**env_for(fake, kafka_bootstrap, topic, SHUTDOWN_BACKFILL_TIMEOUT="1"))
        assert wait_for(lambda: len(read_topic(kafka_bootstrap, topic)) >= 1002, timeout=20)
        assert p.stop() == 0, p.output

    m = re.search(r"backfill cut off by shutdown: BTC-USD trade_id (\d+)\.\.(\d+) not fetched", p.output)
    assert m, p.output
    first_missing, last_missing = int(m.group(1)), int(m.group(2))
    ids = ids_by_product(read_topic(kafka_bootstrap, topic))["BTC-USD"]
    # Everything except the reported range made it to Kafka: the log is an exact to-do list
    assert first_missing == 101
    assert ids == [100, *range(last_missing + 1, 5102)]


def test_kafka_down_at_startup_does_not_crash(run_producer):
    with FakeCoinbase(ws_script=[match("BTC-USD", 1)]) as fake:
        p = run_producer(
            **env_for(fake, "127.0.0.1:1", "unused"),
            STARTUP_SCAN_TIMEOUT="2",
            SHUTDOWN_FLUSH_TIMEOUT="1",
        )
        assert wait_for(lambda: len(fake.subscriptions) == 1, timeout=15), "never connected to the feed"
        assert p.proc.poll() is None, "producer exited"
        assert p.stop() == 0, p.output

    assert "could not read last trade_ids from Kafka" in p.output
    assert "messages were not delivered" in p.output  # reported, not silently dropped
