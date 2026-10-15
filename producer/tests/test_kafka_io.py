import json

import pytest
from crypto_producer.kafka_io import DeliveryReporter, max_trade_ids, produce
from fakes import FakeProducer


def msg(product, trade_id, **extra):
    return json.dumps({"product_id": product, "trade_id": trade_id, **extra}).encode()


def test_max_trade_ids_takes_highest_per_product_regardless_of_order():
    values = [msg("BTC-USD", 5), msg("BTC-USD", 9), msg("BTC-USD", 7), msg("ETH-USD", 3)]
    assert max_trade_ids(values, ["BTC-USD", "ETH-USD"]) == {"BTC-USD": 9, "ETH-USD": 3}


def test_max_trade_ids_ignores_unwanted_products_and_bad_messages():
    values = [msg("DOGE-USD", 99), b"not json", b'{"product_id": "BTC-USD"}', b"[]", msg("BTC-USD", 1)]
    assert max_trade_ids(values, ["BTC-USD"]) == {"BTC-USD": 1}


def test_max_trade_ids_handles_backfill_records():
    values = [msg("BTC-USD", 4, source="rest_backfill"), msg("BTC-USD", 2)]
    assert max_trade_ids(values, ["BTC-USD"]) == {"BTC-USD": 4}


class FakeMessage:
    def __init__(self, key=b"BTC-USD"):
        self._key = key

    def key(self):
        return self._key

    def partition(self):
        return 0

    def offset(self):
        return 1


def test_delivery_reporter_counts_successes_and_failures():
    reporter = DeliveryReporter(log_every=1)
    reporter(None, FakeMessage())
    reporter(None, FakeMessage())
    reporter("broker down", FakeMessage())
    assert (reporter.delivered, reporter.failed) == (2, 1)


def test_produce_waits_for_queue_space():
    producer = FakeProducer(full_for=2)
    produce(producer, "t", "k", "v", None)
    assert producer.messages == [("t", "k", "v")]
    assert producer.polls == 2


def test_produce_gives_up_after_block_timeout():
    producer = FakeProducer(full_for=10**6)
    with pytest.raises(BufferError):
        produce(producer, "t", "k", "v", None, block_timeout=0)
