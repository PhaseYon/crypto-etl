import datetime as dt
from decimal import Decimal

import pytest

from lake.jobs import silver
from lake.transforms import dedupe_trades, parse_and_validate, valid_trades

from .helpers import bronze_df, trade_json


def errors_by_value(spark, values):
    rows = parse_and_validate(bronze_df(spark, values)).collect()
    return [r.error for r in sorted(rows, key=lambda r: r.kafka_offset)]


def test_valid_live_trade_is_typed_exactly(spark):
    [row] = valid_trades(parse_and_validate(bronze_df(spark, [trade_json(trade_id=846389513)]))).collect()
    assert row.exchange == "coinbase"
    assert row.product_id == "BTC-USD"
    assert row.trade_id == 846389513
    assert row.price == Decimal("84000.5")
    assert row.size == Decimal("0.01")
    assert row.side == "buy"
    assert row.trade_time == dt.datetime(2026, 10, 15, 9, 14, 26, 443591)  # UTC, microseconds kept
    assert row.trade_date == dt.date(2026, 10, 15)
    assert row.source == "ws"


def test_backfilled_trade_keeps_its_source(spark):
    [row] = valid_trades(parse_and_validate(bronze_df(spark, [trade_json(source="rest_backfill")]))).collect()
    assert row.source == "rest_backfill"


@pytest.mark.parametrize(
    ("value", "error"),
    [
        ("not json", "invalid_json"),
        (None, "invalid_json"),
        ('{"type":"subscriptions"}', "unexpected_type"),
        (trade_json(product_id=None), "missing_product_id"),
        (trade_json(trade_id=None), "missing_or_invalid_trade_id"),
        (trade_json(trade_id="abc"), "missing_or_invalid_trade_id"),
        (trade_json(price="abc"), "missing_or_invalid_price"),
        (trade_json(price="0"), "non_positive_price"),
        (trade_json(price="-1"), "non_positive_price"),
        (trade_json(size="0"), "non_positive_size"),
        (trade_json(side="hold"), "invalid_side"),
        (trade_json(time="yesterday"), "missing_or_invalid_time"),
        (trade_json(time=None), "missing_or_invalid_time"),
    ],
)
def test_invalid_messages_get_a_reason(spark, value, error):
    assert errors_by_value(spark, [value]) == [error]


def test_bad_rows_do_not_fail_the_batch(spark):
    assert errors_by_value(spark, [trade_json(trade_id=1), "garbage", trade_json(trade_id=2)]) == [
        None,
        "invalid_json",
        None,
    ]


def test_dedupe_prefers_live_copy_then_earliest_offset(spark):
    values = [
        trade_json(trade_id=5, source="rest_backfill", price="1"),
        trade_json(trade_id=5, price="2"),
        trade_json(trade_id=5, price="3"),
        trade_json(trade_id=6),
    ]
    rows = dedupe_trades(valid_trades(parse_and_validate(bronze_df(spark, values)))).collect()
    by_id = {r.trade_id: r for r in rows}
    assert sorted(by_id) == [5, 6]
    assert by_id[5].source == "ws" and by_id[5].price == Decimal("2")


def test_same_trade_in_different_products_is_not_a_duplicate(spark):
    values = [trade_json(trade_id=5, product_id="BTC-USD"), trade_json(trade_id=5, product_id="ETH-USD")]
    assert dedupe_trades(valid_trades(parse_and_validate(bronze_df(spark, values)))).count() == 2


def read(spark, path):
    return spark.read.format("delta").load(path)


def test_process_batch_is_idempotent_and_keeps_late_trades(spark, local_config):
    silver.ensure_tables(spark, local_config)
    batch = bronze_df(
        spark,
        [trade_json(trade_id=1), trade_json(trade_id=2), trade_json(trade_id=2), "garbage"],
    )
    silver.process_batch(batch, 0, local_config)
    silver.process_batch(batch, 0, local_config)  # replay after a crash

    trades = read(spark, local_config.silver_trades)
    assert sorted(r.trade_id for r in trades.collect()) == [1, 2]
    quarantine = read(spark, local_config.silver_quarantine).collect()
    assert [(q.value, q.error) for q in quarantine] == [("garbage", "invalid_json")]

    # A backfilled trade from days ago, plus a re-delivery of trade 1, in a later batch
    late = bronze_df(
        spark,
        [trade_json(trade_id=0, time="2026-10-10T08:00:00Z", source="rest_backfill"), trade_json(trade_id=1)],
        start_offset=10,
    )
    silver.process_batch(late, 1, local_config)
    rows = {r.trade_id: r for r in read(spark, local_config.silver_trades).collect()}
    assert sorted(rows) == [0, 1, 2]
    assert rows[0].trade_date == dt.date(2026, 10, 10)
    assert rows[1].kafka_offset == 0  # first copy kept, re-delivery ignored
