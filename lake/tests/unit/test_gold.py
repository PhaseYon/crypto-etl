import datetime as dt

import pytest

from lake.jobs import gold, silver
from lake.schemas import GOLD_OHLCV_1M
from lake.tables import ensure_table, insert_new_trades
from lake.transforms import ohlcv_1m

from .helpers import silver_df

T0 = dt.datetime(2026, 10, 15, 9, 14, 0)


def at(seconds: float) -> dt.datetime:
    return T0 + dt.timedelta(seconds=seconds)


def candles(df):
    return {(r.product_id, r.window_start): r for r in ohlcv_1m(df).collect()}


def test_ohlcv_values_for_one_minute(spark):
    trades = silver_df(
        spark,
        [
            # listed out of time order on purpose
            ("BTC-USD", 12, "103", "1", "sell", at(40)),
            ("BTC-USD", 10, "100", "2", "buy", at(5)),
            ("BTC-USD", 11, "110", "1", "buy", at(20)),
            ("BTC-USD", 13, "95", "4", "sell", at(59.9)),
        ],
    )
    [c] = candles(trades).values()
    assert c.window_start == T0 and c.window_end == T0 + dt.timedelta(minutes=1)
    assert (c.open, c.high, c.low, c.close) == (100.0, 110.0, 95.0, 95.0)
    assert c.volume == pytest.approx(8.0)
    assert c.quote_volume == pytest.approx(100 * 2 + 110 + 103 + 95 * 4)
    assert c.vwap == pytest.approx(c.quote_volume / 8.0)
    assert c.trade_count == 4
    assert c.buy_volume == pytest.approx(3.0) and c.sell_volume == pytest.approx(5.0)
    assert c.window_date == T0.date()


def test_open_close_tie_on_time_breaks_by_trade_id(spark):
    trades = silver_df(
        spark,
        [
            ("BTC-USD", 21, "2", "1", "buy", at(10)),
            ("BTC-USD", 20, "1", "1", "buy", at(10)),
            ("BTC-USD", 23, "4", "1", "buy", at(50)),
            ("BTC-USD", 22, "3", "1", "buy", at(50)),
        ],
    )
    [c] = candles(trades).values()
    assert (c.open, c.close) == (1.0, 4.0)


def test_candles_split_by_minute_and_product(spark):
    trades = silver_df(
        spark,
        [
            ("BTC-USD", 1, "100", "1", "buy", at(59.999999)),
            ("BTC-USD", 2, "101", "1", "buy", at(60)),  # next minute
            ("ETH-USD", 1, "2700", "1", "buy", at(30)),
        ],
    )
    result = candles(trades)
    assert set(result) == {("BTC-USD", T0), ("BTC-USD", at(60)), ("ETH-USD", T0)}
    assert result[("BTC-USD", T0)].trade_count == 1


def test_schema_matches_contract(spark):
    trades = silver_df(spark, [("BTC-USD", 1, "100", "1", "buy", at(1))])
    assert [(f.name, f.dataType) for f in ohlcv_1m(trades).schema] == [(f.name, f.dataType) for f in GOLD_OHLCV_1M]


def test_late_trade_corrects_existing_candle_and_replay_is_idempotent(spark, local_config):
    silver.ensure_tables(spark, local_config)
    ensure_table(spark, local_config.gold_ohlcv_1m, GOLD_OHLCV_1M, partition_by="window_date")

    first = silver_df(spark, [("BTC-USD", 10, "100", "1", "buy", at(10)), ("BTC-USD", 11, "105", "1", "buy", at(30))])
    insert_new_trades(spark, local_config.silver_trades, first)
    gold.process_batch(first, 0, local_config)
    gold.process_batch(first, 0, local_config)  # replay

    def read_gold():
        return {
            (r.product_id, r.window_start): r
            for r in spark.read.format("delta").load(local_config.gold_ohlcv_1m).collect()
        }

    [c] = read_gold().values()
    assert (c.open, c.close, c.trade_count) == (100.0, 105.0, 2)

    # A backfilled trade that happened earlier in the same minute arrives later
    late = silver_df(spark, [("BTC-USD", 9, "90", "3", "sell", at(2), "rest_backfill")])
    insert_new_trades(spark, local_config.silver_trades, late)
    gold.process_batch(late, 1, local_config)

    gold_rows = read_gold()
    assert len(gold_rows) == 1
    [c] = gold_rows.values()
    assert (c.open, c.low, c.close, c.trade_count) == (90.0, 90.0, 105.0, 3)
    assert c.volume == pytest.approx(5.0) and c.sell_volume == pytest.approx(3.0)
