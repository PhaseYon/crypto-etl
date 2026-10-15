import datetime as dt

from lake.jobs import silver, trade_gaps
from lake.tables import insert_new_trades
from lake.transforms import trade_id_gaps

from .helpers import silver_df

NOW = dt.datetime(2026, 10, 15, 12, 0, 0)


def rows(spark, ids_by_product, when=NOW - dt.timedelta(hours=1)):
    trades = [(p, i, "1", "1", "buy", when) for p, ids in ids_by_product.items() for i in ids]
    return silver_df(spark, trades)


def gaps_of(df):
    return sorted((r.product_id, r.gap_start_id, r.gap_end_id, r.missing_count) for r in trade_id_gaps(df).collect())


def test_contiguous_ids_have_no_gaps(spark):
    assert gaps_of(rows(spark, {"BTC-USD": [5, 6, 7, 8]})) == []


def test_gaps_are_inclusive_ranges_per_product(spark):
    df = rows(spark, {"BTC-USD": [1, 2, 5, 6, 10], "ETH-USD": [100, 102], "SOL-USD": [7]})
    assert gaps_of(df) == [
        ("BTC-USD", 3, 4, 2),
        ("BTC-USD", 7, 9, 3),
        ("ETH-USD", 101, 101, 1),
    ]


def test_duplicate_ids_are_not_gaps(spark):
    assert gaps_of(rows(spark, {"BTC-USD": [1, 1, 2, 2, 3]})) == []


def test_job_respects_lookback_and_writes_ops_table(spark, local_config):
    silver.ensure_tables(spark, local_config)
    recent = rows(spark, {"BTC-USD": [10, 11, 14]})
    old = rows(spark, {"BTC-USD": [1]}, when=NOW - dt.timedelta(days=3))  # 1..9 missing, but out of window
    insert_new_trades(spark, local_config.silver_trades, recent.unionByName(old))

    gaps = trade_gaps.write_gaps(spark, local_config, trade_gaps.detect_gaps(spark, local_config, 24, NOW))
    assert gaps == [{"product_id": "BTC-USD", "gap_start_id": 12, "gap_end_id": 13, "missing_count": 2}]

    stored = spark.read.format("delta").load(local_config.ops_trade_gaps).collect()
    assert [(r.gap_start_id, r.gap_end_id) for r in stored] == [(12, 13)]

    # Re-running after the gap is filled overwrites the table with the current state
    insert_new_trades(spark, local_config.silver_trades, rows(spark, {"BTC-USD": [12, 13]}))
    assert trade_gaps.write_gaps(spark, local_config, trade_gaps.detect_gaps(spark, local_config, 24, NOW)) == []
    assert spark.read.format("delta").load(local_config.ops_trade_gaps).count() == 0


def test_run_exports_json_lines_and_signals_gaps(spark, local_config, tmp_path, capsys):
    silver.ensure_tables(spark, local_config)
    insert_new_trades(spark, local_config.silver_trades, rows(spark, {"ETH-USD": [1, 4]}))
    export = tmp_path / "gaps.jsonl"

    assert trade_gaps.run(spark, local_config, 24, NOW, export=str(export)) == 0
    assert trade_gaps.run(spark, local_config, 24, NOW, fail_on_gaps=True) == 3

    expected = '{"product_id": "ETH-USD", "gap_start_id": 2, "gap_end_id": 3, "missing_count": 2}'
    assert export.read_text() == expected + "\n"
    assert expected in capsys.readouterr().out
