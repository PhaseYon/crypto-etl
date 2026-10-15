import datetime as dt

from lake.jobs import maintenance, silver
from lake.tables import insert_new_trades

from .helpers import silver_df


def test_optimize_compacts_vacuum_cleans_and_reruns_are_noops(spark, local_config):
    silver.ensure_tables(spark, local_config)
    when = dt.datetime(2026, 10, 15, 9, 0, 0)
    for i in range(6):  # six small commits -> six small files in one partition
        insert_new_trades(spark, local_config.silver_trades, silver_df(spark, [("BTC-USD", i, "1", "1", "buy", when)]))

    stats = maintenance.maintain(spark, local_config, retention_hours=168)
    silver_stats = stats[local_config.silver_trades]
    assert silver_stats["files_removed"] == 6 and silver_stats["files_added"] == 1
    assert silver_stats["num_files"] == 1

    # Tables that don't exist yet are skipped, not created
    assert local_config.gold_ohlcv_1m not in stats

    again = maintenance.maintain(spark, local_config, retention_hours=168)
    assert again[local_config.silver_trades]["files_removed"] == 0

    # With zero retention VACUUM physically removes the compacted-away files; data is intact
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try:
        maintenance.maintain(spark, local_config, retention_hours=0)
    finally:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    data_files = [p for p in _files(local_config.silver_trades) if p.endswith(".parquet")]
    assert len(data_files) == 1
    assert spark.read.format("delta").load(local_config.silver_trades).count() == 6


def _files(root: str) -> list[str]:
    import os

    return [os.path.join(d, f) for d, _, files in os.walk(root) if "_delta_log" not in d for f in files]
