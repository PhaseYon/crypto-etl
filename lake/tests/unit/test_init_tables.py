from delta.tables import DeltaTable

from lake import schemas
from lake.jobs.init_tables import init_tables


def test_creates_every_contract_table_once(spark, local_config):
    init_tables(spark, local_config)
    init_tables(spark, local_config)  # idempotent

    expected = {
        local_config.bronze_trades: (schemas.BRONZE_TRADES, ["ingest_date"]),
        local_config.silver_trades: (schemas.SILVER_TRADES, ["trade_date"]),
        local_config.silver_quarantine: (schemas.SILVER_QUARANTINE, []),
        local_config.gold_ohlcv_1m: (schemas.GOLD_OHLCV_1M, ["window_date"]),
        local_config.ops_trade_gaps: (schemas.OPS_TRADE_GAPS, []),
    }
    for path, (schema, partitions) in expected.items():
        detail = DeltaTable.forPath(spark, path).detail().collect()[0]
        assert detail["partitionColumns"] == partitions, path
        assert detail["properties"].get("delta.autoOptimize.autoCompact") == "true", path
        table_schema = spark.read.format("delta").load(path).schema
        assert [(f.name, f.dataType) for f in table_schema] == [(f.name, f.dataType) for f in schema], path
        history = DeltaTable.forPath(spark, path).history().collect()
        assert len(history) == 1, f"{path} was created more than once"
