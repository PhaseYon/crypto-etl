"""Data quality job: find missing trade_id ranges in silver/trades.

    python -m lake.jobs.trade_gaps [--lookback-hours 24] [--export gaps.jsonl] [--fail-on-gaps]

Writes the current set of gaps to ops/trade_gaps (overwritten each run; Delta time travel
keeps history) and prints one JSON object per gap, the format the producer's backfill CLI
reads:  {"product_id": "BTC-USD", "gap_start_id": 101, "gap_end_id": 179, "missing_count": 79}

Only trades inside the lookback window are considered, so a gap that straddles the start of
the window is not seen. Run it more often than the lookback (e.g. hourly with 24h).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lake.config import LakeConfig
from lake.jobs._runner import configure_logging
from lake.schemas import OPS_TRADE_GAPS
from lake.spark import build_spark
from lake.tables import ensure_table
from lake.transforms import trade_id_gaps

NAME = "trade_gaps"
log = logging.getLogger("lake.trade_gaps")


def detect_gaps(spark: SparkSession, config: LakeConfig, lookback_hours: float, now: dt.datetime) -> DataFrame:
    since = now - dt.timedelta(hours=lookback_hours)
    trades = (
        spark.read.format("delta")
        .load(config.silver_trades)
        .where((F.col("trade_date") >= F.lit(since.date())) & (F.col("trade_time") >= F.lit(since)))
    )
    return trade_id_gaps(trades)


def write_gaps(spark: SparkSession, config: LakeConfig, gaps: DataFrame) -> list[dict]:
    ensure_table(spark, config.ops_trade_gaps, OPS_TRADE_GAPS)
    gaps = gaps.persist()
    try:
        gaps.write.format("delta").mode("overwrite").save(config.ops_trade_gaps)
        rows = gaps.orderBy("product_id", "gap_start_id").collect()
    finally:
        gaps.unpersist()
    return [
        {
            "product_id": r.product_id,
            "gap_start_id": r.gap_start_id,
            "gap_end_id": r.gap_end_id,
            "missing_count": r.missing_count,
        }
        for r in rows
    ]


def run(
    spark: SparkSession,
    config: LakeConfig,
    lookback_hours: float,
    now: dt.datetime,
    export: str | None = None,
    fail_on_gaps: bool = False,
) -> int:
    """Detect, store, print and optionally export gaps. Returns the process exit code."""
    gaps = write_gaps(spark, config, detect_gaps(spark, config, lookback_hours, now))
    lines = [json.dumps(g) for g in gaps]
    for line in lines:
        print(line)
    if export:
        with open(export, "w") as f:
            f.writelines(line + "\n" for line in lines)
    missing = sum(g["missing_count"] for g in gaps)
    log.info("%d gaps, %d missing trades in the last %.1fh", len(gaps), missing, lookback_hours)
    return 3 if (gaps and fail_on_gaps) else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--lookback-hours", type=float, default=24.0)
    parser.add_argument("--export", help="also write the gaps as JSON lines to this file")
    parser.add_argument("--fail-on-gaps", action="store_true", help="exit 3 if any gap is found (for alerting)")
    args = parser.parse_args(argv)

    configure_logging()
    config = LakeConfig.from_env()
    spark = build_spark(NAME, config, master=config.spark_master)
    try:
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        return run(spark, config, args.lookback_hours, now, args.export, args.fail_on_gaps)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
