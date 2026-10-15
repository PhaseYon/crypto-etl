"""Maintenance job: OPTIMIZE (compact small files) then VACUUM every lake table.

    python -m lake.jobs.maintenance [--retention-hours 168] [--skip-vacuum]

Run it daily, with the streaming writers stopped (lake/scripts/maintenance.sh does that).
On S3 the default Delta log store is only safe with one writing process per table; the
streaming jobs are those writers, so maintenance must not commit while they run. Auto-
compaction already runs inside each writer between runs, so this is the deeper daily pass.

Idempotent: OPTIMIZE on an already compacted table and a repeated VACUUM are no-ops.
VACUUM keeps --retention-hours of history (default 7 days, Delta's safety minimum), which
also bounds time travel and how far a stopped downstream stream may fall behind.
"""

from __future__ import annotations

import argparse
import logging
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from lake.config import LakeConfig
from lake.jobs._runner import configure_logging
from lake.spark import build_spark

NAME = "maintenance"
log = logging.getLogger("lake.maintenance")


def table_paths(config: LakeConfig) -> list[str]:
    return [
        config.bronze_trades,
        config.silver_trades,
        config.silver_quarantine,
        config.gold_ohlcv_1m,
        config.ops_trade_gaps,
    ]


def maintain(spark: SparkSession, config: LakeConfig, retention_hours: float, vacuum: bool = True) -> dict[str, dict]:
    """OPTIMIZE and VACUUM each existing table. Returns per-table stats."""
    results: dict[str, dict] = {}
    for path in table_paths(config):
        if not DeltaTable.isDeltaTable(spark, path):
            log.info("skip %s (no table yet)", path)
            continue
        table = DeltaTable.forPath(spark, path)
        metrics = table.optimize().executeCompaction().collect()[0]["metrics"]
        stats = {"files_removed": metrics["numFilesRemoved"], "files_added": metrics["numFilesAdded"]}
        if vacuum:
            table.vacuum(retention_hours)
        stats["num_files"] = table.detail().collect()[0]["numFiles"]
        log.info("%s: %s", path, stats)
        results[path] = stats
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--retention-hours", type=float, default=168.0)
    parser.add_argument("--skip-vacuum", action="store_true")
    args = parser.parse_args(argv)

    configure_logging()
    config = LakeConfig.from_env()
    spark = build_spark(NAME, config)
    try:
        maintain(spark, config, args.retention_hours, vacuum=not args.skip_vacuum)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
