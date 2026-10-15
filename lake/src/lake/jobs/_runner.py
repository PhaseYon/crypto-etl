"""Shared plumbing for running a streaming job as a long-lived process."""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Callable

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from lake.config import LakeConfig
from lake.spark import build_spark

log = logging.getLogger("lake")

StartFn = Callable[[SparkSession, LakeConfig, dict], StreamingQuery]


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def run_streaming(name: str, start: StartFn, trigger_interval: Callable[[LakeConfig], str]) -> None:
    """Start a streaming query and keep it running until SIGTERM/SIGINT or failure.

    On a signal the query is stopped cleanly; progress is in the checkpoint, so a restart
    resumes exactly where it left off. A query failure exits non-zero so the container's
    restart policy brings the job back.
    """
    configure_logging()
    config = LakeConfig.from_env()
    spark = build_spark(name, config)
    query = start(spark, config, {"processingTime": trigger_interval(config)})
    log.info("%s started (id=%s, trigger=%s)", name, query.id, trigger_interval(config))

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    try:
        # Poll instead of blocking forever so the signal flag is checked between waits
        while query.isActive and not stop.is_set():
            query.awaitTermination(5)
    finally:
        if query.isActive:
            log.info("%s stopping", name)
            query.stop()
        error = query.exception()
        spark.stop()
    if error is not None:
        log.error("%s failed: %s", name, error)
        raise SystemExit(1)
    log.info("%s stopped", name)
