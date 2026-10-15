"""CLI: train a direction model on gold/ohlcv_1m and compare it with baselines.

    python -m crypto_ml.train --products BTC-USD,ETH-USD --start 2026-10-01 --end 2026-10-08 --horizon 5
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime

from crypto_ml.config import GOLD_OHLCV_PATH, StorageConfig
from crypto_ml.data import read_ohlcv
from crypto_ml.dataset import build_dataset
from crypto_ml.tracking import DEFAULT_EXPERIMENT, log_training_run
from crypto_ml.training import TrainResult, train_and_evaluate

log = logging.getLogger("crypto_ml.train")


def parse_time(value: str) -> datetime:
    t = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return t.replace(tzinfo=UTC) if t.tzinfo is None else t.astimezone(UTC)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--products", required=True, help="Comma-separated, e.g. BTC-USD,ETH-USD")
    p.add_argument("--start", type=parse_time, help="Inclusive UTC start (ISO-8601)")
    p.add_argument("--end", type=parse_time, help="Exclusive UTC end (ISO-8601)")
    p.add_argument("--horizon", type=int, default=5, help="Label horizon in minutes")
    p.add_argument("--embargo", type=int, help="Gap between splits in minutes (default: horizon)")
    p.add_argument("--train-frac", type=float, default=0.7)
    p.add_argument("--val-frac", type=float, default=0.15)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--table-uri", help=f"gold table (default: s3://$LAKE_BUCKET/{GOLD_OHLCV_PATH})")
    p.add_argument("--table-version", type=int, help="Delta version to read (default: latest)")
    p.add_argument("--experiment", default=DEFAULT_EXPERIMENT, help="MLflow experiment name")
    p.add_argument("--no-mlflow", action="store_true", help="Skip experiment tracking")
    return p


def summary_table(result: TrainResult) -> str:
    keys = ("accuracy", "roc_auc", "log_loss", "ic")
    lines = [f"{'model':<10}{'split':<6}" + "".join(f"{k:>10}" for k in keys)]
    for model, splits in result.metrics.items():
        for split, m in splits.items():
            lines.append(f"{model:<10}{split:<6}" + "".join(f"{m.get(k, float('nan')):>10.4f}" for k in keys))
    return "\n".join(lines)


def run(args: argparse.Namespace) -> tuple[TrainResult, dict, str | None]:
    uri = args.table_uri or StorageConfig.from_env().table_uri(GOLD_OHLCV_PATH)
    products = [p.strip() for p in args.products.split(",") if p.strip()]
    data_in = read_ohlcv(uri, products=products, start=args.start, end=args.end, version=args.table_version)
    if data_in.frame.empty:
        raise SystemExit(f"no candles in {uri} for {products} in [{args.start}, {args.end})")
    log.info("read %d candles from %s (version %d)", len(data_in.frame), uri, data_in.table_version)

    dataset = build_dataset(data_in.frame, args.horizon)
    result = train_and_evaluate(dataset, args.horizon, seed=args.seed, train_frac=args.train_frac,
                                val_frac=args.val_frac, embargo=args.embargo)
    data_info = {"table_uri": uri, "table_version": data_in.table_version, "products": products,
                 "start": args.start.isoformat() if args.start else None,
                 "end": args.end.isoformat() if args.end else None, "rows": len(dataset)}

    run_id = None
    if not args.no_mlflow:
        params = {"seed": args.seed, "train_frac": args.train_frac, "val_frac": args.val_frac,
                  "embargo": args.embargo if args.embargo is not None else args.horizon}
        run_id = log_training_run(result, data_info, params, dataset[result.feature_columns],
                                  experiment=args.experiment)
    return result, data_info, run_id


def main(argv: list[str] | None = None) -> TrainResult:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    result, data_info, run_id = run(args)
    log.info("data: %s", json.dumps(data_info))
    if run_id:
        log.info("logged MLflow run %s in experiment '%s'", run_id, args.experiment)
    log.info("best iteration %d, splits %s", result.best_iteration, result.split_sizes)
    print(summary_table(result))
    return result


if __name__ == "__main__":
    main()
