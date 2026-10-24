"""CLI: score the most recent minutes of gold/ohlcv_1m with a trained direction or volatility model.

    python -m crypto_ml.predict                     # latest direction model, now, all its products
    python -m crypto_ml.predict --run-id <id> --as-of 2026-10-08T12:00:00Z --score-minutes 30
    python -m crypto_ml.predict --experiment crypto-volatility --as-of 2026-10-22T08:30:00Z --score-minutes 600

Only minutes after the model's validation split (plus the label horizon) are scored, so every
prediction is out of sample: neither fitting nor model selection saw those labels.

Predictions are MERGEd into the ml/predictions Delta table on (product_id, window_start,
model_run_id), so re-running for the same minutes updates rows instead of duplicating them.
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime, timedelta

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from crypto_ml.config import GOLD_OHLCV_PATH, PREDICTIONS_PATH, StorageConfig, normalize_uri, storage_options_for
from crypto_ml.data import read_ohlcv
from crypto_ml.features import add_vol_regime, build_features
from crypto_ml.tracking import DEFAULT_EXPERIMENT, latest_run_id, load_model
from crypto_ml.train import parse_time

log = logging.getLogger("crypto_ml.predict")

PREDICTIONS_SCHEMA = pa.schema(
    [
        ("product_id", pa.string()),
        ("window_start", pa.timestamp("us", tz="UTC")),
        ("horizon_minutes", pa.int64()),
        ("model_run_id", pa.string()),
        ("target", pa.string()),
        ("probability", pa.float64()),
        ("predicted_label", pa.int64()),
        ("is_filled", pa.int64()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("prediction_date", pa.date32()),
    ]
)
MERGE_KEY = ("product_id", "window_start", "model_run_id")


class StaleDataError(RuntimeError):
    """Gold has no candles close enough to the scoring time."""


def score(
    model, meta: dict, ohlcv: pd.DataFrame, run_id: str, score_minutes: int, now: datetime | None = None
) -> pd.DataFrame:
    """Predictions for the last `score_minutes` out-of-sample minutes of each product with complete features."""
    cols = meta["feature_columns"]
    target = meta.get("target", "direction")
    horizon = int(meta["horizon_minutes"])
    feats = build_features(ohlcv)
    if target == "volatility":
        feats = add_vol_regime(feats, horizon)
    feats = feats.dropna(subset=cols)
    unseen_after = out_of_sample_start(meta)
    if unseen_after is not None:
        feats = feats[feats["window_start"] > unseen_after]
    latest = feats.groupby("product_id", sort=True).tail(score_minutes).reset_index(drop=True)
    if latest.empty:
        return PREDICTIONS_SCHEMA.empty_table().to_pandas()
    probability = model.predict_proba(latest[cols])[:, 1]
    created = pd.Timestamp(now or datetime.now(UTC))
    return pd.DataFrame(
        {
            "product_id": latest["product_id"],
            "window_start": latest["window_start"],
            "horizon_minutes": horizon,
            "model_run_id": run_id,
            "target": target,
            "probability": probability,
            "predicted_label": (probability > 0.5).astype("int64"),
            "is_filled": latest["is_filled"].astype("int64"),
            "created_at": created,
            "prediction_date": latest["window_start"].dt.date,
        }
    )


def out_of_sample_start(meta: dict) -> pd.Timestamp | None:
    """Last minute whose label the model's fitting or selection could have seen, if the run recorded it.

    Validation labels look `horizon` minutes past the split's last row, so the gap is added too.
    """
    val_end = (meta.get("split_bounds") or {}).get("val_end")
    if val_end is None:
        return None  # older runs; live inference scores "now", which is always later
    return pd.Timestamp(val_end) + pd.Timedelta(minutes=int(meta["horizon_minutes"]))


def write_predictions(preds: pd.DataFrame, uri: str, storage_options: dict[str, str] | None = None) -> None:
    uri = normalize_uri(uri)
    if storage_options is None:
        storage_options = storage_options_for(uri)
    table = pa.Table.from_pandas(preds[PREDICTIONS_SCHEMA.names], schema=PREDICTIONS_SCHEMA, preserve_index=False)
    if not DeltaTable.is_deltatable(uri, storage_options=storage_options):
        write_deltalake(uri, table, partition_by=["prediction_date"], storage_options=storage_options)
        return
    predicate = " AND ".join(f"t.{k} = s.{k}" for k in MERGE_KEY)
    (
        DeltaTable(uri, storage_options=storage_options)
        .merge(source=table, predicate=predicate, source_alias="s", target_alias="t")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id", help="MLflow run whose model to use (default: latest in --experiment)")
    p.add_argument("--experiment", default=DEFAULT_EXPERIMENT)
    p.add_argument("--products", help="Comma-separated (default: the products the model was trained on)")
    p.add_argument("--as-of", type=parse_time, help="Score up to this UTC time (default: now)")
    p.add_argument("--score-minutes", type=int, default=1, help="Most recent minutes to score per product")
    p.add_argument(
        "--max-staleness-minutes",
        type=int,
        default=15,
        help="Fail if a product's newest candle is older than this before --as-of",
    )
    p.add_argument("--table-uri", help=f"gold table (default: s3://$LAKE_BUCKET/{GOLD_OHLCV_PATH})")
    p.add_argument("--output-uri", help=f"predictions table (default: s3://$LAKE_BUCKET/{PREDICTIONS_PATH})")
    return p


def run(args: argparse.Namespace) -> pd.DataFrame:
    cfg = StorageConfig.from_env()
    table_uri = args.table_uri or cfg.table_uri(GOLD_OHLCV_PATH)
    output_uri = args.output_uri or cfg.table_uri(PREDICTIONS_PATH)
    run_id = args.run_id or latest_run_id(args.experiment)
    model, meta = load_model(run_id)
    products = args.products.split(",") if args.products else meta["products"]
    as_of = args.as_of or datetime.now(UTC)

    # Enough history for every feature's lookback, plus the minutes being scored, plus slack for gaps.
    history = timedelta(minutes=meta["max_lookback_minutes"] + args.score_minutes + 60)
    data = read_ohlcv(table_uri, products=products, start=as_of - history, end=as_of + timedelta(minutes=1))
    _check_fresh(data.frame, products, as_of, args.max_staleness_minutes)

    preds = score(model, meta, data.frame, run_id, args.score_minutes)
    write_predictions(preds, output_uri)
    log.info("wrote %d predictions from run %s to %s", len(preds), run_id, output_uri)
    return preds


def _check_fresh(frame: pd.DataFrame, products: list[str], as_of: datetime, max_minutes: int) -> None:
    newest = frame.groupby("product_id")["window_start"].max() if not frame.empty else pd.Series(dtype=object)
    limit = pd.Timestamp(as_of) - pd.Timedelta(minutes=max_minutes)
    stale = [p for p in products if p not in newest.index or newest[p] < limit]
    if stale:
        raise StaleDataError(f"no candles newer than {limit.isoformat()} for {stale}; is the lake pipeline running?")


def main(argv: list[str] | None = None) -> pd.DataFrame:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    preds = run(build_parser().parse_args(argv))
    print(preds[["product_id", "window_start", "target", "probability", "predicted_label"]].to_string(index=False))
    return preds


if __name__ == "__main__":
    main()
