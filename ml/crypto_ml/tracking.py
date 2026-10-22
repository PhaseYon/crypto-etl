"""MLflow experiment tracking: log training runs and find/load models for inference."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import mlflow
import mlflow.sklearn
import pandas as pd

from crypto_ml.config import mlflow_artifact_root, mlflow_tracking_uri
from crypto_ml.features import MAX_LOOKBACK_MINUTES, VOL_REGIME_WINDOW_MINUTES
from crypto_ml.training import TrainResult

DEFAULT_EXPERIMENT = "crypto-direction"
# One experiment per target, so the default "latest model" lookup never mixes them up
EXPERIMENTS = {"direction": DEFAULT_EXPERIMENT, "volatility": "crypto-volatility"}
MODEL_ARTIFACT = "model"
META_ARTIFACT = "model_meta.json"
# MLflow serializes sklearn models with skops, which refuses unreviewed types on load. We only
# load models this code trained, so trust exactly the tree type HistGradientBoosting needs.
SKOPS_TRUSTED_TYPES = ["sklearn.ensemble._hist_gradient_boosting.predictor.TreePredictor"]


def configure() -> str:
    """Point MLflow at MLFLOW_TRACKING_URI, creating the local SQLite directory if needed."""
    uri = mlflow_tracking_uri()
    if uri.startswith("sqlite:///"):
        Path(uri.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(uri)
    return uri


def _artifact_location() -> str:
    root = mlflow_artifact_root()
    if "://" in root:  # e.g. s3://bucket/mlflow
        return root
    path = Path(root).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path.as_uri()


def ensure_experiment(name: str) -> str:
    exp = mlflow.get_experiment_by_name(name)
    if exp is not None:
        return exp.experiment_id
    return mlflow.create_experiment(name, artifact_location=_artifact_location())


def log_training_run(
    result: TrainResult,
    data_info: dict[str, Any],
    params: dict[str, Any],
    X_example: pd.DataFrame,
    experiment: str = DEFAULT_EXPERIMENT,
) -> str:
    """Log one training run. Returns the MLflow run id."""
    configure()
    experiment_id = ensure_experiment(experiment)
    with mlflow.start_run(experiment_id=experiment_id) as run:
        mlflow.log_params(
            {
                **params,
                "target": result.target,
                "horizon": result.horizon,
                "best_iteration": result.best_iteration,
                "n_features": len(result.feature_columns),
            }
        )
        mlflow.log_params({f"{result.selected_model}.{k}": v for k, v in result.model.get_params().items()})
        mlflow.log_params({f"split.{k}_rows": v for k, v in result.split_sizes.items()})
        mlflow.log_metrics({k: v for k, v in result.flat_metrics().items() if not math.isnan(v)})
        mlflow.set_tags(
            {
                "data.table_uri": data_info["table_uri"],
                "data.table_version": str(data_info["table_version"]),
                "data.products": ",".join(data_info["products"]),
                "data.start": str(data_info.get("start")),
                "data.end": str(data_info.get("end")),
                "data.rows": str(data_info.get("rows")),
                "target": result.target,
                "selected_model": result.selected_model,
            }
        )
        mlflow.log_dict({**result.split_bounds, "sizes": result.split_sizes}, "splits.json")
        mlflow.log_dict(model_meta(result, data_info), META_ARTIFACT)
        mlflow.sklearn.log_model(
            result.model, name=MODEL_ARTIFACT, input_example=X_example.head(5), skops_trusted_types=SKOPS_TRUSTED_TYPES
        )
        mlflow.set_tag("model_logged", "true")
        return run.info.run_id


def model_meta(result: TrainResult, data_info: dict[str, Any]) -> dict[str, Any]:
    """Everything inference needs to reproduce the model's inputs."""
    if result.target == "volatility":
        window = VOL_REGIME_WINDOW_MINUTES
        label = f"1 if realized vol over (t, t+{result.horizon}m] > its trailing {window}m median at t"
        lookback = max(MAX_LOOKBACK_MINUTES, VOL_REGIME_WINDOW_MINUTES + result.horizon)
    else:
        label = f"1 if log(close[t+{result.horizon}m] / close[t]) > 0 else 0"
        lookback = MAX_LOOKBACK_MINUTES
    return {
        "target": result.target,
        "model": result.selected_model,
        "feature_columns": result.feature_columns,
        "horizon_minutes": result.horizon,
        "label": label,
        "products": data_info["products"],
        "max_lookback_minutes": lookback,
        "trained_on": {k: data_info.get(k) for k in ("table_uri", "table_version", "start", "end")},
    }


def latest_run_id(experiment: str = DEFAULT_EXPERIMENT) -> str:
    """Most recent finished run in `experiment` that logged a model."""
    configure()
    runs = mlflow.search_runs(
        experiment_names=[experiment],
        filter_string="attributes.status = 'FINISHED' and tags.model_logged = 'true'",
        order_by=["attributes.start_time DESC"],
        max_results=1,
    )
    if runs.empty:
        raise LookupError(f"no finished runs with a model in MLflow experiment '{experiment}'")
    return str(runs.iloc[0]["run_id"])


def load_model(run_id: str) -> tuple[Any, dict[str, Any]]:
    """The fitted model and its metadata (feature columns, horizon, products) for a run."""
    configure()
    model = mlflow.sklearn.load_model(f"runs:/{run_id}/{MODEL_ARTIFACT}")
    meta = mlflow.artifacts.load_dict(f"runs:/{run_id}/{META_ARTIFACT}")
    return model, meta
