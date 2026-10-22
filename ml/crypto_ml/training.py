"""Train the model and baselines on a time split and evaluate them side by side."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from crypto_ml.dataset import LABEL, TARGET_VALUE, TimeSplit, feature_columns_for, time_split, walk_forward_splits
from crypto_ml.features import VOL_REGIME
from crypto_ml.models import (
    MajorityBaseline,
    MomentumBaseline,
    best_iteration,
    evaluate,
    make_gbm,
    make_logit,
)

# Models eligible to be shipped per target; everything else fitted is a baseline for comparison.
# The candidate with the lowest validation log loss is selected.
CANDIDATES = {"direction": ("gbm",), "volatility": ("gbm", "logit")}
# Share of a walk-forward fold's training window held back to choose boosting iterations
INNER_VAL_FRAC = 0.15

Fitted = dict[str, tuple[Any, list[str]]]  # model name -> (fitted estimator, its feature columns)


@dataclass
class TrainResult:
    model: Any
    feature_columns: list[str]
    horizon: int
    best_iteration: int
    target: str = "direction"
    selected_model: str = "gbm"
    # metrics[model_name][split_name] -> {metric: value}
    metrics: dict[str, dict[str, dict[str, float]]] = field(default_factory=dict)
    split_sizes: dict[str, int] = field(default_factory=dict)
    split_bounds: dict[str, str] = field(default_factory=dict)
    # walk_forward[model_name] -> {metric: mean over folds}, plus "folds"
    walk_forward: dict[str, dict[str, float]] = field(default_factory=dict)

    def flat_metrics(self) -> dict[str, float]:
        """{'gbm.test.roc_auc': 0.51, 'wf.gbm.roc_auc': 0.52, ...} for experiment tracking."""
        flat = {
            f"{m}.{s}.{k}": v
            for m, splits in self.metrics.items()
            for s, vals in splits.items()
            for k, v in vals.items()
        }
        flat.update({f"wf.{m}.{k}": v for m, vals in self.walk_forward.items() for k, v in vals.items()})
        return flat


def fit_models(train: pd.DataFrame, val: pd.DataFrame, target: str, seed: int) -> tuple[Fitted, int]:
    """Fit candidates and baselines on `train`; `val` only chooses the boosting iteration count."""
    cols = feature_columns_for(target)
    X_tr, y_tr = train[cols], train[LABEL]

    # Pick the iteration count on validation, then refit on train only with that count.
    probe = make_gbm(seed).fit(X_tr, y_tr)
    n_iter = best_iteration(probe, val[cols], val[LABEL])
    fitted: Fitted = {"gbm": (make_gbm(seed, max_iter=n_iter).fit(X_tr, y_tr), cols)}
    if target == "volatility":
        regime = [VOL_REGIME]
        fitted["logit"] = (make_logit(seed).fit(train[regime], y_tr), regime)
        fitted["majority"] = (MajorityBaseline().fit(X_tr, y_tr), cols)
        # "Volatility stays where it is": P(high) learned separately for above/below-median vol now
        fitted["persistence"] = (MomentumBaseline(VOL_REGIME).fit(train[regime], y_tr), regime)
    else:
        fitted["majority"] = (MajorityBaseline().fit(X_tr, y_tr), cols)
        fitted["momentum"] = (MomentumBaseline().fit(X_tr, y_tr), cols)
    return fitted, n_iter


def _evaluate(fitted: Fitted, df: pd.DataFrame, target: str) -> dict[str, dict[str, float]]:
    return {
        name: evaluate(df[LABEL], est.predict_proba(df[cols])[:, 1], df[TARGET_VALUE[target]])
        for name, (est, cols) in fitted.items()
    }


def train_and_evaluate(
    data: pd.DataFrame,
    horizon: int,
    seed: int = 42,
    train_frac: float = 0.7,
    val_frac: float = 0.15,
    embargo: int | None = None,
    target: str = "direction",
) -> TrainResult:
    split = time_split(data, horizon, train_frac=train_frac, val_frac=val_frac, embargo=embargo)
    fitted, n_iter = fit_models(split.train, split.val, target, seed)
    val_metrics = _evaluate(fitted, split.val, target)
    test_metrics = _evaluate(fitted, split.test, target)

    selected = min(CANDIDATES[target], key=lambda name: val_metrics[name]["log_loss"])
    model, cols = fitted[selected]
    result = TrainResult(
        model=model,
        feature_columns=list(cols),
        horizon=horizon,
        best_iteration=n_iter,
        target=target,
        selected_model=selected,
    )
    result.metrics = {name: {"val": val_metrics[name], "test": test_metrics[name]} for name in fitted}
    result.split_sizes = {p: len(getattr(split, p)) for p in ("train", "val", "test")}
    result.split_bounds = _bounds(split)
    return result


def walk_forward_evaluate(
    data: pd.DataFrame,
    horizon: int,
    n_splits: int = 5,
    seed: int = 42,
    embargo: int | None = None,
    target: str = "direction",
) -> dict[str, dict[str, float]]:
    """Mean metrics per model over expanding-window folds, each fold fitted from scratch.

    Within a fold, the last INNER_VAL_FRAC of the training window (after an embargo) chooses the
    boosting iteration count; the test block is only scored.
    """
    gap = pd.Timedelta(minutes=horizon if embargo is None else embargo)
    per_model: dict[str, list[dict[str, float]]] = {}
    for train_idx, test_idx in walk_forward_splits(data, horizon, n_splits=n_splits, embargo=embargo):
        train = data.loc[train_idx]
        cut = train["window_start"].quantile(1 - INNER_VAL_FRAC)
        fit_part = train[train["window_start"] < cut - gap]
        val_part = train[train["window_start"] >= cut]
        if fit_part.empty or val_part.empty:
            continue
        fitted, _ = fit_models(fit_part, val_part, target, seed)
        # Refit on the whole training window now that the iteration count is fixed
        fitted = {name: (est.fit(train[cols], train[LABEL]), cols) for name, (est, cols) in fitted.items()}
        for name, m in _evaluate(fitted, data.loc[test_idx], target).items():
            per_model.setdefault(name, []).append(m)
    summary = {}
    for name, folds in per_model.items():
        # A constant baseline has no IC on any fold; report NaN without numpy's empty-mean warning
        summary[name] = {k: _nanmean([f[k] for f in folds]) for k in folds[0]}
        summary[name]["roc_auc_min"] = float(np.nanmin([f["roc_auc"] for f in folds]))
        summary[name]["folds"] = float(len(folds))
    return summary


def _nanmean(values: list[float]) -> float:
    finite = [v for v in values if not np.isnan(v)]
    return float(np.mean(finite)) if finite else float("nan")


def _bounds(split: TimeSplit) -> dict[str, str]:
    out = {}
    for part in ("train", "val", "test"):
        ts = getattr(split, part)["window_start"]
        out[f"{part}_start"], out[f"{part}_end"] = ts.min().isoformat(), ts.max().isoformat()
    return out
