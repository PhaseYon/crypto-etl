"""Train the model and baselines on a time split and evaluate them side by side."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

from crypto_ml.dataset import LABEL, TARGET_RETURN, TimeSplit, time_split
from crypto_ml.features import FEATURE_COLUMNS
from crypto_ml.models import MajorityBaseline, MomentumBaseline, best_iteration, evaluate, make_gbm


@dataclass
class TrainResult:
    model: HistGradientBoostingClassifier
    feature_columns: list[str]
    horizon: int
    best_iteration: int
    # metrics[model_name][split_name] -> {metric: value}
    metrics: dict[str, dict[str, dict[str, float]]] = field(default_factory=dict)
    split_sizes: dict[str, int] = field(default_factory=dict)
    split_bounds: dict[str, str] = field(default_factory=dict)

    def flat_metrics(self) -> dict[str, float]:
        """{'gbm.test.roc_auc': 0.51, ...} for experiment tracking."""
        return {
            f"{m}.{s}.{k}": v
            for m, splits in self.metrics.items()
            for s, vals in splits.items()
            for k, v in vals.items()
        }


def train_and_evaluate(
    data: pd.DataFrame,
    horizon: int,
    seed: int = 42,
    train_frac: float = 0.7,
    val_frac: float = 0.15,
    embargo: int | None = None,
    feature_columns: list[str] | None = None,
) -> TrainResult:
    cols = feature_columns or FEATURE_COLUMNS
    split = time_split(data, horizon, train_frac=train_frac, val_frac=val_frac, embargo=embargo)
    X_tr, y_tr = split.train[cols], split.train[LABEL]

    # Pick the iteration count on validation, then refit on train only with that count.
    probe = make_gbm(seed).fit(X_tr, y_tr)
    n_iter = best_iteration(probe, split.val[cols], split.val[LABEL])
    model = make_gbm(seed, max_iter=n_iter).fit(X_tr, y_tr)

    candidates = {
        "gbm": model,
        "majority": MajorityBaseline().fit(X_tr, y_tr),
        "momentum": MomentumBaseline().fit(X_tr, y_tr),
    }
    result = TrainResult(model=model, feature_columns=list(cols), horizon=horizon, best_iteration=n_iter)
    for name, est in candidates.items():
        result.metrics[name] = {
            part: evaluate(df[LABEL], est.predict_proba(df[cols])[:, 1], df[TARGET_RETURN])
            for part, df in (("val", split.val), ("test", split.test))
        }
    result.split_sizes = {p: len(getattr(split, p)) for p in ("train", "val", "test")}
    result.split_bounds = _bounds(split)
    return result


def _bounds(split: TimeSplit) -> dict[str, str]:
    out = {}
    for part in ("train", "val", "test"):
        ts = getattr(split, part)["window_start"]
        out[f"{part}_start"], out[f"{part}_end"] = ts.min().isoformat(), ts.max().isoformat()
    return out
