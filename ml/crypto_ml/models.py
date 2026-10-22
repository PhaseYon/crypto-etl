"""Baselines, the gradient-boosted model, and evaluation metrics."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score

_EPS = 1e-6


class MajorityBaseline(ClassifierMixin, BaseEstimator):
    """Always predicts the training base rate of 'up'."""

    def fit(self, X: pd.DataFrame, y: pd.Series) -> MajorityBaseline:
        self.classes_ = np.array([0, 1])
        self.p_up_ = float(np.clip(np.mean(y), _EPS, 1 - _EPS))
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        p = np.full(len(X), self.p_up_)
        return np.column_stack([1 - p, p])

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return (self.predict_proba(X)[:, 1] > 0.5).astype(int)


class MomentumBaseline(ClassifierMixin, BaseEstimator):
    """'Recent direction continues': P(up) estimated on train separately for feature > 0 and <= 0."""

    def __init__(self, feature: str = "ret_5"):
        self.feature = feature

    def fit(self, X: pd.DataFrame, y: pd.Series) -> MomentumBaseline:
        self.classes_ = np.array([0, 1])
        y = np.asarray(y)
        rising = X[self.feature].to_numpy() > 0
        prior = float(np.mean(y))
        self.p_up_if_rising_ = float(np.clip(y[rising].mean() if rising.any() else prior, _EPS, 1 - _EPS))
        self.p_up_if_falling_ = float(np.clip(y[~rising].mean() if (~rising).any() else prior, _EPS, 1 - _EPS))
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        p = np.where(X[self.feature].to_numpy() > 0, self.p_up_if_rising_, self.p_up_if_falling_)
        return np.column_stack([1 - p, p])

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return (self.predict_proba(X)[:, 1] > 0.5).astype(int)


def make_gbm(seed: int, max_iter: int = 400) -> HistGradientBoostingClassifier:
    # Built-in early stopping uses a random validation split, which would leak across time,
    # so it's disabled; the iteration count is chosen on the time-based validation split instead.
    return HistGradientBoostingClassifier(
        learning_rate=0.05,
        max_iter=max_iter,
        max_leaf_nodes=15,
        min_samples_leaf=100,
        l2_regularization=1.0,
        early_stopping=False,
        random_state=seed,
    )


def make_logit(seed: int) -> LogisticRegression:
    """Plain logistic regression: the simple challenger, meant for a handful of strong features."""
    return LogisticRegression(random_state=seed)


def best_iteration(model: HistGradientBoostingClassifier, X_val: pd.DataFrame, y_val: pd.Series) -> int:
    """Number of boosting iterations with the lowest validation log loss."""
    losses = [log_loss(y_val, proba[:, 1], labels=[0, 1]) for proba in model.staged_predict_proba(X_val)]
    return int(np.argmin(losses)) + 1


def evaluate(y_true: pd.Series, p_up: np.ndarray, fwd_ret: pd.Series | None = None) -> dict[str, float]:
    """Classification metrics for P(up), plus rank IC against the realized forward return."""
    y = np.asarray(y_true)
    p = np.clip(np.asarray(p_up, dtype=float), _EPS, 1 - _EPS)
    metrics = {
        "accuracy": float(accuracy_score(y, p > 0.5)),
        "log_loss": float(log_loss(y, p, labels=[0, 1])),
        "brier": float(brier_score_loss(y, p)),
        "positive_rate": float(y.mean()),
        "n": float(len(y)),
    }
    metrics["roc_auc"] = float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else float("nan")
    if fwd_ret is not None:
        ic = spearmanr(p, np.asarray(fwd_ret)).statistic if np.ptp(p) > 0 else np.nan
        metrics["ic"] = float(ic)
    return metrics
