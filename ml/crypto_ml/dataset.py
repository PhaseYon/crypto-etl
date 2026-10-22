"""Labels, the modelling dataset, and leakage-safe time-based splits."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import numpy as np
import pandas as pd

from crypto_ml.features import (
    FEATURE_COLUMNS,
    REALIZED_VOL,
    VOL_REGIME,
    VOL_THRESHOLD,
    add_vol_regime,
    build_features,
)

LABEL = "label"
# direction: label = 1 if the forward log return over the horizon is > 0, else 0 (down or flat)
TARGET_RETURN = "fwd_ret"
# volatility: label = 1 if realized vol over the next horizon minutes exceeds its trailing-day median
TARGET_VOL = "fwd_vol"
TARGETS = ("direction", "volatility")
# Realized value each target's IC is measured against
TARGET_VALUE = {"direction": TARGET_RETURN, "volatility": TARGET_VOL}


def add_labels(features: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Add the forward log return over `horizon` minutes and its direction label.

    Rows whose label window runs past the end of the data are dropped.
    """
    if horizon < 1:
        raise ValueError("horizon must be >= 1 minute")
    out = features.copy()
    future_close = out.groupby("product_id", sort=False)["close"].shift(-horizon)
    out[TARGET_RETURN] = np.log(future_close / out["close"])
    out = out[out[TARGET_RETURN].notna()].copy()
    out[LABEL] = (out[TARGET_RETURN] > 0).astype("int8")
    return out.reset_index(drop=True)


def add_volatility_labels(features: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Add the vol regime feature, realized vol over the next `horizon` minutes, and its label.

    The label compares future vol with the threshold known at t (the trailing-day median), so the
    threshold itself never sees the future. Rows whose label window runs past the data, or that are
    still warming up the threshold, are dropped.
    """
    if horizon < 2:
        raise ValueError("volatility horizon must be >= 2 minutes (a standard deviation needs 2 returns)")
    out = add_vol_regime(features, horizon)
    out[TARGET_VOL] = out.groupby("product_id", sort=False)[REALIZED_VOL].shift(-horizon)
    out = out[out[TARGET_VOL].notna() & out[VOL_THRESHOLD].notna()].copy()
    out[LABEL] = (out[TARGET_VOL] > out[VOL_THRESHOLD]).astype("int8")
    return out.reset_index(drop=True)


def feature_columns_for(target: str) -> list[str]:
    """Every causal feature available to models of `target`."""
    if target not in TARGETS:
        raise ValueError(f"unknown target {target!r}; expected one of {TARGETS}")
    return FEATURE_COLUMNS + [VOL_REGIME] if target == "volatility" else list(FEATURE_COLUMNS)


def build_dataset(ohlcv: pd.DataFrame, horizon: int, target: str = "direction") -> pd.DataFrame:
    """Features + labels with warm-up and incomplete-label rows removed, sorted by time."""
    cols = feature_columns_for(target)
    features = build_features(ohlcv)
    data = add_volatility_labels(features, horizon) if target == "volatility" else add_labels(features, horizon)
    data = data.dropna(subset=cols)
    return data.sort_values(["window_start", "product_id"]).reset_index(drop=True)


@dataclass(frozen=True)
class TimeSplit:
    train: pd.DataFrame
    val: pd.DataFrame
    test: pd.DataFrame


def _check_embargo(horizon: int, embargo: int | None) -> pd.Timedelta:
    embargo = horizon if embargo is None else embargo
    if embargo < horizon:
        # A smaller gap lets training labels (which look `horizon` minutes ahead) see the next split's prices.
        raise ValueError(f"embargo ({embargo} min) must be >= horizon ({horizon} min)")
    return pd.Timedelta(minutes=embargo)


def time_split(
    data: pd.DataFrame,
    horizon: int,
    train_frac: float = 0.7,
    val_frac: float = 0.15,
    embargo: int | None = None,
) -> TimeSplit:
    """Chronological train/val/test split with an embargo gap (default = horizon) between parts.

    No row of a later split is within `embargo` minutes of an earlier split, so no training
    label window overlaps validation/test data.
    """
    if not (0 < train_frac < 1 and 0 < val_frac < 1 and train_frac + val_frac < 1):
        raise ValueError("need 0 < train_frac, val_frac and train_frac + val_frac < 1")
    gap = _check_embargo(horizon, embargo)
    times = np.sort(data["window_start"].unique())
    t1 = pd.Timestamp(times[int(len(times) * train_frac)])
    t2 = pd.Timestamp(times[int(len(times) * (train_frac + val_frac))])
    ts = data["window_start"]
    split = TimeSplit(
        train=data[ts < t1].reset_index(drop=True),
        val=data[(ts >= t1 + gap) & (ts < t2)].reset_index(drop=True),
        test=data[ts >= t2 + gap].reset_index(drop=True),
    )
    for name in ("train", "val", "test"):
        if getattr(split, name).empty:
            raise ValueError(f"{name} split is empty; need more data for horizon={horizon}")
    return split


def walk_forward_splits(
    data: pd.DataFrame,
    horizon: int,
    n_splits: int = 5,
    min_train_frac: float = 0.5,
    embargo: int | None = None,
) -> Iterator[tuple[pd.Index, pd.Index]]:
    """Expanding-window CV: (train_index, test_index) pairs over consecutive future test blocks.

    Train rows end `embargo` minutes before each test block starts.
    """
    gap = _check_embargo(horizon, embargo)
    times = np.sort(data["window_start"].unique())
    first_test = int(len(times) * min_train_frac)
    bounds = np.linspace(first_test, len(times), n_splits + 1).astype(int)
    ts = data["window_start"]
    for lo, hi in zip(bounds[:-1], bounds[1:], strict=True):
        if hi <= lo:
            continue
        start = pd.Timestamp(times[lo])
        end = pd.Timestamp(times[hi]) if hi < len(times) else None
        train_idx = data.index[ts < start - gap]
        test_mask = ts >= start if end is None else (ts >= start) & (ts < end)
        test_idx = data.index[test_mask]
        if len(train_idx) and len(test_idx):
            yield train_idx, test_idx
