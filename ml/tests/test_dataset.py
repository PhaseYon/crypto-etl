from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from crypto_ml.dataset import (
    LABEL,
    TARGET_RETURN,
    TARGET_VOL,
    add_labels,
    add_volatility_labels,
    build_dataset,
    feature_columns_for,
    time_split,
    walk_forward_splits,
)
from crypto_ml.features import FEATURE_COLUMNS, VOL_REGIME, build_features
from crypto_ml.synthetic import generate_ohlcv

T0 = datetime(2026, 1, 1, tzinfo=UTC)
H = 5


@pytest.fixture(scope="module")
def data():
    ohlcv = generate_ohlcv(["BTC-USD", "ETH-USD"], T0, 1500, seed=5, trades_per_minute=4.0)
    return build_dataset(ohlcv, horizon=H)


def test_labels_look_exactly_horizon_ahead():
    f = build_features(generate_ohlcv(["BTC-USD"], T0, 50, seed=0, trades_per_minute=50))
    labelled = add_labels(f, H)
    assert len(labelled) == len(f) - H  # last H rows have incomplete label windows
    i = 10
    expected = np.log(f["close"].iloc[i + H] / f["close"].iloc[i])
    assert labelled[TARGET_RETURN].iloc[i] == pytest.approx(expected)
    assert labelled[LABEL].iloc[i] == int(expected > 0)


def test_labels_do_not_cross_products():
    ohlcv = generate_ohlcv(["BTC-USD", "ETH-USD"], T0, 30, seed=0, trades_per_minute=50)
    labelled = add_labels(build_features(ohlcv), H)
    assert labelled.groupby("product_id").size().to_dict() == {"BTC-USD": 30 - H, "ETH-USD": 30 - H}


def test_dataset_has_no_missing_features(data):
    assert not data[FEATURE_COLUMNS + [LABEL]].isna().any().any()
    assert data["window_start"].is_monotonic_increasing


def test_time_split_is_chronological_with_embargo(data):
    s = time_split(data, horizon=H)
    gap = pd.Timedelta(minutes=H)
    assert s.train["window_start"].max() < s.val["window_start"].min()
    assert s.val["window_start"].max() < s.test["window_start"].min()
    # Embargo: the last training label window ends before validation starts.
    assert s.train["window_start"].max() + gap < s.val["window_start"].min()
    assert s.val["window_start"].max() + gap < s.test["window_start"].min()
    assert len(s.train) > len(s.val) > 0 and len(s.test) > 0


def test_larger_embargo_widens_gap(data):
    s = time_split(data, horizon=H, embargo=60)
    assert s.val["window_start"].min() - s.train["window_start"].max() > pd.Timedelta(minutes=60)


def test_embargo_shorter_than_horizon_is_rejected(data):
    with pytest.raises(ValueError, match="embargo"):
        time_split(data, horizon=H, embargo=H - 1)


def test_split_rejects_too_little_data():
    tiny = build_dataset(generate_ohlcv(["BTC-USD"], T0, 70, seed=0, trades_per_minute=50), horizon=H)
    with pytest.raises(ValueError, match="empty"):
        time_split(tiny, horizon=H, embargo=50)


def test_walk_forward_is_expanding_and_embargoed(data):
    folds = list(walk_forward_splits(data, horizon=H, n_splits=4))
    assert len(folds) == 4
    gap = pd.Timedelta(minutes=H)
    prev_train, prev_test_end = 0, None
    for train_idx, test_idx in folds:
        train_t, test_t = data.loc[train_idx, "window_start"], data.loc[test_idx, "window_start"]
        assert train_t.max() + gap < test_t.min()
        assert len(train_idx) > prev_train  # expanding window
        if prev_test_end is not None:
            assert test_t.min() > prev_test_end  # test blocks don't overlap
        prev_train, prev_test_end = len(train_idx), test_t.max()


def test_volatility_labels_compare_future_vol_with_threshold_known_now():
    f = build_features(generate_ohlcv(["BTC-USD"], T0, 900, seed=1, trades_per_minute=30))
    labelled = add_volatility_labels(f, H)
    row = labelled.iloc[100]
    i = f.index[f["window_start"] == row["window_start"]][0]
    future_returns = np.log(f["close"]).diff().iloc[i + 1 : i + 1 + H]
    assert row[TARGET_VOL] == pytest.approx(future_returns.std())
    assert row[LABEL] == int(row[TARGET_VOL] > row["vol_threshold"])
    # The threshold needs VOL_REGIME_MIN_MINUTES of history; the last H rows have no complete label
    assert labelled["window_start"].min() > f["window_start"].min() + pd.Timedelta(minutes=300)
    assert labelled["window_start"].max() == f["window_start"].max() - pd.Timedelta(minutes=H)


def test_volatility_dataset_has_regime_feature_and_no_gaps():
    ohlcv = generate_ohlcv(["BTC-USD", "ETH-USD"], T0, 1200, seed=5, trades_per_minute=10.0)
    data = build_dataset(ohlcv, horizon=H, target="volatility")
    cols = feature_columns_for("volatility")
    assert VOL_REGIME in cols and not data[cols + [LABEL, TARGET_VOL]].isna().any().any()
    assert 0.2 < data[LABEL].mean() < 0.8


def test_unknown_target_and_tiny_volatility_horizon_are_rejected():
    with pytest.raises(ValueError, match="unknown target"):
        feature_columns_for("price")
    with pytest.raises(ValueError, match=">= 2"):
        add_volatility_labels(build_features(generate_ohlcv(["BTC-USD"], T0, 50, seed=0)), 1)
