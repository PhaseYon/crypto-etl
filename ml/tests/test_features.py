from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from crypto_ml.features import FEATURE_COLUMNS, MAX_LOOKBACK_MINUTES, add_vol_regime, build_features, to_regular_grid
from crypto_ml.synthetic import generate_ohlcv

T0 = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def ohlcv():
    return generate_ohlcv(["BTC-USD", "ETH-USD"], T0, 600, seed=11, trades_per_minute=3.0, drop_rate=0.05)


def test_grid_fills_missing_minutes(ohlcv):
    grid = to_regular_grid(ohlcv)
    for _, g in grid.groupby("product_id"):
        assert (g["window_start"].diff().dropna() == pd.Timedelta(minutes=1)).all()
    filled = grid[grid["is_filled"] == 1]
    assert len(filled) > 0
    assert (filled["volume"] == 0).all() and (filled["trade_count"] == 0).all()
    assert (filled["open"] == filled["close"]).all() and (filled["high"] == filled["close"]).all()
    assert not grid["close"].isna().any()


def test_grid_carries_previous_close():
    df = generate_ohlcv(["BTC-USD"], T0, 5, seed=0, trades_per_minute=50)
    df = df.drop(index=2).reset_index(drop=True)  # remove the 3rd minute
    grid = to_regular_grid(df)
    assert grid.loc[2, "is_filled"] == 1
    assert grid.loc[2, "close"] == grid.loc[1, "close"]


def test_columns_and_warmup(ohlcv):
    f = build_features(ohlcv)
    assert set(FEATURE_COLUMNS) <= set(f.columns)
    for _, g in f.groupby("product_id"):
        g = g.reset_index(drop=True)
        assert g.loc[: MAX_LOOKBACK_MINUTES - 2, "vol_60"].isna().all()  # warm-up rows
        assert not g.loc[MAX_LOOKBACK_MINUTES:, FEATURE_COLUMNS].isna().any().any()
    assert np.isfinite(f[FEATURE_COLUMNS].dropna().to_numpy()).all()


def test_return_feature_values():
    df = generate_ohlcv(["BTC-USD"], T0, 30, seed=0, trades_per_minute=50)
    f = build_features(df)
    expected = np.log(f["close"].iloc[20] / f["close"].iloc[15])
    assert f["ret_5"].iloc[20] == pytest.approx(expected)


def test_products_do_not_leak_into_each_other(ohlcv):
    together = build_features(ohlcv)
    alone = build_features(ohlcv[ohlcv["product_id"] == "ETH-USD"])
    pd.testing.assert_frame_equal(
        together[together["product_id"] == "ETH-USD"].reset_index(drop=True), alone.reset_index(drop=True)
    )


def test_features_are_causal(ohlcv):
    """Changing or removing anything after cutoff must not change any feature at or before cutoff."""
    cutoff = pd.Timestamp(T0) + pd.Timedelta(minutes=400)
    base = build_features(ohlcv)

    perturbed = ohlcv.copy()
    future = perturbed["window_start"] > cutoff
    for col in ("open", "high", "low", "close", "vwap"):
        perturbed.loc[future, col] *= 3.0
    perturbed.loc[future, ["volume", "quote_volume", "buy_volume"]] *= 10.0
    truncated = ohlcv[~future]

    def past(frame):
        return frame[frame["window_start"] <= cutoff].reset_index(drop=True)

    pd.testing.assert_frame_equal(past(base), past(build_features(perturbed)))
    pd.testing.assert_frame_equal(past(base), past(build_features(truncated)))


def test_empty_input():
    empty = generate_ohlcv([], T0, 10)
    assert build_features(empty).empty


def test_vol_regime_is_causal(ohlcv):
    cutoff = pd.Timestamp(T0) + pd.Timedelta(minutes=400)
    future = ohlcv["window_start"] > cutoff
    perturbed = ohlcv.copy()
    for col in ("open", "high", "low", "close", "vwap"):
        perturbed.loc[future, col] *= 3.0

    def past(frame):
        regime = add_vol_regime(build_features(frame), 15)
        return regime[regime["window_start"] <= cutoff].reset_index(drop=True)

    pd.testing.assert_frame_equal(past(ohlcv), past(perturbed))
    pd.testing.assert_frame_equal(past(ohlcv), past(ohlcv[~future]))
