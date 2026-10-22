"""Feature engineering on gold/ohlcv_1m.

Every feature at minute t uses only candles at or before t: only backward shifts and
trailing rolling windows are allowed here. tests/test_features.py enforces this.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

RETURN_HORIZONS = (1, 5, 15, 60)
VOL_WINDOWS = (15, 60)
# Longest lookback any feature needs, in minutes (rolling 60 over 1-minute returns needs 61 closes).
MAX_LOOKBACK_MINUTES = 61

FEATURE_COLUMNS: list[str] = [
    *(f"ret_{h}" for h in RETURN_HORIZONS),
    *(f"vol_{w}" for w in VOL_WINDOWS),
    "range_hl",
    "vwap_dev",
    "vwap_dev_15",
    "volume_z_60",
    "imbalance_1",
    "imbalance_15",
    "trades_log",
    "trades_ratio_15_60",
    "filled_frac_15",
    "tod_sin",
    "tod_cos",
    "dow_sin",
    "dow_cos",
]

_ZERO_FILL = ["volume", "quote_volume", "trade_count", "buy_volume", "sell_volume"]


def to_regular_grid(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """One row per product per minute between its first and last candle.

    Minutes without trades get close carried forward (open/high/low/vwap = that close),
    zero volume and trade_count, and is_filled = 1.
    """
    parts = []
    for product, g in ohlcv.groupby("product_id", sort=True):
        g = g.set_index("window_start").sort_index()
        grid = pd.date_range(g.index.min(), g.index.max(), freq="min", name="window_start")
        r = g.reindex(grid)
        r["is_filled"] = r["close"].isna().astype("int8")
        r["close"] = r["close"].ffill()
        for col in ("open", "high", "low", "vwap"):
            r[col] = r[col].fillna(r["close"])
        for col in _ZERO_FILL:
            r[col] = r[col].fillna(0)
        r["trade_count"] = r["trade_count"].astype("int64")
        r["product_id"] = product
        parts.append(r.reset_index())
    cols = ["product_id", "window_start", "open", "high", "low", "close", "vwap", *_ZERO_FILL, "is_filled"]
    if not parts:
        return pd.DataFrame(columns=cols)
    return pd.concat(parts, ignore_index=True)[cols]


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    out = a / b.replace(0, np.nan)
    return out.fillna(0.0)


def _product_features(g: pd.DataFrame) -> pd.DataFrame:
    f = pd.DataFrame(index=g.index)
    log_close = np.log(g["close"])
    for h in RETURN_HORIZONS:
        f[f"ret_{h}"] = log_close.diff(h)
    ret_1 = log_close.diff(1)
    for w in VOL_WINDOWS:
        f[f"vol_{w}"] = ret_1.rolling(w, min_periods=w).std()

    f["range_hl"] = np.log(g["high"] / g["low"])
    f["vwap_dev"] = np.log(g["close"] / g["vwap"])
    # Deviation from the trailing 15-minute VWAP; 0 when nothing traded, NaN during warm-up.
    vol_15 = g["volume"].rolling(15, min_periods=15).sum()
    vwap_15 = g["quote_volume"].rolling(15, min_periods=15).sum() / vol_15.where(vol_15 > 0)
    f["vwap_dev_15"] = np.log(g["close"] / vwap_15).where(vol_15 > 0, 0.0).where(vol_15.notna())

    log_vol = np.log1p(g["volume"])
    mean_60 = log_vol.rolling(60, min_periods=60).mean()
    std_60 = log_vol.rolling(60, min_periods=60).std()
    f["volume_z_60"] = _safe_div(log_vol - mean_60, std_60).where(mean_60.notna())

    f["imbalance_1"] = _safe_div(g["buy_volume"] - g["sell_volume"], g["volume"])
    buy_15 = g["buy_volume"].rolling(15, min_periods=15).sum()
    sell_15 = g["sell_volume"].rolling(15, min_periods=15).sum()
    f["imbalance_15"] = _safe_div(buy_15 - sell_15, buy_15 + sell_15).where(buy_15.notna())

    f["trades_log"] = np.log1p(g["trade_count"].astype("float64"))
    tc = g["trade_count"].astype("float64")
    mean_tc_15 = tc.rolling(15, min_periods=15).mean()
    f["trades_ratio_15_60"] = _safe_div(mean_tc_15, tc.rolling(60, min_periods=60).mean()).where(
        tc.rolling(60, min_periods=60).mean().notna()
    )
    f["filled_frac_15"] = g["is_filled"].astype("float64").rolling(15, min_periods=15).mean()

    ts = g["window_start"]
    minute_of_day = ts.dt.hour * 60 + ts.dt.minute
    f["tod_sin"] = np.sin(2 * np.pi * minute_of_day / 1440)
    f["tod_cos"] = np.cos(2 * np.pi * minute_of_day / 1440)
    f["dow_sin"] = np.sin(2 * np.pi * ts.dt.dayofweek / 7)
    f["dow_cos"] = np.cos(2 * np.pi * ts.dt.dayofweek / 7)
    return f


VOL_REGIME = "vol_regime"  # log(realized vol over the last h minutes / its trailing-day median)
REALIZED_VOL = "realized_vol"
VOL_THRESHOLD = "vol_threshold"
VOL_REGIME_WINDOW_MINUTES = 1440
# The trailing median starts after 6 hours of history rather than a full day, so a week of data
# loses a quarter day instead of a whole one to warm-up.
VOL_REGIME_MIN_MINUTES = 360


def add_vol_regime(features: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Add realized volatility over the last `horizon` minutes, its trailing-day median and their log ratio.

    Expects build_features() output (one row per product per minute, sorted by time within a
    product). Everything is computed from minutes at or before t.
    """
    out = features.copy()
    by_product = out.groupby("product_id", sort=False)
    ret_1 = by_product["close"].transform(lambda c: np.log(c).diff())
    out[REALIZED_VOL] = ret_1.groupby(out["product_id"], sort=False).transform(
        lambda r: r.rolling(horizon, min_periods=horizon).std()
    )
    out[VOL_THRESHOLD] = out.groupby("product_id", sort=False)[REALIZED_VOL].transform(
        lambda v: v.rolling(VOL_REGIME_WINDOW_MINUTES, min_periods=VOL_REGIME_MIN_MINUTES).median()
    )
    # A flat stretch (zero vol) has no defined ratio; leave it NaN so the row is dropped, not infinite
    ratio = out[REALIZED_VOL] / out[VOL_THRESHOLD].where(out[VOL_THRESHOLD] > 0)
    out[VOL_REGIME] = np.log(ratio.where(ratio > 0))
    return out


def build_features(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Features per product and minute on a regular grid.

    Returns product_id, window_start, close, is_filled and FEATURE_COLUMNS. The first
    MAX_LOOKBACK_MINUTES rows of each product contain NaNs (warm-up); callers drop them.
    """
    grid = to_regular_grid(ohlcv)
    if grid.empty:
        return pd.DataFrame(columns=["product_id", "window_start", "close", "is_filled", *FEATURE_COLUMNS])
    feats = [
        pd.concat([g[["product_id", "window_start", "close", "is_filled"]], _product_features(g)], axis=1)
        for _, g in grid.groupby("product_id", sort=True)
    ]
    out = pd.concat(feats, ignore_index=True)
    out[FEATURE_COLUMNS] = out[FEATURE_COLUMNS].astype("float64")
    return out
