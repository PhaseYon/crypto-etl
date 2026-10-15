from datetime import UTC, datetime

import numpy as np
import pandas as pd

from crypto_ml.data import read_ohlcv
from crypto_ml.synthetic import generate_ohlcv, main, write_ohlcv_delta

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def test_candles_are_internally_consistent():
    df = generate_ohlcv(["BTC-USD", "ETH-USD"], T0, 600, seed=1)
    assert set(df["product_id"]) == {"BTC-USD", "ETH-USD"}
    assert (df["low"] <= df[["open", "close"]].min(axis=1)).all()
    assert (df["high"] >= df[["open", "close"]].max(axis=1)).all()
    assert (df["vwap"] >= df["low"] - 1e-9).all() and (df["vwap"] <= df["high"] + 1e-9).all()
    assert np.allclose(df["buy_volume"] + df["sell_volume"], df["volume"])
    assert (df["trade_count"] > 0).all()
    assert (df["window_start"].dt.second == 0).all()
    assert (df["window_end"] - df["window_start"] == pd.Timedelta(minutes=1)).all()
    assert not df.duplicated(["product_id", "window_start"]).any()


def test_deterministic_for_seed():
    a = generate_ohlcv(["BTC-USD"], T0, 100, seed=7)
    b = generate_ohlcv(["BTC-USD"], T0, 100, seed=7)
    pd.testing.assert_frame_equal(a, b)
    assert not a.equals(generate_ohlcv(["BTC-USD"], T0, 100, seed=8))


def test_has_missing_minutes_when_trading_is_sparse():
    df = generate_ohlcv(["SOL-USD"], T0, 500, trades_per_minute=1.0, seed=3)
    assert 0 < len(df) < 500


def test_drop_rate_removes_rows():
    full = generate_ohlcv(["BTC-USD"], T0, 1000, seed=2)
    dropped = generate_ohlcv(["BTC-USD"], T0, 1000, seed=2, drop_rate=0.3)
    assert len(dropped) < 0.8 * len(full)


def test_roundtrip_through_delta_matches_contract(tmp_path):
    df = generate_ohlcv(["BTC-USD", "ETH-USD"], T0, 2000, seed=0)  # crosses a day boundary
    write_ohlcv_delta(df, str(tmp_path))
    back = read_ohlcv(str(tmp_path)).frame
    assert len(back) == len(df)
    assert back["window_date"].nunique() == 2
    pd.testing.assert_series_equal(back["close"], df["close"], check_names=False)


def test_cli_writes_table(tmp_path):
    main(["--uri", str(tmp_path), "--products", "BTC-USD", "--days", "0.1"])
    assert len(read_ohlcv(str(tmp_path)).frame) > 100
