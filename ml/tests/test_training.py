from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from crypto_ml.dataset import LABEL, TARGET_RETURN, build_dataset
from crypto_ml.features import FEATURE_COLUMNS
from crypto_ml.models import MajorityBaseline, MomentumBaseline, evaluate
from crypto_ml.synthetic import generate_ohlcv, write_ohlcv_delta
from crypto_ml.train import main
from crypto_ml.training import train_and_evaluate

T0 = datetime(2026, 1, 1, tzinfo=UTC)
H = 5


def _signal_dataset(n: int = 6000, seed: int = 0) -> pd.DataFrame:
    """Features with a planted relationship: the label depends on imbalance_15."""
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(rng.normal(size=(n, len(FEATURE_COLUMNS))), columns=FEATURE_COLUMNS)
    df["window_start"] = pd.date_range(T0, periods=n, freq="min")
    df["product_id"] = "BTC-USD"
    df[TARGET_RETURN] = 0.8 * df["imbalance_15"] + rng.normal(size=n)
    df[LABEL] = (df[TARGET_RETURN] > 0).astype("int8")
    return df


def test_model_learns_planted_signal_and_beats_baselines():
    result = train_and_evaluate(_signal_dataset(), horizon=H, seed=1)
    test = result.metrics
    assert test["gbm"]["test"]["roc_auc"] > 0.65
    assert test["majority"]["test"]["roc_auc"] == 0.5  # constant score ranks nothing
    assert test["gbm"]["test"]["log_loss"] < test["majority"]["test"]["log_loss"]
    assert test["gbm"]["test"]["ic"] > 0.2
    assert 1 <= result.best_iteration <= 400


def test_training_is_deterministic():
    a = train_and_evaluate(_signal_dataset(n=2000, seed=3), horizon=H, seed=7)
    b = train_and_evaluate(_signal_dataset(n=2000, seed=3), horizon=H, seed=7)
    assert a.flat_metrics() == pytest.approx(b.flat_metrics(), nan_ok=True)


def test_flat_metrics_and_split_info():
    r = train_and_evaluate(_signal_dataset(n=3000), horizon=H)
    flat = r.flat_metrics()
    assert "gbm.test.roc_auc" in flat and "momentum.val.accuracy" in flat
    assert r.split_sizes["train"] > r.split_sizes["val"] > 0
    assert r.split_bounds["train_end"] < r.split_bounds["val_start"]


def test_baselines():
    X = pd.DataFrame({"ret_5": [0.1, 0.2, -0.1, -0.2]})
    y = pd.Series([1, 1, 0, 1])
    assert MajorityBaseline().fit(X, y).predict_proba(X)[:, 1] == pytest.approx([0.75] * 4)
    m = MomentumBaseline().fit(X, y)
    assert m.predict_proba(X)[:, 1] == pytest.approx([1 - 1e-6, 1 - 1e-6, 0.5, 0.5])


def test_evaluate_metrics():
    m = evaluate(pd.Series([0, 1, 1, 0]), np.array([0.2, 0.8, 0.6, 0.4]), pd.Series([-1.0, 2.0, 1.0, -0.5]))
    assert m["accuracy"] == 1.0 and m["roc_auc"] == 1.0 and m["ic"] == pytest.approx(1.0)
    single_class = evaluate(pd.Series([1, 1]), np.array([0.6, 0.7]))
    assert np.isnan(single_class["roc_auc"])


def test_random_walk_has_no_real_edge():
    """On a pure random walk the model must not look clearly better than chance."""
    data = build_dataset(generate_ohlcv(["BTC-USD", "ETH-USD"], T0, 4000, seed=9), horizon=H)
    r = train_and_evaluate(data, horizon=H)
    assert abs(r.metrics["gbm"]["test"]["roc_auc"] - 0.5) < 0.06


def test_cli_end_to_end_on_local_table(tmp_path, capsys):
    write_ohlcv_delta(generate_ohlcv(["BTC-USD"], T0, 3000, seed=4), str(tmp_path))
    result = main(
        [
            "--products",
            "BTC-USD",
            "--horizon",
            "5",
            "--table-uri",
            str(tmp_path),
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-03T00:00:00Z",
        ]
    )
    assert result.split_sizes["test"] > 0
    out = capsys.readouterr().out
    assert "gbm" in out and "momentum" in out


def test_cli_fails_clearly_without_data(tmp_path):
    write_ohlcv_delta(generate_ohlcv(["BTC-USD"], T0, 100, seed=4), str(tmp_path))
    with pytest.raises(SystemExit, match="no candles"):
        main(["--products", "ETH-USD", "--table-uri", str(tmp_path)])
