from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from crypto_ml.dataset import LABEL, TARGET_RETURN, build_dataset
from crypto_ml.features import FEATURE_COLUMNS
from crypto_ml.models import MajorityBaseline, MomentumBaseline, evaluate
from crypto_ml.synthetic import generate_ohlcv, write_ohlcv_delta
from crypto_ml.train import main
from crypto_ml.training import train_and_evaluate, walk_forward_evaluate

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


def regime_ohlcv(products=("BTC-USD",), segments: int = 24, minutes: int = 180, seed: int = 0) -> pd.DataFrame:
    """Candles whose volatility switches between calm and wild regimes, so it clusters in time."""
    rng = np.random.default_rng(seed)
    parts = []
    for product in products:
        last_close = None
        for k in range(segments):
            vol = 0.0002 if k % 2 == 0 else rng.uniform(0.0006, 0.001)
            seg = generate_ohlcv(
                [product], T0 + pd.Timedelta(minutes=k * minutes), minutes, seed=seed * 1000 + k, per_trade_vol=vol
            )
            if last_close is not None:  # continue the price path instead of restarting it
                scale = last_close / seg["open"].iloc[0]
                seg[["open", "high", "low", "close", "vwap", "quote_volume"]] *= scale
            last_close = seg["close"].iloc[-1]
            parts.append(seg)
    return pd.concat(parts, ignore_index=True)


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


def test_volatility_model_learns_clustering_and_beats_baselines():
    data = build_dataset(regime_ohlcv(), horizon=15, target="volatility")
    r = train_and_evaluate(data, horizon=15, target="volatility")
    assert r.target == "volatility" and r.selected_model in ("gbm", "logit")
    assert r.metrics.keys() == {"gbm", "logit", "majority", "persistence"}
    chosen = r.metrics[r.selected_model]["test"]
    assert chosen["roc_auc"] > 0.7
    assert chosen["log_loss"] < r.metrics["majority"]["test"]["log_loss"]
    assert r.model is not None and len(r.feature_columns) == r.model.n_features_in_


def test_selection_uses_validation_log_loss():
    data = build_dataset(regime_ohlcv(), horizon=15, target="volatility")
    r = train_and_evaluate(data, horizon=15, target="volatility")
    other = "gbm" if r.selected_model == "logit" else "logit"
    assert r.metrics[r.selected_model]["val"]["log_loss"] <= r.metrics[other]["val"]["log_loss"]


def test_walk_forward_reports_mean_metrics_per_model():
    data = build_dataset(regime_ohlcv(), horizon=15, target="volatility")
    wf = walk_forward_evaluate(data, horizon=15, n_splits=3, target="volatility")
    assert wf.keys() == {"gbm", "logit", "majority", "persistence"}
    assert wf["logit"]["folds"] == 3
    assert wf["majority"]["roc_auc"] == 0.5
    assert wf["logit"]["roc_auc"] > 0.7 and wf["logit"]["roc_auc_min"] <= wf["logit"]["roc_auc"]


def test_volatility_cli_logs_to_its_own_experiment(tmp_path, capsys):
    import mlflow

    from crypto_ml.tracking import configure, latest_run_id, load_model

    write_ohlcv_delta(regime_ohlcv(("BTC-USD", "ETH-USD"), segments=16), str(tmp_path))
    args = ["--products", "BTC-USD,ETH-USD", "--table-uri", str(tmp_path), "--horizon", "15"]
    result = main([*args, "--target", "volatility", "--walk-forward", "3"])
    assert "wf" in capsys.readouterr().out and result.walk_forward

    run_id = latest_run_id("crypto-volatility")
    configure()
    run = mlflow.get_run(run_id)
    assert run.data.params["target"] == "volatility"
    assert run.data.tags["selected_model"] == result.selected_model
    assert "wf.logit.roc_auc" in run.data.metrics
    model, meta = load_model(run_id)
    assert meta["target"] == "volatility" and meta["feature_columns"] == result.feature_columns
    assert meta["max_lookback_minutes"] >= 1440
    X = build_dataset(regime_ohlcv(("BTC-USD",), segments=8, seed=3), 15, target="volatility")[result.feature_columns]
    np.testing.assert_allclose(model.predict_proba(X), result.model.predict_proba(X))

    with pytest.raises(LookupError):  # direction's default experiment stays untouched
        latest_run_id("crypto-direction")
