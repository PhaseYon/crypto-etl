from datetime import UTC, datetime

import mlflow
import numpy as np
import pytest

from crypto_ml.synthetic import generate_ohlcv, write_ohlcv_delta
from crypto_ml.tracking import configure, latest_run_id, load_model
from crypto_ml.train import build_parser, run

T0 = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def table(tmp_path):
    path = tmp_path / "gold"
    write_ohlcv_delta(generate_ohlcv(["BTC-USD", "ETH-USD"], T0, 2500, seed=2), str(path))
    return str(path)


def _train(table, *extra):
    args = build_parser().parse_args(["--products", "BTC-USD,ETH-USD", "--horizon", "5",
                                      "--table-uri", table, "--experiment", "test-exp", *extra])
    return run(args)


def test_run_is_logged_with_params_metrics_and_data_version(table):
    result, _, run_id = _train(table)
    configure()
    r = mlflow.get_run(run_id)
    assert r.info.status == "FINISHED"
    assert r.data.params["horizon"] == "5" and r.data.params["seed"] == "42"
    assert r.data.params["embargo"] == "5"
    assert float(r.data.metrics["gbm.test.roc_auc"]) == pytest.approx(result.metrics["gbm"]["test"]["roc_auc"])
    assert r.data.tags["data.table_version"] == "0"
    assert r.data.tags["data.products"] == "BTC-USD,ETH-USD"


def test_logged_model_reloads_with_identical_predictions(table):
    result, _, run_id = _train(table)
    model, meta = load_model(run_id)
    assert meta["feature_columns"] == result.feature_columns
    assert meta["horizon_minutes"] == 5 and meta["products"] == ["BTC-USD", "ETH-USD"]
    X = np.random.default_rng(0).normal(size=(10, len(result.feature_columns)))
    import pandas as pd
    X = pd.DataFrame(X, columns=result.feature_columns)
    np.testing.assert_allclose(model.predict_proba(X), result.model.predict_proba(X))


def test_latest_run_is_most_recent(table):
    _, _, first = _train(table)
    _, _, second = _train(table, "--seed", "7")
    assert latest_run_id("test-exp") == second != first


def test_latest_run_requires_existing_runs():
    with pytest.raises(LookupError, match="no finished runs"):
        latest_run_id("does-not-exist")


def test_no_mlflow_flag_skips_tracking(table):
    _, _, run_id = _train(table, "--no-mlflow")
    assert run_id is None
