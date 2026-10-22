from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
from deltalake import DeltaTable

from crypto_ml.predict import StaleDataError, main
from crypto_ml.synthetic import generate_ohlcv, write_ohlcv_delta
from crypto_ml.train import build_parser, run

T0 = datetime(2026, 1, 1, tzinfo=UTC)
MINUTES = 2500
END = T0 + timedelta(minutes=MINUTES)


@pytest.fixture
def trained(tmp_path):
    gold = str(tmp_path / "gold")
    write_ohlcv_delta(generate_ohlcv(["BTC-USD", "ETH-USD"], T0, MINUTES, seed=2, trades_per_minute=20), gold)
    args = build_parser().parse_args(["--products", "BTC-USD,ETH-USD", "--horizon", "5", "--table-uri", gold])
    _, _, run_id = run(args)
    return gold, str(tmp_path / "predictions"), run_id


def _predict(gold, out, *extra):
    return main(["--table-uri", gold, "--output-uri", out, "--as-of", END.isoformat(), *extra])


def test_scores_latest_minutes_and_writes_delta(trained):
    gold, out, run_id = trained
    preds = _predict(gold, out, "--score-minutes", "3")
    assert len(preds) == 6  # 2 products x 3 minutes
    assert preds["p_up"].between(0, 1).all()
    assert set(preds["model_run_id"]) == {run_id}  # latest run used by default
    assert preds.groupby("product_id")["window_start"].max().max() >= pd.Timestamp(END) - pd.Timedelta(minutes=2)
    table = DeltaTable(out).to_pandas()
    assert len(table) == 6 and set(table["horizon_minutes"]) == {5}


def test_rerun_is_idempotent(trained):
    gold, out, run_id = trained
    _predict(gold, out, "--score-minutes", "3")
    _predict(gold, out, "--score-minutes", "3", "--run-id", run_id)
    table = DeltaTable(out).to_pandas()
    assert len(table) == 6
    assert not table.duplicated(["product_id", "window_start", "model_run_id"]).any()


def test_new_minutes_are_appended(trained):
    gold, out, _ = trained
    _predict(gold, out, "--score-minutes", "2")
    main(
        [
            "--table-uri",
            gold,
            "--output-uri",
            out,
            "--as-of",
            (END - timedelta(minutes=10)).isoformat(),
            "--score-minutes",
            "2",
        ]
    )
    assert len(DeltaTable(out).to_pandas()) == 8


def test_product_subset(trained):
    gold, out, _ = trained
    preds = _predict(gold, out, "--products", "ETH-USD")
    assert set(preds["product_id"]) == {"ETH-USD"}


def test_stale_gold_fails_loudly(trained):
    gold, out, _ = trained
    with pytest.raises(StaleDataError, match="no candles newer"):
        main(["--table-uri", gold, "--output-uri", out, "--as-of", (END + timedelta(hours=2)).isoformat()])


def test_volatility_models_are_refused(tmp_path):
    from tests.test_training import regime_ohlcv

    gold = str(tmp_path / "gold")
    ohlcv = regime_ohlcv(("BTC-USD",), segments=16)
    write_ohlcv_delta(ohlcv, gold)
    args = ["--products", "BTC-USD", "--horizon", "15", "--target", "volatility", "--table-uri", gold]
    _, _, run_id = run(build_parser().parse_args(args))
    as_of = ohlcv["window_start"].max().isoformat()
    with pytest.raises(ValueError, match="volatility model"):
        main(["--run-id", run_id, "--table-uri", gold, "--output-uri", str(tmp_path / "p"), "--as-of", as_of])
