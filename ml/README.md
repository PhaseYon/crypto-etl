# ml: features, training and batch inference on the lake

Reads `gold/ohlcv_1m` (see `docs/CONTRACTS.md`), builds features and labels, trains a
direction or volatility model against baselines, tracks everything in MLflow, and writes predictions
back to the lake.

```
gold/ohlcv_1m ──► regular minute grid ──► causal features ──► labels (t+h) ──► time split + embargo
                                                                                     │
           ml/predictions ◄── predict ◄── MLflow (params, metrics, data version, model) ◄── train
```

| Module | Purpose |
|---|---|
| `crypto_ml/data.py` | Read gold via delta-rs (local path or S3/MinIO), validate the contract schema, filter by product/time |
| `crypto_ml/synthetic.py` | Contract-conformant synthetic candles for tests and demos |
| `crypto_ml/features.py` | Forward-filled minute grid and strictly causal features |
| `crypto_ml/dataset.py` | Direction and volatility labels, dataset, train/val/test split and walk-forward CV with an embargo |
| `crypto_ml/models.py` | Baselines, gradient-boosted and logistic models, metrics |
| `crypto_ml/training.py` | Train candidates + baselines on one split, select on validation, walk-forward evaluation |
| `crypto_ml/tracking.py` | MLflow logging, finding and loading models |
| `crypto_ml/train.py`, `predict.py` | CLIs |

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r ml/requirements-dev.txt
```

Tested with Python 3.14.4 on the host and in the image (`python:3.14.4-slim`).

Configuration comes from the environment (`.env.example` at the repo root): `LAKE_BUCKET`,
`S3_ENDPOINT` (empty = real AWS S3), `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`MLFLOW_TRACKING_URI` (default `sqlite:///mlruns/mlflow.db`), `MLFLOW_ARTIFACT_ROOT`
(default `mlruns/artifacts`, or an `s3://` URI). Every CLI also accepts `--table-uri` /
`--output-uri`, which can be local paths.

## Train

```bash
cd ml
python -m crypto_ml.train --products BTC-USD,ETH-USD --start 2026-10-01 --end 2026-10-08 --horizon 5
```

Prints model vs baseline metrics on validation and test, and logs a run to MLflow. Options:
`--target` (`direction` or `volatility`, below), `--walk-forward N` (also report mean metrics over
N expanding-window folds), `--embargo` (minutes, default = horizon), `--train-frac`, `--val-frac`,
`--seed`, `--table-version` (train on a fixed Delta version), `--experiment` (default: one per
target, `crypto-direction` or `crypto-volatility`), `--no-mlflow`.

### Targets

- `direction` (default): will the close be higher `horizon` minutes from now? Candidate: the
  gradient-boosted model. Baselines: `majority`, `momentum`.
- `volatility`: will realized volatility over the next `horizon` minutes exceed its trailing-day
  median (the threshold known now)? Volatility clusters, so this is far more predictable than
  direction. Candidates: the gradient-boosted model on every feature plus `vol_regime` (current
  volatility vs that median), and a logistic regression on `vol_regime` alone. Baselines:
  `majority`, and `persistence` ("the regime stays as it is now").

The candidate with the lowest validation log loss is selected and logged as the run's model;
test is still touched once, for reporting.

```bash
python -m crypto_ml.train --products BTC-USD,ETH-USD,SOL-USD --target volatility --horizon 60 --walk-forward 5
```

`crypto_ml.predict` only accepts direction models for now: `ml/predictions` and the dashboard
describe P(up), so a volatility model is refused rather than written under the wrong meaning.

Try it without a lake by writing synthetic data to a local table:

```bash
python -m crypto_ml.synthetic --uri /tmp/gold --days 3
python -m crypto_ml.train --products BTC-USD,ETH-USD --table-uri /tmp/gold
```

## Predict

```bash
python -m crypto_ml.predict                                   # latest model, now
python -m crypto_ml.predict --run-id <id> --score-minutes 30 --as-of 2026-10-08T12:00:00Z
```

Scores the most recent minutes of each product the model was trained on and MERGEs into
`s3://$LAKE_BUCKET/ml/predictions`. If gold has no candle within `--max-staleness-minutes`
(default 15) of `--as-of`, it exits with `StaleDataError` instead of scoring old data.

## Docker

```bash
docker compose --profile ml build
docker compose --profile ml run --rm ml-train      # ML_PRODUCTS, ML_TARGET, ML_HORIZON, ML_WALK_FORWARD override the defaults
docker compose --profile ml run --rm ml-predict    # ML_SCORE_MINUTES
```

Both jobs share the `ml-data` volume, which holds the MLflow database and model artifacts,
so `ml-predict` finds the model `ml-train` logged. Schedule `ml-predict` (cron, Airflow, ...)
for continuous scoring.

## Results in MLflow

```bash
mlflow ui --backend-store-uri sqlite:///mlruns/mlflow.db     # from the directory you trained in
```

Each run has:
- params: target, horizon, seed, split fractions, embargo, chosen boosting iterations, all model hyperparameters
- metrics: `<model>.<split>.<metric>` for every candidate and baseline on `val` and `test`
  (`accuracy`, `roc_auc`, `log_loss`, `brier`, `ic`, `positive_rate`, `n`), and with
  `--walk-forward`, `wf.<model>.<metric>` means over the folds plus `wf.<model>.roc_auc_min`
- tags: `target`, `selected_model`, `data.table_uri`, `data.table_version` (the exact Delta version
  read), products, time range
- artifacts: the model, `model_meta.json` (feature columns, horizon, products, lookback), `splits.json`

MLflow 3.x refuses the plain file store, so tracking uses SQLite. For a shared team server, run
`mlflow server --backend-store-uri postgresql://... --artifacts-destination s3://<bucket>/mlflow`
and point `MLFLOW_TRACKING_URI` at it; no code changes needed.

## Leakage safeguards

- **Causal features.** Only backward shifts and trailing windows. `test_features_are_causal`
  perturbs and deletes future candles and asserts no past feature changes.
- **Gaps are explicit.** Minutes without trades are forward-filled on a regular grid with
  `is_filled = 1`, so rolling windows always span real minutes.
- **Labels look exactly `horizon` minutes ahead** within one product; rows whose label window
  runs past the data are dropped.
- **Time-based splits only, with an embargo** (default = horizon, smaller is rejected) between
  train, validation and test, so no training label window overlaps later data.
- **No random validation split.** sklearn's built-in early stopping is disabled because it splits
  randomly; the iteration count is chosen on the time-based validation split, and the model is
  refit on train only.
- **Test is touched once**, for reporting.

## Read the metrics honestly

Short-horizon crypto direction is close to unpredictable, and most apparent edges are noise or
leakage. Judge the model against the baselines, not against 50%:

- `majority`: always predicts the training up-rate. Beating its log loss is the minimum bar.
- `momentum`: "the last 5 minutes' direction continues".
- `roc_auc` near 0.5 and `ic` near 0 mean no ranking skill. On synthetic random-walk data the
  model must show no real edge, and `test_random_walk_has_no_real_edge` checks that.

A single split is one sample; confirm any edge with `walk_forward_splits` over a longer history
before trusting it.

## Tests

```bash
pytest ml                    # unit tests: synthetic data, local Delta tables, temporary MLflow store
```

Integration tests run end to end against a real S3-compatible store in a throwaway bucket. They
are skipped unless the store is configured:

```bash
docker run -d --name cetl-ml-s3 -p 29000:9000 \
  -e RUSTFS_ACCESS_KEY=<access-key> -e RUSTFS_SECRET_KEY=<secret-key> rustfs/rustfs:1.0.1
ML_IT_S3_ENDPOINT=http://localhost:29000 ML_IT_ACCESS_KEY=<access-key> ML_IT_SECRET_KEY=<secret-key> \
  pytest ml -m integration
docker rm -f cetl-ml-s3
```

## Output table `ml/predictions` (proposed addition to docs/CONTRACTS.md)

| Column | Type | Notes |
|---|---|---|
| `product_id` | string | |
| `window_start` | timestamp | minute the features describe (UTC) |
| `horizon_minutes` | long | the prediction is for `window_start + horizon` |
| `model_run_id` | string | MLflow run id |
| `p_up` | double | P(forward log return > 0) |
| `predicted_label` | long | `p_up > 0.5` |
| `is_filled` | long | 1 if that minute had no trades (features from carried-forward prices) |
| `created_at` | timestamp | |
| `prediction_date` | date | partition column |

Unique key: `(product_id, window_start, model_run_id)`, enforced by MERGE.
