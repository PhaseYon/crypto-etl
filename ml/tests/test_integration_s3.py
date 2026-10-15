"""End to end against a real S3-compatible store (MinIO / RustFS): gold -> train -> MLflow -> predict.

Needs a running store and these env vars (see ml/README.md):
    ML_IT_S3_ENDPOINT (e.g. http://localhost:29000), ML_IT_ACCESS_KEY, ML_IT_SECRET_KEY
Run with: pytest ml -m integration
"""

import os
import socket
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import pytest
from deltalake import DeltaTable
from pyarrow import fs

from crypto_ml import predict, train
from crypto_ml.config import StorageConfig
from crypto_ml.data import read_ohlcv
from crypto_ml.synthetic import generate_ohlcv, write_ohlcv_delta

pytestmark = pytest.mark.integration

T0 = datetime(2026, 1, 1, tzinfo=UTC)
MINUTES = 2500


@pytest.fixture
def lake(monkeypatch):
    endpoint = os.getenv("ML_IT_S3_ENDPOINT")
    key, secret = os.getenv("ML_IT_ACCESS_KEY"), os.getenv("ML_IT_SECRET_KEY")
    if not (endpoint and key and secret):
        pytest.skip("set ML_IT_S3_ENDPOINT, ML_IT_ACCESS_KEY and ML_IT_SECRET_KEY to run S3 integration tests")
    url = urlparse(endpoint)
    try:
        socket.create_connection((url.hostname, url.port or 80), timeout=2).close()
    except OSError:
        pytest.fail(f"S3 store not reachable at {endpoint}")

    bucket = f"cetl-ml-it-{uuid.uuid4().hex[:8]}"
    s3 = fs.S3FileSystem(
        access_key=key,
        secret_key=secret,
        endpoint_override=url.netloc,
        scheme=url.scheme,
        region="us-east-1",
        allow_bucket_creation=True,
        allow_bucket_deletion=True,
    )
    s3.create_dir(bucket)
    for name, value in {
        "LAKE_BUCKET": bucket,
        "S3_ENDPOINT": endpoint,
        "AWS_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": key,
        "AWS_SECRET_ACCESS_KEY": secret,
    }.items():
        monkeypatch.setenv(name, value)
    yield StorageConfig.from_env()
    s3.delete_dir(bucket)


def test_gold_to_predictions_on_s3(lake):
    gold_uri = lake.table_uri("gold/ohlcv_1m")
    write_ohlcv_delta(generate_ohlcv(["BTC-USD", "ETH-USD"], T0, MINUTES, seed=3, trades_per_minute=20), gold_uri)
    assert read_ohlcv(gold_uri).table_version == 0

    # Defaults resolve every location from the environment, as in the container.
    result = train.main(["--products", "BTC-USD,ETH-USD", "--horizon", "5"])
    assert result.split_sizes["test"] > 0

    as_of = (T0 + timedelta(minutes=MINUTES)).isoformat()
    preds = predict.main(["--as-of", as_of, "--score-minutes", "5"])
    assert len(preds) == 10

    out = DeltaTable(lake.table_uri("ml/predictions"), storage_options=lake.storage_options()).to_pandas()
    assert len(out) == 10 and out["p_up"].between(0, 1).all()

    predict.main(["--as-of", as_of, "--score-minutes", "5"])  # idempotent rerun
    out = DeltaTable(lake.table_uri("ml/predictions"), storage_options=lake.storage_options()).to_pandas()
    assert len(out) == 10


def test_s3a_uris_are_accepted(lake):
    write_ohlcv_delta(generate_ohlcv(["BTC-USD"], T0, 100, seed=1), f"s3a://{lake.bucket}/gold/ohlcv_1m")
    assert len(read_ohlcv(f"s3a://{lake.bucket}/gold/ohlcv_1m").frame) > 0
