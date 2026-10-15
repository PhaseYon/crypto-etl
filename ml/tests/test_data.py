from datetime import UTC, date, datetime, timedelta

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import write_deltalake

from crypto_ml.config import StorageConfig, normalize_uri, storage_options_for
from crypto_ml.data import read_ohlcv
from crypto_ml.schema import OHLCV_ARROW_SCHEMA, SchemaDriftError, validate_delta_types

T0 = datetime(2026, 10, 1, 23, 58, tzinfo=UTC)


def _rows(product: str, minutes: int, start: datetime = T0) -> list[dict]:
    rows = []
    for i in range(minutes):
        ws = start + timedelta(minutes=i)
        rows.append({
            "product_id": product, "window_start": ws, "window_end": ws + timedelta(minutes=1),
            "open": 100.0 + i, "high": 101.0 + i, "low": 99.0 + i, "close": 100.5 + i,
            "volume": 2.0, "quote_volume": 201.0 + 2 * i, "vwap": 100.5 + i, "trade_count": 3,
            "buy_volume": 1.0, "sell_volume": 1.0, "updated_at": ws + timedelta(minutes=2),
            "window_date": ws.date(),
        })
    return rows


def _write(path, rows, schema=OHLCV_ARROW_SCHEMA, mode="overwrite"):
    write_deltalake(str(path), pa.Table.from_pylist(rows, schema=schema), mode=mode, partition_by=["window_date"])


def test_reads_filters_and_sorts(tmp_path):
    _write(tmp_path, _rows("ETH-USD", 5) + _rows("BTC-USD", 5))
    data = read_ohlcv(str(tmp_path), products=["BTC-USD"], start=T0 + timedelta(minutes=1),
                      end=T0 + timedelta(minutes=4))
    f = data.frame
    assert list(f["product_id"].unique()) == ["BTC-USD"]
    assert f["window_start"].min() == pd.Timestamp(T0 + timedelta(minutes=1))
    assert f["window_start"].max() == pd.Timestamp(T0 + timedelta(minutes=3))  # end is exclusive
    assert f["window_start"].is_monotonic_increasing
    assert data.table_version == 0


def test_time_filter_spans_partitions(tmp_path):
    # T0 is 23:58, so 5 minutes cross midnight into a second window_date partition.
    _write(tmp_path, _rows("BTC-USD", 5))
    f = read_ohlcv(str(tmp_path), start=T0 + timedelta(minutes=1)).frame
    assert len(f) == 4
    assert set(f["window_date"]) == {date(2026, 10, 1), date(2026, 10, 2)}


def test_naive_datetimes_are_treated_as_utc(tmp_path):
    _write(tmp_path, _rows("BTC-USD", 5))
    f = read_ohlcv(str(tmp_path), start=T0.replace(tzinfo=None) + timedelta(minutes=3)).frame
    assert len(f) == 2


def test_reads_specific_version(tmp_path):
    _write(tmp_path, _rows("BTC-USD", 2))
    _write(tmp_path, _rows("BTC-USD", 2, start=T0 + timedelta(minutes=2)), mode="append")
    assert len(read_ohlcv(str(tmp_path)).frame) == 4
    old = read_ohlcv(str(tmp_path), version=0)
    assert len(old.frame) == 2 and old.table_version == 0


def test_duplicate_keys_keep_latest(tmp_path):
    rows = _rows("BTC-USD", 1)
    newer = dict(rows[0], close=999.0, updated_at=rows[0]["updated_at"] + timedelta(minutes=5))
    _write(tmp_path, rows + [newer])
    f = read_ohlcv(str(tmp_path)).frame
    assert len(f) == 1 and f["close"].iloc[0] == 999.0


def test_schema_drift_fails_loudly(tmp_path):
    drifted = pa.schema([f if f.name != "trade_count" else pa.field("trade_count", pa.float64())
                         for f in OHLCV_ARROW_SCHEMA])
    _write(tmp_path, _rows("BTC-USD", 1), schema=drifted)
    with pytest.raises(SchemaDriftError, match="trade_count.*double, expected long"):
        read_ohlcv(str(tmp_path))


def test_validate_reports_missing_and_allows_extra():
    problems = validate_delta_types({"product_id": "string", "extra": "string"})
    assert "missing column 'close' (expected double)" in problems
    assert not any("extra" in p for p in problems)


def test_storage_options_for_minio():
    cfg = StorageConfig(s3_endpoint="http://minio:9000", access_key_id="k", secret_access_key="s")
    opts = storage_options_for("s3a://crypto-lake/gold/ohlcv_1m", cfg)
    assert opts["AWS_ENDPOINT_URL"] == "http://minio:9000"
    assert opts["AWS_ALLOW_HTTP"] == "true"
    assert opts["AWS_ACCESS_KEY_ID"] == "k"


def test_storage_options_for_aws_and_local():
    assert "AWS_ENDPOINT_URL" not in StorageConfig().storage_options()
    assert storage_options_for("/tmp/table") is None


def test_secret_not_in_repr():
    assert "s3cr3t" not in repr(StorageConfig(secret_access_key="s3cr3t"))


def test_uri_helpers():
    assert normalize_uri("s3a://b/x") == "s3://b/x"
    assert StorageConfig(bucket="b").table_uri("/gold/ohlcv_1m/") == "s3://b/gold/ohlcv_1m"
