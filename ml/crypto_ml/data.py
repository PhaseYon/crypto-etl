"""Read gold/ohlcv_1m from the lake (local path or S3/MinIO) via delta-rs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable

from crypto_ml.config import normalize_uri, storage_options_for
from crypto_ml.schema import OHLCV_DELTA_TYPES, SchemaDriftError, validate_delta_types

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class OhlcvData:
    frame: pd.DataFrame
    table_uri: str
    table_version: int  # Delta version read, for reproducibility


def delta_column_types(table: DeltaTable) -> dict[str, str]:
    return {f.name: getattr(f.type, "type", repr(f.type)) for f in table.schema().fields}


def read_ohlcv(
    uri: str,
    products: list[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    version: int | None = None,
    storage_options: dict[str, str] | None = None,
) -> OhlcvData:
    """Read candles for `products` with start <= window_start < end (UTC), sorted by product and time.

    Raises SchemaDriftError if the table's schema no longer matches docs/CONTRACTS.md.
    """
    uri = normalize_uri(uri)
    if storage_options is None:
        storage_options = storage_options_for(uri)
    table = DeltaTable(uri, version=version, storage_options=storage_options)

    problems = validate_delta_types(delta_column_types(table))
    if problems:
        raise SchemaDriftError(f"{uri} does not match the gold/ohlcv_1m contract: " + "; ".join(problems))

    flt = None

    def _and(expr):
        nonlocal flt
        flt = expr if flt is None else flt & expr

    if products:
        _and(pc.field("product_id").isin(products))
    if start is not None:
        start = _utc(start)
        _and(pc.field("window_date") >= start.date())  # partition pruning
        _and(pc.field("window_start") >= _ts_scalar(start))
    if end is not None:
        end = _utc(end)
        _and(pc.field("window_date") <= end.date())
        _and(pc.field("window_start") < _ts_scalar(end))

    columns = list(OHLCV_DELTA_TYPES)
    arrow = table.to_pyarrow_dataset().to_table(columns=columns, filter=flt)
    frame = arrow.to_pandas()

    dupes = frame.duplicated(["product_id", "window_start"], keep=False)
    if dupes.any():
        # Contract says (product_id, window_start) is unique; keep the latest version of a row if not.
        log.warning("%d duplicate (product_id, window_start) rows in %s; keeping latest updated_at", dupes.sum(), uri)
        frame = frame.sort_values("updated_at").drop_duplicates(["product_id", "window_start"], keep="last")

    frame = frame.sort_values(["product_id", "window_start"]).reset_index(drop=True)
    return OhlcvData(frame=frame, table_uri=uri, table_version=table.version())


def _utc(ts: datetime) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def _ts_scalar(ts: pd.Timestamp) -> pa.Scalar:
    return pa.scalar(ts.to_pydatetime(), pa.timestamp("us", tz="UTC"))
