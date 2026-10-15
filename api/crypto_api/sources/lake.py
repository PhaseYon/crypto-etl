"""Delta Lake reads via delta-rs. Prefers Delta log metadata and file stats over scanning data."""

from datetime import UTC, date, datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pds
from deltalake import DeltaTable, QueryBuilder
from deltalake.exceptions import TableNotFoundError

from .base import (
    TABLE_SPECS,
    BackfillCounts,
    CandleRow,
    GapRow,
    QuarantineData,
    QuarantineRow,
    TableMissingError,
    TableSnapshot,
    TableSpec,
)

VALUE_PREVIEW_CHARS = 300
# Rows of one data-quality scan share (roughly) one detected_at; see "Assumptions" in ui/README.md
GAP_SCAN_WINDOW_SECONDS = 60


class DeltaLakeSource:
    def __init__(self, lake_root: str, storage_options: dict[str, str]):
        self._root = lake_root
        self._storage_options = storage_options

    def _open(self, path: str) -> DeltaTable:
        try:
            return DeltaTable(f"{self._root}/{path}", storage_options=self._storage_options or None)
        except TableNotFoundError as e:
            raise TableMissingError(path) from e

    def table_snapshot(self, spec: TableSpec) -> TableSnapshot:
        dt = self._open(spec.path)
        history = dt.history(1)
        last_commit_at = (
            datetime.fromtimestamp(history[0]["timestamp"] / 1000, tz=UTC)
            if history and "timestamp" in history[0]
            else None
        )
        actions = pa.table(dt.get_add_actions(flatten=True))
        row_count = _sum_column(actions, "num_records")
        newest_at = _max_column(actions, f"max.{spec.freshness_column}")
        return TableSnapshot(dt.version(), last_commit_at, row_count, _as_utc(newest_at))

    def bronze_max_offsets(self) -> dict[int, int]:
        dt = self._open(TABLE_SPECS["bronze"].path)
        actions = pa.table(dt.get_add_actions(flatten=True)).to_pylist()
        result: dict[int, int] = {}
        mixed_dates: list[date] = []
        for action in actions:
            low, high = action.get("min.kafka_partition"), action.get("max.kafka_partition")
            max_offset = action.get("max.kafka_offset")
            if low is not None and low == high and max_offset is not None:
                # Spark writes one file per Kafka partition per batch, so stats are usually enough
                result[low] = max(result.get(low, -1), max_offset)
            else:
                mixed_dates.append(action.get("partition.ingest_date") or date.min)
        if mixed_dates:
            # Files spanning several partitions (or lacking stats): aggregate just those dates
            since = min(mixed_dates)
            where = "" if since == date.min else f"WHERE ingest_date >= DATE '{since.isoformat()}'"
            sql = (
                f"SELECT kafka_partition, MAX(kafka_offset) AS max_offset FROM bronze {where} GROUP BY kafka_partition"
            )
            table = pa.table(QueryBuilder().register("bronze", dt).execute(sql).read_all())
            for row in table.to_pylist():
                partition, offset = row["kafka_partition"], row["max_offset"]
                if partition is not None and offset is not None:
                    result[partition] = max(result.get(partition, -1), offset)
        return result

    def ohlcv(self, product_id: str, since: datetime) -> list[CandleRow]:
        dt = self._open(TABLE_SPECS["gold"].path)
        expression = (
            (pds.field("window_date") >= since.date())
            & (pds.field("product_id") == product_id)
            & (pds.field("window_start") >= pa.scalar(since, pa.timestamp("us", tz="UTC")))
        )
        columns = ["window_start", "open", "high", "low", "close", "volume", "vwap", "trade_count"]
        table = dt.to_pyarrow_table(columns=columns, filters=expression).sort_by("window_start")
        return [
            CandleRow(
                window_start=_as_utc(row["window_start"]),
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"] or 0.0,
                vwap=row["vwap"],
                trade_count=row["trade_count"] or 0,
            )
            for row in table.to_pylist()
        ]

    def quarantine(self, recent_limit: int) -> QuarantineData:
        dt = self._open(TABLE_SPECS["quarantine"].path)
        table = dt.to_pyarrow_table(columns=["error", "quarantined_at", "kafka_partition", "kafka_offset", "value"])
        reasons = {
            row["values"]: row["counts"]
            for row in pc.value_counts(table["error"]).to_pylist()
            if row["values"] is not None
        }
        recent = table.sort_by([("quarantined_at", "descending")]).slice(0, recent_limit).to_pylist()
        return QuarantineData(
            total=table.num_rows,
            reasons=reasons,
            recent=[
                QuarantineRow(
                    error=row["error"] or "",
                    quarantined_at=_as_utc(row["quarantined_at"]),
                    kafka_partition=row["kafka_partition"],
                    kafka_offset=row["kafka_offset"],
                    value=(row["value"] or "")[:VALUE_PREVIEW_CHARS],
                )
                for row in recent
            ],
        )

    def latest_gaps(self) -> list[GapRow]:
        dt = self._open(TABLE_SPECS["trade_gaps"].path)
        rows = dt.to_pyarrow_table().to_pylist()
        detected = [_as_utc(r.get("detected_at")) for r in rows if r.get("detected_at") is not None]
        if detected:
            newest = max(detected)
            rows = [
                r
                for r in rows
                if r.get("detected_at") is not None
                and (newest - _as_utc(r["detected_at"])).total_seconds() <= GAP_SCAN_WINDOW_SECONDS
            ]
        gaps = [
            GapRow(
                product_id=r["product_id"],
                gap_start_id=r["gap_start_id"],
                gap_end_id=r["gap_end_id"],
                missing_count=r["missing_count"],
                detected_at=_as_utc(r.get("detected_at")),
            )
            for r in rows
        ]
        return sorted(gaps, key=lambda g: (g.product_id, g.gap_start_id))

    def backfill_counts(self, since: date) -> BackfillCounts:
        dt = self._open(TABLE_SPECS["silver"].path)
        table = dt.to_pyarrow_table(columns=["source"], filters=pds.field("trade_date") >= since)
        counts = {row["values"]: row["counts"] for row in pc.value_counts(table["source"]).to_pylist()}
        return BackfillCounts(total=table.num_rows, backfilled=counts.get("rest_backfill", 0))

    def predictions(self, limit: int) -> list[dict[str, Any]]:
        dt = self._open(TABLE_SPECS["predictions"].path)
        table = dt.to_pyarrow_table()
        for column in ("created_at", "window_start"):
            if column in table.column_names:
                table = table.sort_by([(column, "descending")])
                break
        return table.slice(0, limit).to_pylist()


def _sum_column(table: pa.Table, name: str) -> int | None:
    if name not in table.column_names or table.num_rows == 0:
        return 0 if table.num_rows == 0 else None
    column = table[name]
    if column.null_count:
        return None  # some files lack stats; an undercount would be misleading
    return int(pc.sum(column).as_py() or 0)


def _max_column(table: pa.Table, name: str) -> Any:
    if name not in table.column_names or table.num_rows == 0:
        return None
    return pc.max(table[name]).as_py()


def _as_utc(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return value
