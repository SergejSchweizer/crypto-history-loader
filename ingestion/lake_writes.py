"""Bronze parquet partition write helpers."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

import concurrent.futures
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from typing import Any, cast

from ingestion.lake_layout import PartitionKey, partition_path
from ingestion.lake_sidecars import write_bronze_sidecars

NaturalKey = tuple[str, str, str, str, datetime, str, str]
EmptyMinuteKey = tuple[str, str, str, str, datetime]


def record_natural_key(record: dict[str, object]) -> NaturalKey:
    """Build natural key for per-partition deduplication."""

    open_time = record["open_time"]
    if not isinstance(open_time, datetime):
        raise ValueError("open_time must be datetime")
    return (
        str(record["exchange"]),
        str(record["instrument_type"]),
        str(record["symbol"]),
        str(record["timeframe"]),
        open_time,
        str(record.get("trade_id", "")),
        str(record.get("instrument_name", "")),
    )


def merge_and_deduplicate_rows(
    existing: Sequence[Mapping[str, object]], new: Sequence[Mapping[str, object]]
) -> list[dict[str, object]]:
    """Merge old/new rows and keep latest version for duplicate keys."""

    merged: dict[NaturalKey, dict[str, object]] = {}
    for record in existing:
        normalized = dict(record)
        merged[record_natural_key(normalized)] = normalized
    for record in new:
        normalized = dict(record)
        merged[record_natural_key(normalized)] = normalized

    rows = list(merged.values())
    rows.sort(key=lambda item: cast(datetime, item["open_time"]))
    return rows


def empty_minute_record_key(record: dict[str, object]) -> EmptyMinuteKey:
    """Build natural key for confirmed-empty trade minutes."""

    minute = record["minute"]
    if not isinstance(minute, datetime):
        raise ValueError("minute must be datetime")
    return (
        str(record["exchange"]),
        str(record["instrument_type"]),
        str(record["symbol"]),
        str(record["timeframe"]),
        minute,
    )


def merge_empty_minute_rows(
    existing: Sequence[Mapping[str, object]], new: Sequence[Mapping[str, object]]
) -> list[dict[str, object]]:
    """Merge confirmed-empty minute rows by minute key."""

    merged: dict[EmptyMinuteKey, dict[str, object]] = {}
    for record in existing:
        normalized = dict(record)
        merged[empty_minute_record_key(normalized)] = normalized
    for record in new:
        normalized = dict(record)
        merged[empty_minute_record_key(normalized)] = normalized

    rows = list(merged.values())
    rows.sort(key=lambda item: cast(datetime, item["minute"]))
    return rows


def minute_starts_for_window(start_open_ms: int, end_open_ms: int) -> list[datetime]:
    """Return UTC minute starts covered by an inclusive millisecond window."""

    if end_open_ms < start_open_ms:
        return []
    minute_ms = 60_000
    start_minute_ms = start_open_ms - (start_open_ms % minute_ms)
    end_minute_ms = end_open_ms - (end_open_ms % minute_ms)
    return [
        datetime.fromtimestamp(value / 1000, tz=UTC) for value in range(start_minute_ms, end_minute_ms + 1, minute_ms)
    ]


def require_pyarrow() -> tuple[Any, Any]:
    """Load pyarrow modules required for parquet read/write operations."""

    try:
        pa = import_module("pyarrow")
        pq = import_module("pyarrow.parquet")
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc
    return pa, pq


def write_empty_trade_minutes(
    *,
    lake_root: str,
    dataset_type: str,
    exchange: str,
    instrument_type: str,
    symbol: str,
    timeframe: str,
    start_open_ms: int,
    end_open_ms: int,
    checked_at: datetime,
) -> list[str]:
    """Mark a successful zero-row trade fetch window as confirmed-empty minutes.

    Args:
        lake_root: Root directory for Bronze parquet partitions.
        dataset_type: Bronze trade dataset type, for example ``perps_trades`` or ``options_trades``.
        exchange: Exchange partition label.
        instrument_type: Instrument type partition label.
        symbol: Symbol partition label.
        timeframe: Timeframe partition label.
        start_open_ms: Inclusive checked window start.
        end_open_ms: Inclusive checked window end.
        checked_at: UTC timestamp when the zero-row response was observed.

    Returns:
        Paths of sidecar files written.
    """

    minutes = minute_starts_for_window(start_open_ms, end_open_ms)
    if not minutes:
        return []

    pa, pq = require_pyarrow()
    rows_by_date: dict[str, list[dict[str, object]]] = {}
    for minute in minutes:
        rows_by_date.setdefault(minute.date().isoformat(), []).append(
            {
                "dataset_type": dataset_type,
                "exchange": exchange,
                "instrument_type": instrument_type,
                "symbol": symbol,
                "timeframe": timeframe,
                "minute": minute,
                "status": "confirmed_empty",
                "checked_at": checked_at,
                "request_start_ms": start_open_ms,
                "request_end_ms": end_open_ms,
                "row_count": 0,
            }
        )

    written: list[str] = []
    for date_partition, rows in rows_by_date.items():
        key: PartitionKey = (exchange, instrument_type, symbol, timeframe, date_partition)
        part_dir = partition_path(lake_root=lake_root, dataset_type=dataset_type, key=key)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "empty_minutes.parquet"
        staging_path = part_dir / (
            f".empty-minutes-{int(checked_at.timestamp() * 1000)}-{start_open_ms}-{end_open_ms}.parquet"
        )
        existing_rows: list[dict[str, object]] = []
        if file_path.exists():
            existing_rows = pq.ParquetFile(file_path).read().to_pylist()
        merged_rows = merge_empty_minute_rows(existing=existing_rows, new=rows)
        pq.write_table(pa.Table.from_pylist(merged_rows), staging_path)
        staging_path.replace(file_path)
        written.append(str(file_path.resolve()))
    return sorted(written)


def write_partition_file(
    *,
    pa: Any,
    pq: Any,
    lake_root: str,
    dataset_type: str,
    run_id: str,
    key: PartitionKey,
    rows: list[dict[str, object]],
) -> str:
    """Write one partition file via staging replace after merge/dedup."""

    part_dir = partition_path(lake_root=lake_root, dataset_type=dataset_type, key=key)
    part_dir.mkdir(parents=True, exist_ok=True)
    file_path = part_dir / "data.parquet"
    staging_path = part_dir / f".staging-{run_id}.parquet"

    existing_rows: list[dict[str, object]] = []
    if file_path.exists():
        existing_table = pq.ParquetFile(file_path).read()
        existing_rows = existing_table.to_pylist()

    merged_rows = merge_and_deduplicate_rows(existing=existing_rows, new=rows)
    table = pa.Table.from_pylist(merged_rows)
    pq.write_table(table, staging_path)
    staging_path.replace(file_path)
    return str(file_path.resolve())


def write_grouped_rows(
    *,
    pa: Any,
    pq: Any,
    lake_root: str,
    dataset_type: str,
    run_id: str,
    grouped: dict[PartitionKey, list[dict[str, object]]],
) -> list[str]:
    """Write grouped partition rows concurrently and return written file paths."""

    written_files: list[str] = []
    if grouped:
        max_workers = min(4, len(grouped))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    write_partition_file,
                    pa=pa,
                    pq=pq,
                    lake_root=lake_root,
                    dataset_type=dataset_type,
                    run_id=run_id,
                    key=key,
                    rows=rows,
                )
                for key, rows in grouped.items()
            ]
            for future in concurrent.futures.as_completed(futures):
                written_files.append(future.result())
        # Render sidecars on the main thread: matplotlib backends are not reliably thread-safe.
        key_by_path: dict[str, PartitionKey] = {
            str(
                (partition_path(lake_root=lake_root, dataset_type=dataset_type, key=key) / "data.parquet").resolve()
            ): key
            for key in grouped
        }
        for file_str in sorted(written_files):
            file_path = Path(file_str)
            key = key_by_path.get(file_str)
            if key is None:
                continue
            table = pq.ParquetFile(file_path).read()
            write_bronze_sidecars(
                file_path=file_path,
                dataset_type=dataset_type,
                key=key,
                rows=table.to_pylist(),
            )
    return sorted(written_files)
