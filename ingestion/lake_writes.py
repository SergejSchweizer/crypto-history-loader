"""Bronze parquet partition write helpers."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

import concurrent.futures
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from ingestion.lake_layout import PartitionKey, partition_path
from ingestion.lake_sidecars import write_bronze_sidecars

NaturalKey = tuple[str, str, str, str, datetime, str, str]


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


def require_pyarrow() -> tuple[Any, Any]:
    """Load pyarrow modules required for parquet read/write operations."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc
    return pa, pq


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
