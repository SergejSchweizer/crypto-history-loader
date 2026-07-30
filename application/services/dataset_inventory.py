"""Read-only inventory reporting for Bronze, Silver, and Gold lake datasets."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal

from application.dataset_contracts import (
    BRONZE_TO_SILVER_DATASETS,
    GOLD_DATASET_CONTRACTS,
    SILVER_DATASET_CONTRACTS,
    supported_gold_dataset_ids,
)

LayerName = Literal["bronze", "silver", "gold"]

_TIMESTAMP_CANDIDATES: tuple[str, ...] = (
    "timestamp_m1",
    "timestamp",
    "open_time",
    "trade_time",
    "funding_time",
    "snapshot_date",
    "ts_minute",
    "event_time",
)
_SERIES_CANDIDATES: tuple[str, ...] = ("symbol", "index_name", "currency", "instrument_name")


@dataclass(frozen=True)
class DatasetInventoryRow:
    """One deterministic inventory row for a physical or contracted dataset."""

    layer: LayerName
    dataset: str
    state: str
    origin_repository: str
    physical_dataset: str | None
    timestamp_column: str | None
    schema_columns: tuple[str, ...]
    file_count: int
    row_count: int
    series_count: int
    start_date: str | None
    end_date: str | None
    expected_days: int | None
    observed_days: int | None
    missing_days: int | None
    per_series_missing_days: tuple[str, ...]
    source_hash: str | None
    builder_commit: str
    quality_counters: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""

        payload = asdict(self)
        payload["schema_columns"] = list(self.schema_columns)
        payload["per_series_missing_days"] = list(self.per_series_missing_days)
        return payload


def build_dataset_inventory(
    *,
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
    builder_commit: str | None = None,
) -> list[DatasetInventoryRow]:
    """Return deterministic inventory rows for local lake roots.

    Args:
        bronze_root: Bronze parquet lake root.
        silver_root: Silver parquet lake root.
        gold_root: Gold parquet lake root.
        builder_commit: Commit identifier for the code that generated the report.

    Returns:
        Sorted inventory rows for physical Bronze data, contracted Silver outputs, and contracted Gold outputs.

    Side Effects:
        None. The function only reads parquet files and directory names.
    """

    physical_bronze = _physical_dataset_files(bronze_root, "dataset_type")
    physical_silver = _physical_dataset_files(silver_root, "dataset_type")
    physical_gold = _physical_dataset_files(gold_root, "dataset_id")
    commit = builder_commit or "unknown"

    rows: list[DatasetInventoryRow] = []
    for dataset in sorted(physical_bronze):
        state = "mapped" if dataset in BRONZE_TO_SILVER_DATASETS else "unmapped"
        rows.append(
            _inventory_row(
                layer="bronze",
                dataset=dataset,
                state=state,
                origin_repository=_origin_for_dataset(dataset),
                physical_dataset=dataset,
                files=physical_bronze[dataset],
                timestamp_hint=None,
                builder_commit=commit,
            )
        )

    for dataset, silver_contract in sorted(SILVER_DATASET_CONTRACTS.items()):
        physical_dataset = dataset
        files = physical_silver.get(dataset, [])
        state = "materialized" if files else "not_materialized"
        if dataset == "perps_ohlcv" and not files and "perp" in physical_silver:
            # Older Silver builds wrote perpetual OHLCV under dataset_type=perp.
            # Keep that compatibility visible until PR-20 materializes the canonical name.
            physical_dataset = "perp"
            files = physical_silver["perp"]
            state = "legacy_artifact"
        rows.append(
            _inventory_row(
                layer="silver",
                dataset=dataset,
                state=state,
                origin_repository=_origin_for_dataset(dataset),
                physical_dataset=physical_dataset if files else None,
                files=files,
                timestamp_hint=silver_contract.timestamp_column,
                fallback_columns=silver_contract.output_columns,
                builder_commit=commit,
            )
        )

    supported_gold = set(supported_gold_dataset_ids())
    for dataset, gold_contract in sorted(GOLD_DATASET_CONTRACTS.items()):
        if dataset not in supported_gold:
            continue
        files = physical_gold.get(dataset, [])
        rows.append(
            _inventory_row(
                layer="gold",
                dataset=dataset,
                state="materialized" if files else "not_materialized",
                origin_repository=_origin_for_dataset(dataset),
                physical_dataset=dataset if files else None,
                files=files,
                timestamp_hint=gold_contract.timestamp_column,
                builder_commit=commit,
            )
        )

    return sorted(rows, key=lambda row: (row.layer, row.dataset))


def inventory_to_json(rows: list[DatasetInventoryRow]) -> str:
    """Render inventory rows as stable JSON."""

    return json.dumps([row.to_dict() for row in rows], indent=2, sort_keys=True) + "\n"


def inventory_to_markdown(rows: list[DatasetInventoryRow]) -> str:
    """Render inventory rows as deterministic Markdown."""

    lines = [
        "# Dataset Inventory",
        "",
        "| Layer | Dataset | State | Origin | Physical Dataset | Files | Rows | Series | Start | End "
        "| Expected Days | Observed Days | Missing Days | Timestamp | Source Hash | Builder Commit "
        "| Quality Counters | Columns | Per-Series Missing |",
        "|---|---|---|---|---|---:|---:|---:|---|---|---:|---:|---:|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    row.layer,
                    f"`{row.dataset}`",
                    row.state,
                    row.origin_repository,
                    f"`{row.physical_dataset}`" if row.physical_dataset else "n/a",
                    str(row.file_count),
                    str(row.row_count),
                    str(row.series_count),
                    row.start_date or "n/a",
                    row.end_date or "n/a",
                    str(row.expected_days) if row.expected_days is not None else "n/a",
                    str(row.observed_days) if row.observed_days is not None else "n/a",
                    str(row.missing_days) if row.missing_days is not None else "n/a",
                    f"`{row.timestamp_column}`" if row.timestamp_column else "n/a",
                    f"`{row.source_hash}`" if row.source_hash else "n/a",
                    f"`{row.builder_commit}`",
                    "`" + json.dumps(row.quality_counters, sort_keys=True, separators=(",", ":")) + "`",
                    ", ".join(f"`{column}`" for column in row.schema_columns) or "n/a",
                    "; ".join(row.per_series_missing_days) or "n/a",
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _physical_dataset_files(root: Path, partition_key: str) -> dict[str, list[Path]]:
    if not root.exists():
        return {}
    grouped: dict[str, list[Path]] = defaultdict(list)
    for parquet_path in sorted(root.rglob("*.parquet")):
        dataset = _partition_value(parquet_path, partition_key)
        if dataset is None:
            continue
        grouped[dataset].append(parquet_path)
    return {dataset: files for dataset, files in sorted(grouped.items())}


def _inventory_row(
    *,
    layer: LayerName,
    dataset: str,
    state: str,
    origin_repository: str,
    physical_dataset: str | None,
    files: list[Path],
    timestamp_hint: str | None,
    fallback_columns: tuple[str, ...] = (),
    builder_commit: str = "unknown",
) -> DatasetInventoryRow:
    if not files:
        return DatasetInventoryRow(
            layer=layer,
            dataset=dataset,
            state=state,
            origin_repository=origin_repository,
            physical_dataset=physical_dataset,
            timestamp_column=timestamp_hint,
            schema_columns=tuple(fallback_columns),
            file_count=0,
            row_count=0,
            series_count=0,
            start_date=None,
            end_date=None,
            expected_days=None,
            observed_days=None,
            missing_days=None,
            per_series_missing_days=(),
            source_hash=None,
            builder_commit=builder_commit,
            quality_counters=_quality_counters(
                file_count=0,
                row_count=0,
                series_count=0,
                expected_days=None,
                observed_days=None,
                missing_days=None,
            ),
        )

    pl = _require_polars()
    schema_columns = _schema_columns(pl, files[0])
    timestamp_column = _timestamp_column(schema_columns, timestamp_hint)
    row_count = _row_count(pl, files)
    coverage = _coverage(pl, files, timestamp_column, schema_columns)
    return DatasetInventoryRow(
        layer=layer,
        dataset=dataset,
        state=state,
        origin_repository=origin_repository,
        physical_dataset=physical_dataset,
        timestamp_column=timestamp_column,
        schema_columns=schema_columns,
        file_count=len(files),
        row_count=row_count,
        series_count=coverage["series_count"],
        start_date=coverage["start_date"],
        end_date=coverage["end_date"],
        expected_days=coverage["expected_days"],
        observed_days=coverage["observed_days"],
        missing_days=coverage["missing_days"],
        per_series_missing_days=coverage["per_series_missing_days"],
        source_hash=_source_hash(files),
        builder_commit=builder_commit,
        quality_counters=_quality_counters(
            file_count=len(files),
            row_count=row_count,
            series_count=coverage["series_count"],
            expected_days=coverage["expected_days"],
            observed_days=coverage["observed_days"],
            missing_days=coverage["missing_days"],
        ),
    )


def _quality_counters(
    *,
    file_count: int,
    row_count: int,
    series_count: int,
    expected_days: int | None,
    observed_days: int | None,
    missing_days: int | None,
) -> dict[str, int]:
    return {
        "file_count": file_count,
        "row_count": row_count,
        "series_count": series_count,
        "expected_days": expected_days or 0,
        "observed_days": observed_days or 0,
        "missing_days": missing_days or 0,
    }


def _source_hash(files: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(files, key=_stable_file_identity):
        digest.update(_stable_file_identity(path).encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def _stable_file_identity(path: Path) -> str:
    parts = path.parts
    for index, part in enumerate(parts):
        if part.startswith(("dataset_type=", "dataset_id=")):
            return "/".join(parts[index:])
    return path.name


def _schema_columns(pl: Any, parquet_path: Path) -> tuple[str, ...]:
    return tuple(pl.read_parquet(str(parquet_path), n_rows=0).columns)


def _row_count(pl: Any, files: list[Path]) -> int:
    frame = _scan_inventory_parquet(pl, files).select(pl.len().alias("row_count")).collect()
    return int(frame.item())


def _coverage(
    pl: Any,
    files: list[Path],
    timestamp_column: str | None,
    schema_columns: tuple[str, ...],
) -> dict[str, Any]:
    if timestamp_column is None:
        return _empty_coverage()

    series_column = next((column for column in _SERIES_CANDIDATES if column in schema_columns), None)
    selected_columns = [timestamp_column]
    if series_column is not None:
        selected_columns.append(series_column)
    frame = _scan_inventory_parquet(pl, files).select(selected_columns).collect()
    if frame.height == 0:
        return _empty_coverage()
    frame = frame.drop_nulls(timestamp_column)
    if frame.height == 0:
        return _empty_coverage()

    day_expr = _day_expr(pl, timestamp_column)
    if series_column is None:
        frame = frame.with_columns(pl.lit("aggregate").alias("_inventory_series"))
        series_column = "_inventory_series"
    frame = frame.with_columns(day_expr.alias("_inventory_date"))

    per_series_missing: list[str] = []
    expected_days = 0
    observed_days = 0
    min_day: date | None = None
    max_day: date | None = None
    for row in frame.group_by(series_column).agg(pl.col("_inventory_date").unique().sort()).iter_rows(named=True):
        series = str(row[series_column])
        days = [value for value in row["_inventory_date"] if isinstance(value, date)]
        if not days:
            continue
        first = min(days)
        last = max(days)
        expected = (last - first).days + 1
        observed = len(set(days))
        missing = max(expected - observed, 0)
        expected_days += expected
        observed_days += observed
        min_day = first if min_day is None else min(min_day, first)
        max_day = last if max_day is None else max(max_day, last)
        per_series_missing.append(f"{series}={missing}")

    return {
        "series_count": len(per_series_missing),
        "start_date": min_day.isoformat() if min_day else None,
        "end_date": max_day.isoformat() if max_day else None,
        "expected_days": expected_days,
        "observed_days": observed_days,
        "missing_days": max(expected_days - observed_days, 0),
        "per_series_missing_days": tuple(sorted(per_series_missing)),
    }


def _day_expr(pl: Any, timestamp_column: str) -> Any:
    column = pl.col(timestamp_column)
    return (
        pl.when(column.cast(pl.Utf8).str.contains(r"^\d{4}-\d{2}-\d{2}$"))
        .then(column.cast(pl.Date))
        .otherwise(column.cast(pl.Datetime(time_zone="UTC"), strict=False).dt.date())
    )


def _empty_coverage() -> dict[str, Any]:
    return {
        "series_count": 0,
        "start_date": None,
        "end_date": None,
        "expected_days": None,
        "observed_days": None,
        "missing_days": None,
        "per_series_missing_days": (),
    }


def _scan_inventory_parquet(pl: Any, files: list[Path]) -> Any:
    # Inventory scans summarize historical lake files that may contain additive schema drift.
    # Extra columns are ignored and missing selected columns are inserted so coverage reporting remains read-only and
    # tolerant of backward-compatible field additions or historical sparse partitions.
    return pl.scan_parquet([str(path) for path in files], extra_columns="ignore", missing_columns="insert")


def _timestamp_column(schema_columns: tuple[str, ...], timestamp_hint: str | None) -> str | None:
    if timestamp_hint in schema_columns:
        return timestamp_hint
    return next((column for column in _TIMESTAMP_CANDIDATES if column in schema_columns), None)


def _partition_value(path: Path, key: str) -> str | None:
    prefix = f"{key}="
    for part in path.parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)
    return None


def _origin_for_dataset(dataset: str) -> str:
    if dataset.startswith("gold.live."):
        return "crypto-live-loader"
    if "snapshot" in dataset or dataset in {"recent_trade_snapshot_1m"}:
        return "crypto-live-loader"
    return "crypto-history-loader"


def _require_polars() -> Any:
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for dataset inventory. Install project dependencies.") from exc
    return pl
