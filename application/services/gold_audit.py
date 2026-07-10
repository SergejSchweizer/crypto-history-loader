"""Gold dataset audit helpers for manifests and validation reports."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def time_span_coverage(
    pl: Any,
    frame: Any,
) -> tuple[datetime | None, datetime | None, int | None, int | None, float | None]:
    """Return timestamp span, expected rows, missing minutes, and row coverage ratio."""

    min_ts = frame.select(pl.col("timestamp_m1").min()).item()
    max_ts = frame.select(pl.col("timestamp_m1").max()).item()
    expected_minutes: int | None = None
    missing_minutes: int | None = None
    observed_coverage_ratio: float | None = None
    if isinstance(min_ts, datetime) and isinstance(max_ts, datetime):
        expected_minutes = int(((max_ts - min_ts).total_seconds() // 60) + 1)
        if expected_minutes > 0:
            observed_coverage_ratio = frame.height / float(expected_minutes)
            missing_minutes = max(expected_minutes - frame.height, 0)
    return min_ts, max_ts, expected_minutes, missing_minutes, observed_coverage_ratio


def source_dataset_summary(
    pl: Any, raw_by_dataset: dict[str, Any], l2_source_path: Path | None
) -> dict[str, dict[str, object]]:
    """Build manifest summary of raw Silver and optional L2 source inputs."""

    summary: dict[str, dict[str, object]] = {}
    for dataset_type, raw in raw_by_dataset.items():
        source_key = f"{dataset_type}_1m" if dataset_type in {"spot_ohlcv", "perps_ohlcv"} else dataset_type
        source_symbols = (
            sorted(set(raw.get_column("symbol").cast(pl.Utf8).to_list())) if "symbol" in raw.columns else []
        )
        summary[source_key] = {
            "columns": raw.columns,
            "rows": raw.height,
            "source_symbols": source_symbols,
        }
        if dataset_type == "gold_l2_m1" and l2_source_path is not None:
            summary[source_key]["source_artifact"] = l2_source_path.name
    return summary


def optional_source_availability(
    pl: Any,
    optional_requirements: list[tuple[str, str]],
    raw_by_dataset: dict[str, Any],
    prepared_by_dataset: dict[str, Any],
    required_grid: Any,
) -> dict[str, dict[str, object]]:
    """Report optional-source presence, grid coverage, and end-of-grid freshness."""

    grid_min = required_grid.select(pl.col("timestamp_m1").min()).item()
    grid_max = required_grid.select(pl.col("timestamp_m1").max()).item()
    grid_rows = required_grid.height
    availability: dict[str, dict[str, object]] = {}
    for dataset_type, timeframe in optional_requirements:
        raw = raw_by_dataset.get(dataset_type)
        prepared = prepared_by_dataset.get(dataset_type)
        if raw is None or prepared is None or prepared.height == 0:
            availability[dataset_type] = {
                "timeframe": timeframe,
                "available": False,
                "source_rows": 0,
                "prepared_rows": 0,
                "grid_covered_minutes": 0,
                "grid_coverage_ratio": 0.0,
                "min_source_timestamp": None,
                "max_source_timestamp": None,
                "freshness_minutes_at_grid_end": None,
            }
            continue
        source_min = prepared.select(pl.col("timestamp_m1").min()).item()
        source_max = prepared.select(pl.col("timestamp_m1").max()).item()
        in_grid = prepared
        if isinstance(grid_min, datetime) and isinstance(grid_max, datetime):
            in_grid = prepared.filter((pl.col("timestamp_m1") >= grid_min) & (pl.col("timestamp_m1") <= grid_max))
        covered_minutes = in_grid.select(pl.col("timestamp_m1").n_unique()).item()
        covered = int(covered_minutes or 0)
        freshness: float | None = None
        if isinstance(grid_max, datetime) and isinstance(source_max, datetime):
            freshness = max((grid_max - source_max).total_seconds() / 60.0, 0.0)
        availability[dataset_type] = {
            "timeframe": timeframe,
            "available": True,
            "source_rows": raw.height,
            "prepared_rows": prepared.height,
            "grid_covered_minutes": covered,
            "grid_coverage_ratio": covered / grid_rows if grid_rows else 0.0,
            "min_source_timestamp": _iso_utc(source_min if isinstance(source_min, datetime) else None),
            "max_source_timestamp": _iso_utc(source_max if isinstance(source_max, datetime) else None),
            "freshness_minutes_at_grid_end": freshness,
        }
    return availability


def missing_value_audit(pl: Any, frame: Any) -> tuple[dict[str, int], int]:
    """Return missing-value counts per column and in total."""

    missing_by_column = {col: int(frame.select(pl.col(col).is_null().sum()).item()) for col in frame.columns}
    missing_total = int(sum(missing_by_column.values()))
    return missing_by_column, missing_total


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
