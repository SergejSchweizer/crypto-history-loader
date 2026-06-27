"""Bronze parquet sidecar generation and repair helpers."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from ingestion.feature_profile import feature_metadata, write_feature_distribution_plot
from ingestion.lake_layout import (
    PartitionKey,
    dataset_data_files,
    partition_key_from_parquet_path,
)

DEFAULT_BRONZE_SIDECAR_DATASET_TYPES = ("spot", "perp", "oi", "funding", "perp_trades", "option_trades")


def write_bronze_sidecars(
    *,
    file_path: Path,
    dataset_type: str,
    key: PartitionKey,
    rows: list[dict[str, object]],
) -> None:
    """Write required manifest and plot sidecars for one Bronze parquet file."""

    pl = _require_polars()
    exchange, instrument_type, symbol, timeframe, date_partition = key
    frame = pl.DataFrame(rows) if rows else pl.DataFrame()
    min_ts = None
    max_ts = None
    if "open_time" in frame.columns and frame.height > 0:
        min_v = frame.select(pl.col("open_time").min()).item()
        max_v = frame.select(pl.col("open_time").max()).item()
        min_ts = min_v if isinstance(min_v, datetime) else None
        max_ts = max_v if isinstance(max_v, datetime) else None

    payload: dict[str, object] = {
        "dataset": f"{dataset_type}_1m",
        "dataset_type": dataset_type,
        "exchange": exchange,
        "instrument_type": instrument_type,
        "symbol": symbol,
        "timeframe": timeframe,
        "date_partition": date_partition,
        "rows_out": int(frame.height),
        "columns": frame.columns,
        "min_timestamp": _iso_utc(min_ts),
        "max_timestamp": _iso_utc(max_ts),
        "source_data_hash": sha256(
            json.dumps(
                {
                    "rows_out": int(frame.height),
                    "columns": frame.columns,
                    "min_timestamp": _iso_utc(min_ts),
                    "max_timestamp": _iso_utc(max_ts),
                },
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest()[:12],
        "feature_metadata": feature_metadata(pl, frame, exchange),
    }
    manifest_path = file_path.with_suffix(".json")
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    ts_col = next(
        (candidate for candidate in ("timestamp_m1", "open_time", "timestamp") if candidate in frame.columns),
        None,
    )
    if ts_col is not None and ts_col != "timestamp_m1":
        frame = frame.with_columns(pl.col(ts_col).alias("timestamp_m1"))
    plotted = write_feature_distribution_plot(frame, file_path.with_suffix(".png"), normalize_y=False)
    if plotted is None:
        raise RuntimeError(
            "Bronze sidecar policy requires plot generation for every parquet file, but plot generation failed "
            "(missing matplotlib dependency or no plottable numeric columns)."
        )


def ensure_bronze_sidecars(
    *,
    lake_root: str,
    dataset_types: list[str] | None = None,
    log_fn: Any | None = None,
) -> list[str]:
    """Ensure bronze sidecars exist for parquet files under requested dataset types."""

    pq = _require_pyarrow_parquet()
    root = Path(lake_root)
    if not root.exists():
        return []

    selected = dataset_types or list(DEFAULT_BRONZE_SIDECAR_DATASET_TYPES)
    written: list[str] = []
    log = log_fn if callable(log_fn) else None
    if log is not None:
        log("Bronze sidecar backfill start dataset_types=%s", selected)
    for dataset_type in selected:
        paths = dataset_data_files(lake_root, dataset_type)
        dataset_total = len(paths)
        dataset_written = 0
        if log is not None:
            log("Bronze sidecar backfill scan dataset_type=%s parquet_files=%s", dataset_type, dataset_total)
        for idx, parquet_path in enumerate(paths, start=1):
            parsed = partition_key_from_parquet_path(parquet_path)
            if parsed is None:
                continue
            parsed_dataset_type, key = parsed
            if parsed_dataset_type != dataset_type:
                continue
            manifest_path = parquet_path.with_suffix(".json")
            plot_path = parquet_path.with_suffix(".png")
            if manifest_path.exists() and plot_path.exists():
                continue
            table = pq.ParquetFile(parquet_path).read()
            write_bronze_sidecars(
                file_path=parquet_path,
                dataset_type=dataset_type,
                key=key,
                rows=table.to_pylist(),
            )
            written.append(str(parquet_path.resolve()))
            dataset_written += 1
            if log is not None and (dataset_written <= 3 or idx % 50 == 0):
                log(
                    "Bronze sidecar backfill progress dataset_type=%s scanned=%s/%s repaired=%s path=%s",
                    dataset_type,
                    idx,
                    dataset_total,
                    dataset_written,
                    parquet_path,
                )
        if log is not None:
            log(
                "Bronze sidecar backfill done dataset_type=%s scanned=%s repaired=%s",
                dataset_type,
                dataset_total,
                dataset_written,
            )
    if log is not None:
        log("Bronze sidecar backfill complete repaired_total=%s", len(written))
    return written


def _require_pyarrow_parquet() -> Any:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "pyarrow is required for parquet lake sidecar repair. Install project dependencies."
        ) from exc
    return pq


def _require_polars() -> Any:
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for bronze sidecar generation. Install project dependencies.") from exc
    return pl


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
