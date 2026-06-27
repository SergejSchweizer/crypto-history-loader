"""Silver sidecar manifest and plot writers."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from ingestion.feature_profile import (
    feature_hash,
    feature_metadata,
    write_feature_distribution_plot,
)


class SilverSidecarReport(Protocol):
    """Report fields required to write Silver monthly sidecars."""

    @property
    def dataset(self) -> str: ...

    @property
    def exchange(self) -> str: ...

    @property
    def symbol(self) -> str: ...

    @property
    def timeframe(self) -> str: ...

    @property
    def months_processed(self) -> list[str]: ...


def _require_polars() -> Any:
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for silver sidecar writing. Install project dependencies.") from exc
    return pl


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _with_timestamp_m1(frame: Any) -> Any:
    pl = _require_polars()
    ts_col = next(
        (candidate for candidate in ("timestamp_m1", "open_time", "timestamp") if candidate in frame.columns), None
    )
    if ts_col is None or ts_col == "timestamp_m1":
        return frame
    return frame.with_columns(pl.col(ts_col).alias("timestamp_m1"))


def _write_silver_plot(frame: Any, output_path: Path) -> str | None:
    pl = _require_polars()
    ts_col = next(
        (candidate for candidate in ("timestamp_m1", "open_time", "timestamp") if candidate in frame.columns), None
    )
    if ts_col is None:
        return None
    if ts_col != "timestamp_m1":
        frame = frame.with_columns(pl.col(ts_col).alias("timestamp_m1"))
    if "exchange" not in frame.columns:
        frame = frame.with_columns(pl.lit("deribit").alias("exchange"))
    if "symbol" not in frame.columns:
        frame = frame.with_columns(pl.lit("unknown").alias("symbol"))
    return write_feature_distribution_plot(frame, output_path, normalize_y=False)


def write_monthly_sidecars(
    *,
    silver_root: str,
    market: str,
    exchange: str,
    symbol: str,
    report: SilverSidecarReport,
    write_manifest: bool = True,
    plot: bool = False,
) -> tuple[list[str], list[str]]:
    """Write per-month manifest and plot sidecars next to Silver monthly parquet files."""

    pl = _require_polars()
    base_root = (
        Path(silver_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={report.timeframe}"
    )
    manifest_paths: list[str] = []
    plot_paths: list[str] = []

    for month in report.months_processed:
        year = month.split("-", 1)[0]
        stem = f"{symbol}-{month}"
        parquet_path = base_root / f"year={year}" / f"month={month}" / f"{stem}.parquet"
        if not parquet_path.exists():
            continue
        frame = pl.read_parquet(parquet_path)
        frame_for_gold = _with_timestamp_m1(frame)
        plotted: str | None = None

        if plot:
            plotted = _write_silver_plot(frame_for_gold, parquet_path.with_suffix(".png"))
            if plotted is not None:
                plot_paths.append(plotted)

        if write_manifest:
            min_ts: datetime | None = None
            max_ts: datetime | None = None
            if "timestamp_m1" in frame_for_gold.columns and frame_for_gold.height > 0:
                min_v = frame_for_gold.select(pl.col("timestamp_m1").min()).item()
                max_v = frame_for_gold.select(pl.col("timestamp_m1").max()).item()
                min_ts = min_v if isinstance(min_v, datetime) else None
                max_ts = max_v if isinstance(max_v, datetime) else None
            payload = {
                "dataset": report.dataset,
                "exchange": report.exchange,
                "symbol": report.symbol,
                "build_date_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "column_hash": feature_hash(frame.columns),
                "rows_out": frame.height,
                "columns": frame.columns,
                "min_timestamp": _iso_utc(min_ts),
                "max_timestamp": _iso_utc(max_ts),
                "source_silver_datasets": {
                    report.dataset: {
                        "columns": frame.columns,
                        "rows": frame.height,
                        "source_symbols": sorted(set(frame.get_column("symbol").cast(pl.Utf8).to_list()))
                        if "symbol" in frame.columns
                        else [report.symbol],
                    }
                },
                "feature_metadata": feature_metadata(pl, frame_for_gold, report.exchange),
                "plot_generated": plotted is not None,
            }
            manifest_path = parquet_path.with_suffix(".json")
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            manifest_paths.append(str(manifest_path.resolve()))

    return manifest_paths, plot_paths
