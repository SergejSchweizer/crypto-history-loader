"""Silver transformations for volatility dataset families."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_VOLATILITY_OBSERVED_COLUMNS


class SilverReportFactory(Protocol):
    """Factory contract for constructing Silver build reports."""

    def __call__(
        self,
        *,
        dataset: str,
        exchange: str,
        symbol: str,
        timeframe: str,
        period_start: str | None,
        period_end: str | None,
        months_processed: list[str],
        rows_in: int,
        rows_out: int,
        duplicates_removed: int,
        invalid_ohlc_rows: int,
        null_price_rows: int,
        min_timestamp: str | None,
        max_timestamp: str | None,
        symbols: list[str],
        columns: list[str],
    ) -> object: ...


@dataclass(frozen=True)
class VolatilityObservedDependencies:
    """Shared Silver helpers required by volatility observed transformations."""

    require_polars: Callable[[], Any]
    discover_months: Callable[..., list[str]]
    bronze_month_files: Callable[..., list[str]]
    silver_month_path: Callable[..., Path]
    normalize_symbol_expr: Callable[..., Any]
    iso_utc: Callable[[datetime | None], str | None]
    report_factory: SilverReportFactory


def build_volatility_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    bronze_dataset_type: str,
    output_dataset_type: str,
    dependencies: VolatilityObservedDependencies,
) -> object:
    """Build monthly volatility-observed Silver outputs from Bronze volatility datasets.

    Args:
        bronze_root: Root directory for Bronze input parquet files.
        silver_root: Root directory for Silver output parquet files.
        exchange: Exchange partition value.
        symbol: Symbol partition value.
        timeframe: Timeframe partition value for both Bronze input and Silver output.
        bronze_dataset_type: Source Bronze volatility dataset type.
        output_dataset_type: Target Silver observed dataset type.
        dependencies: Shared Silver helper functions supplied by the orchestration service.

    Returns:
        A Silver build report object created by ``dependencies.report_factory``.
    """

    pl = dependencies.require_polars()
    months = dependencies.discover_months(
        bronze_root=bronze_root,
        market=bronze_dataset_type,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        instrument_type="perp",
    )
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    agg_invalid_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = dependencies.bronze_month_files(
            bronze_root=bronze_root,
            market=bronze_dataset_type,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
            instrument_type="perp",
        )
        if not files:
            continue
        frame = pl.scan_parquet(files).collect()
        rows_in = frame.height
        if rows_in == 0:
            continue

        frame = frame.with_columns(
            [
                pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp"),
                pl.col("value").cast(pl.Float64).alias("volatility_value"),
                dependencies.normalize_symbol_expr(pl, "symbol").alias("symbol"),
                pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
                pl.col("instrument_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("instrument_type"),
                pl.col("dataset_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("dataset_type"),
                pl.col("source_endpoint").cast(pl.Utf8).alias("source_endpoint"),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("ingested_at"),
            ]
        )

        invalid_expr = (
            pl.col("timestamp").is_null()
            | pl.col("symbol").is_null()
            | (pl.col("symbol").str.len_chars() == 0)
            | pl.col("volatility_value").is_null()
            | (~pl.col("volatility_value").is_finite())
            | (pl.col("volatility_value") < 0.0)
        )
        invalid_rows = frame.select(invalid_expr.cast(pl.Int64).sum().alias("count")).item()
        cleaned = frame.filter(~invalid_expr)
        observed = (
            cleaned.unique(
                subset=["exchange", "symbol", "dataset_type", "timestamp"],
                keep="last",
                maintain_order=True,
            )
            .sort(["exchange", "symbol", "timestamp"])
            .with_columns(pl.col("timestamp").alias("volatility_source_timestamp"))
            .select(SILVER_VOLATILITY_OBSERVED_COLUMNS)
        )
        duplicates_removed = cleaned.height - observed.height

        target = dependencies.silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        observed.write_parquet(target)

        month_min = observed.select(pl.col("timestamp").min()).item()
        month_max = observed.select(pl.col("timestamp").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += rows_in
        agg_rows_out += observed.height
        agg_duplicates_removed += int(duplicates_removed)
        agg_invalid_rows += int(invalid_rows)

    return dependencies.report_factory(
        dataset=output_dataset_type,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        period_start=months[0] if months else None,
        period_end=months[-1] if months else None,
        months_processed=months,
        rows_in=agg_rows_in,
        rows_out=agg_rows_out,
        duplicates_removed=agg_duplicates_removed,
        invalid_ohlc_rows=agg_invalid_rows,
        null_price_rows=0,
        min_timestamp=dependencies.iso_utc(min_timestamp),
        max_timestamp=dependencies.iso_utc(max_timestamp),
        symbols=[symbol],
        columns=SILVER_VOLATILITY_OBSERVED_COLUMNS,
    )
