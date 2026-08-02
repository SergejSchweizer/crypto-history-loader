"""Build observed Silver rows for the external historical-volatility reference source."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_HISTORICAL_VOLATILITY_OBSERVED_COLUMNS
from application.services.silver_partition_manifest import (
    load_current_manifest,
    publish_partition_atomically,
    source_fingerprint,
)

_HISTORICAL_VOLATILITY_CONTRACT_VERSION = "silver-historical-volatility-observed/v1"


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
class HistoricalVolatilityDependencies:
    """Shared Silver helpers required by historical-volatility transformations."""

    require_polars: Any
    discover_months: Any
    bronze_month_files: Any
    silver_month_path: Any
    iso_utc: Any
    report_factory: SilverReportFactory


def build_historical_volatility_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    bronze_dataset_type: str = "historical_volatility",
    output_dataset_type: str = "historical_volatility_observed",
    dependencies: HistoricalVolatilityDependencies,
) -> object:
    """Build an external historical-volatility reference without RV resampling."""

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    months = dependencies.discover_months(
        bronze_root=bronze_root,
        market=bronze_dataset_type,
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
        instrument_type="perp",
    )
    rows_in = 0
    rows_out = 0
    duplicates_removed = 0
    invalid_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None
    processed: list[str] = []

    for month in months:
        files = dependencies.bronze_month_files(
            bronze_root=bronze_root,
            market=bronze_dataset_type,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            month=month,
            instrument_type="perp",
        )
        if not files:
            continue
        target = dependencies.silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            month=month,
        )
        source_schema = dict(pl.scan_parquet(files).collect_schema())
        fingerprint = source_fingerprint(
            bronze_root=Path(bronze_root),
            source_files=files,
            source_schema=source_schema,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            builder_contract_version=_HISTORICAL_VOLATILITY_CONTRACT_VERSION,
        )
        cached = load_current_manifest(
            parquet_path=target,
            expected_input_fingerprint=fingerprint,
            expected_builder_contract_version=_HISTORICAL_VOLATILITY_CONTRACT_VERSION,
        )
        if cached is not None:
            processed.append(month)
            rows_out += cached.row_count
            continue
        frame = pl.scan_parquet(files).collect()
        rows_in += frame.height
        typed = frame.with_columns(
            [
                pl.col("event_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp"),
                pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
                pl.lit(normalized_symbol).alias("symbol"),
                pl.col("value").cast(pl.Float64).alias("historical_volatility"),
                pl.col("open_time")
                .cast(pl.Datetime(time_unit="us", time_zone="UTC"))
                .alias("historical_volatility_source_timestamp"),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.col("source_endpoint").cast(pl.Utf8),
            ]
        )
        invalid = (
            pl.col("timestamp").is_null()
            | pl.col("historical_volatility_source_timestamp").is_null()
            | pl.col("historical_volatility").is_null()
            | (~pl.col("historical_volatility").is_finite())
            | (pl.col("historical_volatility") < 0.0)
        )
        month_invalid = int(typed.select(invalid.cast(pl.Int64).sum()).item())
        cleaned = typed.filter(~invalid)
        observed = (
            cleaned.sort(["timestamp", "ingested_at"])
            .unique(subset=["exchange", "symbol", "timestamp"], keep="last", maintain_order=True)
            .sort(["exchange", "symbol", "timestamp"])
            .select(SILVER_HISTORICAL_VOLATILITY_OBSERVED_COLUMNS)
        )
        invalid_rows += month_invalid
        duplicates_removed += cleaned.height - observed.height
        if observed.height == 0:
            continue
        publish_partition_atomically(
            frame=observed,
            parquet_path=target,
            input_fingerprint=fingerprint,
            source_schema=source_schema,
            sort_keys=("exchange", "symbol", "timestamp"),
            deduplication_keys=("exchange", "symbol", "timestamp"),
            builder_contract_version=_HISTORICAL_VOLATILITY_CONTRACT_VERSION,
        )
        processed.append(month)
        rows_out += observed.height
        month_min = observed.select(pl.col("timestamp").min()).item()
        month_max = observed.select(pl.col("timestamp").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

    return dependencies.report_factory(
        dataset=output_dataset_type,
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
        period_start=processed[0] if processed else None,
        period_end=processed[-1] if processed else None,
        months_processed=processed,
        rows_in=rows_in,
        rows_out=rows_out,
        duplicates_removed=duplicates_removed,
        invalid_ohlc_rows=invalid_rows,
        null_price_rows=0,
        min_timestamp=dependencies.iso_utc(min_timestamp),
        max_timestamp=dependencies.iso_utc(max_timestamp),
        symbols=[normalized_symbol],
        columns=SILVER_HISTORICAL_VOLATILITY_OBSERVED_COLUMNS,
    )
