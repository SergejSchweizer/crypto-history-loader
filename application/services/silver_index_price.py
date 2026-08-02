"""Silver builders for index-price snapshot datasets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_INDEX_PRICE_FEATURE_COLUMNS, SILVER_INDEX_PRICE_OBSERVED_COLUMNS
from application.services.silver_partition_manifest import (
    load_current_manifest,
    publish_partition_atomically,
    source_fingerprint,
)

_INDEX_PRICE_CONTRACT_VERSION = "silver-index-price-observed/v1"
_INDEX_PRICE_FEATURE_CONTRACT_VERSION = "silver-index-price-feature/v1"


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
class IndexPriceDependencies:
    """Shared Silver helpers required by index-price transformations."""

    require_polars: Any
    silver_month_path: Any
    iso_utc: Any
    report_factory: SilverReportFactory


def _symbol_from_index_name(index_name: str) -> str:
    return index_name.strip().upper().replace("-", "_").split("_", 1)[0]


def discover_index_price_symbols(
    *,
    bronze_root: str,
    exchange: str,
    dataset_type: str = "index_price_snapshot_1m",
) -> list[str]:
    """Discover base symbols from index-price Bronze partitions."""

    root = Path(bronze_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
    if not root.exists():
        return []
    return sorted(
        _symbol_from_index_name(path.name.split("=", 1)[1])
        for path in root.glob("index_name=*")
        if path.is_dir() and path.name.startswith("index_name=")
    )


def _matching_index_dirs(
    *,
    bronze_root: str,
    dataset_type: str,
    exchange: str,
    symbol: str,
) -> list[Path]:
    root = Path(bronze_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.glob("index_name=*")
        if path.is_dir()
        and path.name.startswith("index_name=")
        and _symbol_from_index_name(path.name.split("=", 1)[1]) == symbol
    )


def _bronze_months(index_dirs: list[Path]) -> list[str]:
    months: set[str] = set()
    for index_dir in index_dirs:
        for month_dir in index_dir.glob("year=*/month=*"):
            year = month_dir.parent.name.split("=", 1)[1]
            month = month_dir.name.split("=", 1)[1]
            months.add(f"{year}-{month}" if len(month) == 2 else month)
    return sorted(months)


def _bronze_month_files(index_dirs: list[Path], month: str) -> list[str]:
    year, month_part = month.split("-", 1)
    files: list[Path] = []
    for index_dir in index_dirs:
        files.extend(index_dir.glob(f"year={year}/month={month_part}/date=*/hour=*/data.parquet"))
    return sorted(str(path) for path in files)


def _observed_month_file(
    *,
    silver_root: str,
    dataset_type: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    month: str,
) -> Path:
    year = month.split("-", 1)[0]
    return (
        Path(silver_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"year={year}"
        / f"month={month}"
        / f"{symbol}-{month}.parquet"
    )


def _observed_months(
    *,
    silver_root: str,
    dataset_type: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[str]:
    root = (
        Path(silver_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not root.exists():
        return []
    return sorted(path.name.split("=", 1)[1] for path in root.glob("year=*/month=*") if path.name.startswith("month="))


def build_index_price_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    bronze_dataset_type: str = "index_price_snapshot_1m",
    output_dataset_type: str = "index_price_snapshot_1m_observed",
    dependencies: IndexPriceDependencies,
) -> object:
    """Build observed index-price Silver snapshots for one base symbol."""

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    index_dirs = _matching_index_dirs(
        bronze_root=bronze_root,
        dataset_type=bronze_dataset_type,
        exchange=exchange,
        symbol=normalized_symbol,
    )
    months = _bronze_months(index_dirs)
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    agg_invalid_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = _bronze_month_files(index_dirs, month)
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
            builder_contract_version=_INDEX_PRICE_CONTRACT_VERSION,
        )
        cached = load_current_manifest(
            parquet_path=target,
            expected_input_fingerprint=fingerprint,
            expected_builder_contract_version=_INDEX_PRICE_CONTRACT_VERSION,
        )
        if cached is not None:
            agg_rows_out += cached.row_count
            continue
        frame = pl.scan_parquet(files).collect()
        rows_in = frame.height
        if rows_in == 0:
            continue
        frame = frame.with_columns(
            [
                pl.col("event_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp"),
                pl.lit(normalized_symbol).alias("symbol"),
                pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
                pl.col("index_name").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("index_name"),
                pl.col("price").cast(pl.Float64).alias("index_price"),
                pl.col("snapshot_time")
                .cast(pl.Datetime(time_unit="us", time_zone="UTC"))
                .alias("index_price_source_timestamp"),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("ingested_at"),
                pl.col("source").cast(pl.Utf8).alias("source_endpoint"),
            ]
        )
        invalid_expr = (
            pl.col("timestamp").is_null()
            | pl.col("index_price").is_null()
            | (~pl.col("index_price").is_finite())
            | (pl.col("index_price") <= 0.0)
        )
        invalid_rows = frame.select(invalid_expr.cast(pl.Int64).sum().alias("count")).item()
        cleaned = frame.filter(~invalid_expr)
        observed = (
            cleaned.sort(["timestamp", "ingested_at"])
            .unique(subset=["exchange", "symbol", "timestamp"], keep="last", maintain_order=True)
            .sort(["exchange", "symbol", "timestamp"])
            .select(SILVER_INDEX_PRICE_OBSERVED_COLUMNS)
        )
        duplicates_removed = cleaned.height - observed.height

        publish_partition_atomically(
            frame=observed,
            parquet_path=target,
            input_fingerprint=fingerprint,
            source_schema=source_schema,
            sort_keys=("exchange", "symbol", "timestamp"),
            deduplication_keys=("exchange", "symbol", "timestamp"),
            builder_contract_version=_INDEX_PRICE_CONTRACT_VERSION,
        )

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
        symbol=normalized_symbol,
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
        symbols=[normalized_symbol],
        columns=SILVER_INDEX_PRICE_OBSERVED_COLUMNS,
    )


def build_index_price_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    observed_dataset_type: str = "index_price_snapshot_1m_observed",
    output_dataset_type: str = "index_price_1m_feature",
    dependencies: IndexPriceDependencies,
) -> object:
    """Build minute-grid index-price features from observed index-price snapshots."""

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    months = _observed_months(
        silver_root=silver_root,
        dataset_type=observed_dataset_type,
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
    )
    agg_rows_in = 0
    agg_rows_out = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        path = _observed_month_file(
            silver_root=silver_root,
            dataset_type=observed_dataset_type,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            month=month,
        )
        if not path.exists():
            continue
        source_schema = dict(pl.scan_parquet(str(path)).collect_schema())
        fingerprint = source_fingerprint(
            bronze_root=Path(silver_root),
            source_files=[str(path)],
            source_schema=source_schema,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            builder_contract_version=_INDEX_PRICE_FEATURE_CONTRACT_VERSION,
        )
        target = dependencies.silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            month=month,
        )
        cached = load_current_manifest(
            parquet_path=target,
            expected_input_fingerprint=fingerprint,
            expected_builder_contract_version=_INDEX_PRICE_FEATURE_CONTRACT_VERSION,
        )
        if cached is not None:
            agg_rows_out += cached.row_count
            continue
        observed = pl.read_parquet(path).sort(["exchange", "symbol", "timestamp"])
        rows_in = observed.height
        if rows_in == 0:
            continue
        start = observed.select(pl.col("timestamp").min()).item()
        end = observed.select(pl.col("timestamp").max()).item()
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            continue
        grid = pl.DataFrame(
            {
                "timestamp_m1": pl.datetime_range(start, end, interval="1m", eager=True),
                "exchange": [exchange] * (int((end - start).total_seconds() // 60) + 1),
                "symbol": [normalized_symbol] * (int((end - start).total_seconds() // 60) + 1),
            }
        ).with_columns(pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")))
        lookup = observed.select(
            [
                pl.col("timestamp").alias("observed_timestamp"),
                "exchange",
                "symbol",
                "index_price",
                "index_price_source_timestamp",
            ]
        ).sort(["exchange", "symbol", "observed_timestamp"])
        feature = (
            grid.sort(["exchange", "symbol", "timestamp_m1"])
            .join_asof(
                lookup,
                left_on="timestamp_m1",
                right_on="observed_timestamp",
                by=["exchange", "symbol"],
                strategy="backward",
                check_sortedness=False,
            )
            .with_columns(
                [
                    (pl.col("timestamp_m1") == pl.col("observed_timestamp")).alias("index_price_is_observed"),
                    ((pl.col("timestamp_m1") - pl.col("observed_timestamp")).dt.total_minutes().cast(pl.Int64)).alias(
                        "minutes_since_index_price_observation"
                    ),
                ]
            )
            .select(SILVER_INDEX_PRICE_FEATURE_COLUMNS)
        )

        publish_partition_atomically(
            frame=feature,
            parquet_path=target,
            input_fingerprint=fingerprint,
            source_schema=source_schema,
            sort_keys=("exchange", "symbol", "timestamp_m1"),
            deduplication_keys=("exchange", "symbol", "timestamp_m1"),
            builder_contract_version=_INDEX_PRICE_FEATURE_CONTRACT_VERSION,
        )

        month_min = feature.select(pl.col("timestamp_m1").min()).item()
        month_max = feature.select(pl.col("timestamp_m1").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max
        agg_rows_in += rows_in
        agg_rows_out += feature.height

    return dependencies.report_factory(
        dataset=output_dataset_type,
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
        period_start=months[0] if months else None,
        period_end=months[-1] if months else None,
        months_processed=months,
        rows_in=agg_rows_in,
        rows_out=agg_rows_out,
        duplicates_removed=0,
        invalid_ohlc_rows=0,
        null_price_rows=0,
        min_timestamp=dependencies.iso_utc(min_timestamp),
        max_timestamp=dependencies.iso_utc(max_timestamp),
        symbols=[normalized_symbol],
        columns=SILVER_INDEX_PRICE_FEATURE_COLUMNS,
    )
