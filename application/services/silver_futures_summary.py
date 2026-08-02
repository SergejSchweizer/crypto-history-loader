"""Silver builders for futures summary snapshot datasets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import (
    SILVER_FUTURES_SUMMARY_FEATURE_COLUMNS,
    SILVER_FUTURES_SUMMARY_OBSERVED_COLUMNS,
)
from application.services.silver_partition_manifest import (
    load_current_manifest,
    publish_partition_atomically,
    source_fingerprint,
)

_FUTURES_SUMMARY_CONTRACT_VERSION = "silver-futures-summary-observed/v1"
_FUTURES_SUMMARY_FEATURE_CONTRACT_VERSION = "silver-futures-summary-feature/v1"


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
class FuturesSummaryDependencies:
    """Shared Silver helpers required by futures-summary transformations."""

    require_polars: Any
    silver_month_path: Any
    iso_utc: Any
    report_factory: SilverReportFactory


def discover_futures_summary_symbols(
    *,
    bronze_root: str,
    exchange: str,
    dataset_type: str = "futures_summary_snapshot_1m",
) -> list[str]:
    """Discover currencies available in futures-summary Bronze partitions."""

    root = Path(bronze_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}" / "instrument_type=future"
    if not root.exists():
        return []
    return sorted(
        path.name.split("=", 1)[1].upper()
        for path in root.glob("currency=*")
        if path.is_dir() and path.name.startswith("currency=")
    )


def _currency_root(*, bronze_root: str, dataset_type: str, exchange: str, currency: str, source: str) -> Path:
    return (
        Path(bronze_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / "instrument_type=future"
        / f"currency={currency}"
        / f"source={source}"
    )


def _bronze_months(root: Path) -> list[str]:
    if not root.exists():
        return []
    months: set[str] = set()
    for month_dir in root.glob("year=*/month=*"):
        year = month_dir.parent.name.split("=", 1)[1]
        month = month_dir.name.split("=", 1)[1]
        months.add(f"{year}-{month}" if len(month) == 2 else month)
    return sorted(months)


def _bronze_month_files(root: Path, month: str) -> list[str]:
    year, month_part = month.split("-", 1)
    return sorted(str(path) for path in root.glob(f"year={year}/month={month_part}/date=*/hour=*/data.parquet"))


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


def _observed_months(*, silver_root: str, dataset_type: str, exchange: str, symbol: str, timeframe: str) -> list[str]:
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


def _column_or_null(pl: Any, frame: Any, column_name: str, dtype: Any) -> Any:
    if column_name in frame.columns:
        return pl.col(column_name).cast(dtype)
    return pl.lit(None, dtype=dtype)


def _bronze_scan_schema(pl: Any) -> dict[str, Any]:
    return {
        "snapshot_time": pl.Datetime(time_unit="us", time_zone="UTC"),
        "instrument_name": pl.Utf8,
        "exchange": pl.Utf8,
        "instrument_type": pl.Utf8,
        "mark_price": pl.Float64,
        "underlying_price": pl.Float64,
        "estimated_delivery_price": pl.Float64,
        "open_interest": pl.Float64,
        "volume": pl.Float64,
        "volume_usd": pl.Float64,
        "interest_rate": pl.Float64,
        "ingested_at": pl.Datetime(time_unit="us", time_zone="UTC"),
        "source": pl.Utf8,
    }


def _scan_bronze_month(pl: Any, files: list[str]) -> Any:
    # The Bronze Deribit response includes fields outside the Silver contract whose parquet dtype can drift between
    # all-null and numeric files. Projecting the required contract columns avoids failures from unused source fields.
    return pl.scan_parquet(
        files,
        schema=_bronze_scan_schema(pl),
        missing_columns="insert",
        extra_columns="ignore",
    ).collect()


def build_futures_summary_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    bronze_dataset_type: str = "futures_summary_snapshot_1m",
    output_dataset_type: str = "futures_summary_snapshot_1m_observed",
    source: str = "rest_get_book_summary_by_currency",
    dependencies: FuturesSummaryDependencies,
) -> object:
    """Build observed futures-summary Silver snapshots for one currency."""

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    root = _currency_root(
        bronze_root=bronze_root,
        dataset_type=bronze_dataset_type,
        exchange=exchange,
        currency=normalized_symbol,
        source=source,
    )
    months = _bronze_months(root)
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = _bronze_month_files(root, month)
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
        source_schema = _bronze_scan_schema(pl)
        fingerprint = source_fingerprint(
            bronze_root=Path(bronze_root),
            source_files=files,
            source_schema=source_schema,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            builder_contract_version=_FUTURES_SUMMARY_CONTRACT_VERSION,
        )
        cached = load_current_manifest(
            parquet_path=target,
            expected_input_fingerprint=fingerprint,
            expected_builder_contract_version=_FUTURES_SUMMARY_CONTRACT_VERSION,
        )
        if cached is not None:
            agg_rows_out += cached.row_count
            continue
        frame = _scan_bronze_month(pl, files)
        rows_in = frame.height
        if rows_in == 0:
            continue
        frame = frame.with_columns(
            [
                pl.col("snapshot_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp"),
                pl.col("instrument_name").cast(pl.Utf8).str.strip_chars().str.to_uppercase().alias("symbol"),
                pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
                pl.col("instrument_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("instrument_type"),
                _column_or_null(pl, frame, "mark_price", pl.Float64).alias("mark_price"),
                pl.coalesce(
                    [
                        _column_or_null(pl, frame, "underlying_price", pl.Float64),
                        _column_or_null(pl, frame, "estimated_delivery_price", pl.Float64),
                    ]
                ).alias("index_price"),
                _column_or_null(pl, frame, "open_interest", pl.Float64).alias("open_interest"),
                _column_or_null(pl, frame, "volume", pl.Float64).alias("volume"),
                _column_or_null(pl, frame, "volume_usd", pl.Float64).alias("turnover"),
                _column_or_null(pl, frame, "interest_rate", pl.Float64).alias("funding_rate"),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("ingested_at"),
                pl.col("source").cast(pl.Utf8).alias("source_endpoint"),
            ]
        )
        invalid_expr = (
            pl.col("timestamp").is_null() | pl.col("symbol").is_null() | (pl.col("symbol").str.len_chars() == 0)
        )
        cleaned = frame.filter(~invalid_expr)
        observed = (
            cleaned.sort(["timestamp", "ingested_at"])
            .unique(subset=["exchange", "symbol", "timestamp"], keep="last", maintain_order=True)
            .sort(["exchange", "symbol", "timestamp"])
            .select(SILVER_FUTURES_SUMMARY_OBSERVED_COLUMNS)
        )
        duplicates_removed = cleaned.height - observed.height
        publish_partition_atomically(
            frame=observed,
            parquet_path=target,
            input_fingerprint=fingerprint,
            source_schema=source_schema,
            sort_keys=("exchange", "symbol", "timestamp"),
            deduplication_keys=("exchange", "symbol", "timestamp"),
            builder_contract_version=_FUTURES_SUMMARY_CONTRACT_VERSION,
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
        invalid_ohlc_rows=0,
        null_price_rows=0,
        min_timestamp=dependencies.iso_utc(min_timestamp),
        max_timestamp=dependencies.iso_utc(max_timestamp),
        symbols=[normalized_symbol],
        columns=SILVER_FUTURES_SUMMARY_OBSERVED_COLUMNS,
    )


def build_futures_summary_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    observed_dataset_type: str = "futures_summary_snapshot_1m_observed",
    output_dataset_type: str = "futures_summary_1m_feature",
    dependencies: FuturesSummaryDependencies,
) -> object:
    """Build freshness-aware futures-summary 1m features from observed snapshots."""

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
            builder_contract_version=_FUTURES_SUMMARY_FEATURE_CONTRACT_VERSION,
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
            expected_builder_contract_version=_FUTURES_SUMMARY_FEATURE_CONTRACT_VERSION,
        )
        if cached is not None:
            agg_rows_out += cached.row_count
            continue
        observed = pl.read_parquet(path)
        rows_in = observed.height
        if rows_in == 0:
            continue
        start = observed.select(pl.col("timestamp").min()).item()
        end = observed.select(pl.col("timestamp").max()).item()
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            continue
        instruments = observed.select(["exchange", "symbol", "instrument_type"]).unique().sort(["exchange", "symbol"])
        grid = instruments.join(
            pl.DataFrame({"timestamp_m1": pl.datetime_range(start, end, interval="1m", eager=True)}),
            how="cross",
        ).with_columns(pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")))
        lookup = observed.rename({"timestamp": "observed_timestamp"}).sort(["exchange", "symbol", "observed_timestamp"])
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
                    (pl.col("timestamp_m1") == pl.col("observed_timestamp")).alias("summary_is_observed"),
                    ((pl.col("timestamp_m1") - pl.col("observed_timestamp")).dt.total_minutes().cast(pl.Int64)).alias(
                        "minutes_since_summary_observation"
                    ),
                    (pl.col("mark_price") - pl.col("index_price")).alias("mark_index_spread"),
                    pl.when(pl.col("index_price") > 0.0)
                    .then(pl.col("mark_price") / pl.col("index_price"))
                    .otherwise(None)
                    .alias("mark_index_ratio"),
                ]
            )
            .select(SILVER_FUTURES_SUMMARY_FEATURE_COLUMNS)
        )
        publish_partition_atomically(
            frame=feature,
            parquet_path=target,
            input_fingerprint=fingerprint,
            source_schema=source_schema,
            sort_keys=("exchange", "symbol", "instrument_type", "timestamp_m1"),
            deduplication_keys=("exchange", "symbol", "instrument_type", "timestamp_m1"),
            builder_contract_version=_FUTURES_SUMMARY_FEATURE_CONTRACT_VERSION,
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
        columns=SILVER_FUTURES_SUMMARY_FEATURE_COLUMNS,
    )
