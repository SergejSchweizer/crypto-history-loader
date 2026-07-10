"""Silver builders for options ticker snapshot datasets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS


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
class OptionsTickerDependencies:
    """Shared Silver helpers required by options-ticker transformations."""

    require_polars: Any
    silver_month_path: Any
    iso_utc: Any
    report_factory: SilverReportFactory


def discover_options_ticker_symbols(
    *,
    bronze_root: str,
    exchange: str,
    dataset_type: str = "options_ticker_snapshot_1m",
) -> list[str]:
    """Discover currencies available in options-ticker Bronze partitions."""

    root = Path(bronze_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}" / "instrument_type=option"
    if not root.exists():
        return []
    return sorted(
        path.name.split("=", 1)[1].upper()
        for path in root.glob("currency=*")
        if path.is_dir() and path.name.startswith("currency=")
    )


def _currency_root(
    *,
    bronze_root: str,
    dataset_type: str,
    exchange: str,
    currency: str,
    source: str | None,
) -> Path:
    root = (
        Path(bronze_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / "instrument_type=option"
        / f"currency={currency}"
    )
    if source is not None:
        return root / f"source={source}"
    return root


def _bronze_months(root: Path) -> list[str]:
    if not root.exists():
        return []
    months: set[str] = set()
    for month_dir in root.glob("**/year=*/month=*"):
        year = month_dir.parent.name.split("=", 1)[1]
        month = month_dir.name.split("=", 1)[1]
        months.add(f"{year}-{month}" if len(month) == 2 else month)
    return sorted(months)


def _bronze_month_files(root: Path, month: str) -> list[str]:
    year, month_part = month.split("-", 1)
    return sorted(str(path) for path in root.glob(f"**/year={year}/month={month_part}/date=*/hour=*/data.parquet"))


def _collect_parquet_files(pl: Any, files: list[str]) -> Any:
    try:
        return pl.scan_parquet(files).collect()
    except Exception as exc:
        if exc.__class__.__name__ != "SchemaError":
            raise
    # Deribit ticker snapshots may encode all-null optional fields as Null in
    # some hourly files, numeric in others, or omit sparse optional fields
    # entirely. Diagonal relaxed concat inserts missing optional columns and
    # keeps market fields nullable instead of failing the whole month.
    return pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")


def _column_or_null(pl: Any, frame: Any, column_name: str, dtype: Any) -> Any:
    if column_name in frame.columns:
        return pl.col(column_name).cast(dtype)
    return pl.lit(None, dtype=dtype)


def _first_column_or_null(pl: Any, frame: Any, column_names: tuple[str, ...], dtype: Any) -> Any:
    expressions = [pl.col(column_name).cast(dtype) for column_name in column_names if column_name in frame.columns]
    if expressions:
        return pl.coalesce(expressions)
    return pl.lit(None, dtype=dtype)


def _write_reconciliation_report(
    *,
    pl: Any,
    observed: Any,
    reference_path: Path,
    target_path: Path,
    exchange: str,
    symbol: str,
    month: str,
    output_dataset_type: str,
) -> None:
    report_path = target_path.with_suffix(".reconciliation.json")
    report: dict[str, object] = {
        "dataset": output_dataset_type,
        "reference_dataset": "options_ticker_snapshot_1m_observed",
        "exchange": exchange,
        "symbol": symbol,
        "month": month,
        "precedence_dataset": output_dataset_type,
        "reference_available": reference_path.exists(),
        "overlapping_records": 0,
        "instrument_only_records": observed.height,
        "reference_only_records": 0,
        "field_mismatch_counts": {},
    }
    if reference_path.exists() and observed.height > 0:
        reference = pl.read_parquet(reference_path).select(SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS)
        keys = ["exchange", "instrument_name", "timestamp"]
        overlap = observed.join(reference, on=keys, how="inner", suffix="_reference")
        instrument_only = observed.join(reference.select(keys), on=keys, how="anti")
        reference_only = reference.join(observed.select(keys), on=keys, how="anti")
        fields = [
            "mark_price",
            "bid_price",
            "ask_price",
            "implied_volatility",
            "delta",
            "gamma",
            "vega",
            "theta",
            "open_interest",
            "volume",
        ]
        mismatch_counts: dict[str, int] = {}
        for field in fields:
            mismatch_counts[field] = int(
                overlap.select(
                    (
                        pl.col(field).is_not_null()
                        & pl.col(f"{field}_reference").is_not_null()
                        & (pl.col(field) != pl.col(f"{field}_reference"))
                    )
                    .cast(pl.Int64)
                    .sum()
                    .alias("count")
                ).item()
            )
        report.update(
            {
                "overlapping_records": overlap.height,
                "instrument_only_records": instrument_only.height,
                "reference_only_records": reference_only.height,
                "field_mismatch_counts": mismatch_counts,
            }
        )
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def build_options_ticker_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    bronze_dataset_type: str = "options_ticker_snapshot_1m",
    output_dataset_type: str = "options_ticker_snapshot_1m_observed",
    source: str | None = "rest_get_book_summary_by_currency",
    write_reconciliation: bool = False,
    dependencies: OptionsTickerDependencies,
) -> object:
    """Build observed options-ticker Silver snapshots for one currency."""

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
    agg_invalid_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = _bronze_month_files(root, month)
        if not files:
            continue
        frame = _collect_parquet_files(pl, files)
        rows_in = frame.height
        if rows_in == 0:
            continue
        name = pl.col("instrument_name").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
        frame = frame.with_columns(
            [
                pl.col("snapshot_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp"),
                pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
                pl.lit(normalized_symbol).alias("symbol"),
                name.alias("instrument_name"),
                name.str.extract(r"^([A-Z0-9]+)-[0-9]{1,2}[A-Z]{3}[0-9]{2}-[0-9.]+-[CP]$", 1).alias("underlying"),
                name.str.extract(r"^[A-Z0-9]+-([0-9]{1,2}[A-Z]{3}[0-9]{2})-[0-9.]+-[CP]$", 1)
                .str.strptime(pl.Date, "%d%b%y", strict=False)
                .alias("expiry"),
                name.str.extract(r"^[A-Z0-9]+-[0-9]{1,2}[A-Z]{3}[0-9]{2}-([0-9.]+)-[CP]$", 1)
                .cast(pl.Float64, strict=False)
                .alias("strike"),
                name.str.extract(r"^[A-Z0-9]+-[0-9]{1,2}[A-Z]{3}[0-9]{2}-[0-9.]+-([CP])$", 1).alias("option_type"),
                _column_or_null(pl, frame, "mark_price", pl.Float64).alias("mark_price"),
                _first_column_or_null(pl, frame, ("bid_price", "best_bid_price"), pl.Float64).alias("bid_price"),
                _first_column_or_null(pl, frame, ("ask_price", "best_ask_price"), pl.Float64).alias("ask_price"),
                _column_or_null(pl, frame, "mark_iv", pl.Float64).alias("implied_volatility"),
                _column_or_null(pl, frame, "delta", pl.Float64).alias("delta"),
                _column_or_null(pl, frame, "gamma", pl.Float64).alias("gamma"),
                _column_or_null(pl, frame, "vega", pl.Float64).alias("vega"),
                _column_or_null(pl, frame, "theta", pl.Float64).alias("theta"),
                _column_or_null(pl, frame, "open_interest", pl.Float64).alias("open_interest"),
                _column_or_null(pl, frame, "volume", pl.Float64).alias("volume"),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("ingested_at"),
                pl.col("source").cast(pl.Utf8).alias("source_endpoint"),
            ]
        )
        invalid_expr = (
            pl.col("timestamp").is_null()
            | pl.col("instrument_name").is_null()
            | pl.col("underlying").is_null()
            | pl.col("expiry").is_null()
            | pl.col("strike").is_null()
            | (pl.col("strike") <= 0.0)
            | (~pl.col("option_type").is_in(["C", "P"]))
        )
        invalid_rows = frame.select(invalid_expr.cast(pl.Int64).sum().alias("count")).item()
        cleaned = frame.filter(~invalid_expr)
        observed = (
            cleaned.sort(["timestamp", "ingested_at"])
            .unique(subset=["exchange", "instrument_name", "timestamp"], keep="last", maintain_order=True)
            .sort(["exchange", "instrument_name", "timestamp"])
            .select(SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS)
        )
        duplicates_removed = cleaned.height - observed.height
        target = dependencies.silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        observed.write_parquet(target)
        if write_reconciliation:
            reference_path = dependencies.silver_month_path(
                silver_root=silver_root,
                market="options_ticker_snapshot_1m_observed",
                exchange=exchange,
                symbol=normalized_symbol,
                timeframe=timeframe,
                month=month,
            )
            _write_reconciliation_report(
                pl=pl,
                observed=observed,
                reference_path=reference_path,
                target_path=target,
                exchange=exchange,
                symbol=normalized_symbol,
                month=month,
                output_dataset_type=output_dataset_type,
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
        columns=SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS,
    )


def build_options_instrument_ticker_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    dependencies: OptionsTickerDependencies,
) -> object:
    """Build observed Silver snapshots for instrument-level option ticker data."""

    return build_options_ticker_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        bronze_dataset_type="options_instrument_ticker_snapshot_1m",
        output_dataset_type="options_instrument_ticker_snapshot_1m_observed",
        source=None,
        write_reconciliation=True,
        dependencies=dependencies,
    )
