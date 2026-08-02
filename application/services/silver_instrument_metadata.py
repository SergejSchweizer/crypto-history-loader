"""Build shared daily Silver views for option and futures instrument metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_INSTRUMENT_METADATA_OBSERVED_COLUMNS
from application.services.silver_partition_manifest import (
    load_current_manifest,
    publish_partition_atomically,
    source_fingerprint,
)

_INSTRUMENT_METADATA_CONTRACT_VERSION = "silver-instrument-metadata-observed/v1"


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
class InstrumentMetadataDependencies:
    """Shared Silver helpers required by instrument metadata transformations."""

    require_polars: Any
    silver_month_path: Any
    iso_utc: Any
    report_factory: SilverReportFactory


def _dataset_root(*, bronze_root: str, dataset_type: str, exchange: str) -> Path:
    return Path(bronze_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"


def _files(root: Path) -> list[Path]:
    return sorted(root.glob("year=*/month=*/date=*/hour=*/data.parquet"))


def discover_instrument_metadata_symbols(*, bronze_root: str, exchange: str, dataset_type: str) -> list[str]:
    """Discover base currencies represented in one metadata snapshot family."""

    files = _files(_dataset_root(bronze_root=bronze_root, dataset_type=dataset_type, exchange=exchange))
    if not files:
        return []
    pl = __import__("polars")
    symbols: set[str] = set()
    for path in files:
        values = pl.read_parquet(path, columns=["base_currency"])["base_currency"].drop_nulls()
        symbols.update(str(value).strip().upper() for value in values if str(value).strip())
    return sorted(symbols)


def _months(root: Path) -> list[str]:
    return sorted(
        {f"{path.parent.name.split('=', 1)[1]}-{path.name.split('=', 1)[1]}" for path in root.glob("year=*/month=*")}
    )


def _month_files(root: Path, month: str) -> list[str]:
    year, month_part = month.split("-", 1)
    return sorted(str(path) for path in root.glob(f"year={year}/month={month_part}/date=*/hour=*/data.parquet"))


def _collect_files(pl: Any, files: list[str]) -> Any:
    try:
        return pl.scan_parquet(files).collect()
    except Exception as exc:
        if exc.__class__.__name__ != "SchemaError":
            raise
    return pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")


def _column_or_null(pl: Any, frame: Any, column: str, dtype: Any) -> Any:
    if column in frame.columns:
        return pl.col(column).cast(dtype)
    return pl.lit(None, dtype=dtype)


def _normalized_frame(pl: Any, frame: Any, normalized_symbol: str) -> tuple[Any, int, int]:
    instrument_name = pl.col("instrument_name").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    kind = pl.col("kind").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    expiration = _column_or_null(pl, frame, "expiration_timestamp", pl.Datetime(time_unit="us", time_zone="UTC"))
    creation = _column_or_null(pl, frame, "creation_timestamp", pl.Datetime(time_unit="us", time_zone="UTC"))
    source_state = _column_or_null(pl, frame, "state", pl.Utf8).str.to_lowercase()
    active = pl.col("is_active").cast(pl.Boolean)
    normalized = frame.filter(
        pl.col("base_currency").cast(pl.Utf8).str.to_uppercase() == normalized_symbol
    ).with_columns(
        [
            pl.col("snapshot_date").cast(pl.Date),
            pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
            instrument_name.alias("instrument_name"),
            pl.lit(normalized_symbol).alias("symbol"),
            pl.when(kind == "option")
            .then(pl.lit("option"))
            .when(instrument_name.str.ends_with("-PERPETUAL"))
            .then(pl.lit("perp"))
            .otherwise(pl.lit("future"))
            .alias("instrument_type"),
            pl.col("base_currency").cast(pl.Utf8).str.to_uppercase(),
            pl.col("quote_currency").cast(pl.Utf8).str.to_uppercase(),
            pl.col("settlement_currency").cast(pl.Utf8).str.to_uppercase(),
            expiration.dt.date().alias("expiry"),
            _column_or_null(pl, frame, "strike", pl.Float64).alias("strike"),
            pl.when(kind == "option")
            .then(
                _column_or_null(pl, frame, "option_type", pl.Utf8)
                .str.to_lowercase()
                .replace({"call": "C", "put": "P", "c": "C", "p": "P"})
            )
            .otherwise(None)
            .alias("option_type"),
            pl.col("tick_size").cast(pl.Float64),
            pl.col("contract_size").cast(pl.Float64),
            _column_or_null(pl, frame, "min_trade_amount", pl.Float64).alias("min_trade_amount"),
            creation.alias("creation_timestamp"),
            active.alias("is_active"),
            (
                (creation.is_null() | (creation.dt.date() <= pl.col("snapshot_date")))
                & (expiration.is_null() | (expiration.dt.date() >= pl.col("snapshot_date")))
            ).alias("is_listed"),
            pl.coalesce(
                [
                    source_state,
                    pl.when(active).then(pl.lit("active")).otherwise(pl.lit("inactive")),
                ]
            ).alias("listing_state"),
            pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
            pl.col("source").cast(pl.Utf8).alias("source_endpoint"),
        ]
    )
    invalid = (
        pl.col("snapshot_date").is_null()
        | pl.col("instrument_name").is_null()
        | (pl.col("instrument_name").str.len_chars() == 0)
        | pl.col("instrument_type").is_null()
        | pl.col("base_currency").is_null()
        | pl.col("quote_currency").is_null()
        | pl.col("settlement_currency").is_null()
        | pl.col("tick_size").is_null()
        | (~pl.col("tick_size").is_finite())
        | (pl.col("tick_size") <= 0.0)
        | pl.col("contract_size").is_null()
        | (~pl.col("contract_size").is_finite())
        | (pl.col("contract_size") <= 0.0)
        | pl.col("is_active").is_null()
        | (
            (pl.col("instrument_type") == "option")
            & (
                pl.col("expiry").is_null()
                | pl.col("strike").is_null()
                | (pl.col("strike") <= 0.0)
                | (~pl.col("option_type").is_in(["C", "P"]))
            )
        )
        | ((pl.col("instrument_type") == "future") & pl.col("expiry").is_null())
    )
    invalid_rows = int(normalized.select(invalid.cast(pl.Int64).sum()).item())
    cleaned = normalized.filter(~invalid)
    observed = (
        cleaned.sort(["snapshot_date", "instrument_name", "ingested_at"])
        .unique(
            subset=["snapshot_date", "exchange", "instrument_name"],
            keep="last",
            maintain_order=True,
        )
        .sort(["snapshot_date", "instrument_name"])
        .select(SILVER_INSTRUMENT_METADATA_OBSERVED_COLUMNS)
    )
    return observed, invalid_rows, cleaned.height - observed.height


def build_instrument_metadata_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1d",
    bronze_dataset_type: str = "instrument_metadata_snapshot_daily",
    output_dataset_type: str = "instrument_metadata_snapshot_daily_observed",
    dependencies: InstrumentMetadataDependencies,
) -> object:
    """Build latest-valid daily instrument metadata rows for one base currency."""

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    root = _dataset_root(bronze_root=bronze_root, dataset_type=bronze_dataset_type, exchange=exchange)
    months = _months(root)
    rows_in = 0
    rows_out = 0
    duplicates_removed = 0
    invalid_rows = 0
    min_date: date | None = None
    max_date: date | None = None
    processed: list[str] = []

    for month in months:
        files = _month_files(root, month)
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
            builder_contract_version=_INSTRUMENT_METADATA_CONTRACT_VERSION,
        )
        cached = load_current_manifest(
            parquet_path=target,
            expected_input_fingerprint=fingerprint,
            expected_builder_contract_version=_INSTRUMENT_METADATA_CONTRACT_VERSION,
        )
        if cached is not None:
            processed.append(month)
            rows_out += cached.row_count
            continue
        frame = _collect_files(pl, files)
        filtered_rows = frame.filter(
            pl.col("base_currency").cast(pl.Utf8).str.to_uppercase() == normalized_symbol
        ).height
        rows_in += filtered_rows
        observed, month_invalid, month_duplicates = _normalized_frame(pl, frame, normalized_symbol)
        invalid_rows += month_invalid
        duplicates_removed += month_duplicates
        if observed.height == 0:
            continue
        publish_partition_atomically(
            frame=observed,
            parquet_path=target,
            input_fingerprint=fingerprint,
            source_schema=source_schema,
            sort_keys=("snapshot_date", "instrument_name"),
            deduplication_keys=("snapshot_date", "exchange", "instrument_name"),
            builder_contract_version=_INSTRUMENT_METADATA_CONTRACT_VERSION,
        )
        processed.append(month)
        rows_out += observed.height
        month_min = observed.select(pl.col("snapshot_date").min()).item()
        month_max = observed.select(pl.col("snapshot_date").max()).item()
        if isinstance(month_min, date) and (min_date is None or month_min < min_date):
            min_date = month_min
        if isinstance(month_max, date) and (max_date is None or month_max > max_date):
            max_date = month_max

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
        min_timestamp=min_date.isoformat() if min_date else None,
        max_timestamp=max_date.isoformat() if max_date else None,
        symbols=[normalized_symbol],
        columns=SILVER_INSTRUMENT_METADATA_OBSERVED_COLUMNS,
    )
