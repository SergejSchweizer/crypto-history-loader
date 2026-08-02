"""Build deterministic minute-level option-surface features from observed tickers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import (
    SILVER_OPTION_SURFACE_FEATURE_COLUMNS,
    SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS,
)
from application.services.silver_partition_manifest import (
    load_current_manifest,
    publish_partition_atomically,
    source_fingerprint,
)

ATM_LOG_MONEYNESS_LIMIT = 0.05
SHORT_DTE_DAYS = 7.0
LONG_DTE_DAYS = 30.0
FRESH_QUOTE_SECONDS = 60.0
_OPTION_SURFACE_FEATURE_CONTRACT_VERSION = "silver-options-surface-feature/v1"


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
class OptionsSurfaceDependencies:
    """Shared Silver helpers required by option-surface transformations."""

    require_polars: Any
    silver_month_path: Any
    iso_utc: Any
    report_factory: SilverReportFactory


def _dataset_root(*, silver_root: str, dataset_type: str, exchange: str, symbol: str, timeframe: str) -> Path:
    return (
        Path(silver_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )


def discover_options_surface_symbols(*, silver_root: str, exchange: str, timeframe: str = "1m") -> list[str]:
    """Discover symbols with at least one observed option-ticker source."""

    symbols: set[str] = set()
    for dataset_type in (
        "options_ticker_snapshot_1m_observed",
        "options_instrument_ticker_snapshot_1m_observed",
    ):
        root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
        if not root.exists():
            continue
        for path in root.glob(f"symbol=*/timeframe={timeframe}"):
            if path.parent.name.startswith("symbol="):
                symbols.add(path.parent.name.split("=", 1)[1].upper())
    return sorted(symbols)


def _months(root: Path) -> set[str]:
    if not root.exists():
        return set()
    return {path.name.split("=", 1)[1] for path in root.glob("year=*/month=*") if path.name.startswith("month=")}


def _month_file(root: Path, month: str, symbol: str) -> Path | None:
    path = root / f"year={month[:4]}" / f"month={month}" / f"{symbol}-{month}.parquet"
    if path.exists():
        return path
    files = sorted(path.parent.glob("*.parquet"))
    return files[0] if files else None


def _read_source(pl: Any, path: Path | None, priority: int) -> Any | None:
    if path is None:
        return None
    frame = pl.read_parquet(path)
    missing = [column for column in SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS if column not in frame.columns]
    if missing:
        frame = frame.with_columns([pl.lit(None).alias(column) for column in missing])
    return frame.select(SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS).with_columns(
        pl.lit(priority, dtype=pl.Int8).alias("_source_priority")
    )


def _surface_frame(pl: Any, frame: Any, normalized_symbol: str) -> tuple[Any, int]:
    rows_before_dedup = frame.height
    deduped = (
        frame.with_columns(
            [
                pl.col("timestamp").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
            ]
        )
        .sort(["exchange", "instrument_name", "timestamp", "_source_priority", "ingested_at"])
        .unique(subset=["exchange", "instrument_name", "timestamp"], keep="last", maintain_order=True)
        .with_columns(pl.col("timestamp").dt.truncate("1m").alias("timestamp_m1"))
        .sort(["exchange", "instrument_name", "timestamp_m1", "timestamp", "ingested_at"])
        .unique(subset=["exchange", "instrument_name", "timestamp_m1"], keep="last", maintain_order=True)
    )

    # Expiry and moneyness use only the contract snapshot available inside the
    # current closed minute. No future quote is carried backward into an older minute.
    prepared = deduped.with_columns(
        [
            (
                (pl.col("expiry").cast(pl.Datetime("us", "UTC")) - pl.col("timestamp")).dt.total_seconds() / 86400.0
            ).alias("_dte_days"),
            (pl.col("strike") / pl.col("underlying_price")).alias("_moneyness"),
            ((pl.col("ingested_at") - pl.col("timestamp")).dt.total_milliseconds().cast(pl.Float64) / 1000.0)
            .clip(lower_bound=0.0)
            .alias("_quote_age_seconds"),
        ]
    ).filter(
        pl.col("implied_volatility").is_not_null()
        & pl.col("implied_volatility").is_finite()
        & (pl.col("implied_volatility") >= 0.0)
        & pl.col("_dte_days").is_not_null()
        & (pl.col("_dte_days") > 0.0)
        & pl.col("_moneyness").is_not_null()
        & pl.col("_moneyness").is_finite()
        & (pl.col("_moneyness") > 0.0)
    )

    log_moneyness = pl.col("_moneyness").log().abs()
    atm = log_moneyness <= ATM_LOG_MONEYNESS_LIMIT
    short_atm = atm & (pl.col("_dte_days") <= SHORT_DTE_DAYS)
    long_atm = atm & (pl.col("_dte_days") > LONG_DTE_DAYS)
    put_wing = (pl.col("option_type") == "P") & (pl.col("_moneyness") >= 0.85) & (pl.col("_moneyness") < 0.95)
    call_wing = (pl.col("option_type") == "C") & (pl.col("_moneyness") > 1.05) & (pl.col("_moneyness") <= 1.15)
    quoted = (pl.col("bid_price") > 0.0) & (pl.col("ask_price") > 0.0) & (pl.col("ask_price") >= pl.col("bid_price"))
    fresh = pl.col("_quote_age_seconds") <= FRESH_QUOTE_SECONDS

    grouped = prepared.group_by(["timestamp_m1", "exchange"]).agg(
        [
            pl.col("implied_volatility").filter(atm).median().alias("atm_iv"),
            pl.col("implied_volatility").filter(short_atm).median().alias("short_dated_iv"),
            (
                pl.col("implied_volatility").filter(put_wing).median()
                - pl.col("implied_volatility").filter(call_wing).median()
            ).alias("skew"),
            (
                pl.col("implied_volatility").filter(long_atm).median()
                - pl.col("implied_volatility").filter(short_atm).median()
            ).alias("term_structure"),
            (
                pl.col("implied_volatility").filter(atm & (pl.col("option_type") == "P")).median()
                - pl.col("implied_volatility").filter(atm & (pl.col("option_type") == "C")).median()
            ).alias("put_call_iv_spread"),
            pl.col("instrument_name").n_unique().cast(pl.Int64).alias("contract_count"),
            fresh.cast(pl.Int64).sum().alias("fresh_quote_count"),
            (~fresh).cast(pl.Int64).sum().alias("stale_quote_count"),
            pl.col("_quote_age_seconds").max().alias("max_quote_age_seconds"),
            quoted.cast(pl.Float64).mean().alias("quote_coverage_ratio"),
        ]
    )
    feature = (
        grouped.with_columns(pl.lit(normalized_symbol).alias("symbol"))
        .sort(["exchange", "symbol", "timestamp_m1"])
        .select(SILVER_OPTION_SURFACE_FEATURE_COLUMNS)
    )
    return feature, rows_before_dedup - deduped.height


def build_options_surface_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    output_dataset_type: str = "options_surface_1m_feature",
    dependencies: OptionsSurfaceDependencies,
) -> object:
    """Build deterministic option-surface proxies for one underlying symbol."""

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    currency_root = _dataset_root(
        silver_root=silver_root,
        dataset_type="options_ticker_snapshot_1m_observed",
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
    )
    instrument_root = _dataset_root(
        silver_root=silver_root,
        dataset_type="options_instrument_ticker_snapshot_1m_observed",
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
    )
    months = sorted(_months(currency_root) | _months(instrument_root))
    rows_in = 0
    rows_out = 0
    duplicates_removed = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None
    processed_months: list[str] = []

    for month in months:
        currency_path = _month_file(currency_root, month, normalized_symbol)
        instrument_path = _month_file(instrument_root, month, normalized_symbol)
        source_paths = [path for path in (currency_path, instrument_path) if path is not None]
        source_schema = {
            path.relative_to(Path(silver_root)).as_posix(): dict(pl.scan_parquet(str(path)).collect_schema())
            for path in source_paths
        }
        fingerprint = source_fingerprint(
            bronze_root=Path(silver_root),
            source_files=[str(path) for path in source_paths],
            source_schema=source_schema,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            builder_contract_version=_OPTION_SURFACE_FEATURE_CONTRACT_VERSION,
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
            expected_builder_contract_version=_OPTION_SURFACE_FEATURE_CONTRACT_VERSION,
        )
        if cached is not None:
            processed_months.append(month)
            rows_out += cached.row_count
            continue
        currency = _read_source(pl, currency_path, priority=1)
        instrument = _read_source(pl, instrument_path, priority=2)
        sources = [source for source in (currency, instrument) if source is not None]
        if not sources:
            continue
        frame = pl.concat(sources, how="diagonal_relaxed")
        rows_in += frame.height
        feature, month_duplicates = _surface_frame(pl, frame, normalized_symbol)
        duplicates_removed += month_duplicates
        if feature.height == 0:
            continue
        publish_partition_atomically(
            frame=feature,
            parquet_path=target,
            input_fingerprint=fingerprint,
            source_schema=source_schema,
            sort_keys=("exchange", "symbol", "timestamp_m1"),
            deduplication_keys=("exchange", "symbol", "timestamp_m1"),
            builder_contract_version=_OPTION_SURFACE_FEATURE_CONTRACT_VERSION,
        )
        processed_months.append(month)
        rows_out += feature.height

        month_min = feature.select(pl.col("timestamp_m1").min()).item()
        month_max = feature.select(pl.col("timestamp_m1").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

    return dependencies.report_factory(
        dataset=output_dataset_type,
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
        period_start=processed_months[0] if processed_months else None,
        period_end=processed_months[-1] if processed_months else None,
        months_processed=processed_months,
        rows_in=rows_in,
        rows_out=rows_out,
        duplicates_removed=duplicates_removed,
        invalid_ohlc_rows=0,
        null_price_rows=0,
        min_timestamp=dependencies.iso_utc(min_timestamp),
        max_timestamp=dependencies.iso_utc(max_timestamp),
        symbols=[normalized_symbol],
        columns=SILVER_OPTION_SURFACE_FEATURE_COLUMNS,
    )
