"""Silver transformation service for monthly outputs and symbol reports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from application.dataset_contracts import (
    SILVER_FUTURES_SUMMARY_FEATURE_COLUMNS as SILVER_FUTURES_SUMMARY_FEATURE_COLUMNS,
)
from application.dataset_contracts import (
    SILVER_FUTURES_SUMMARY_OBSERVED_COLUMNS as SILVER_FUTURES_SUMMARY_OBSERVED_COLUMNS,
)
from application.dataset_contracts import SILVER_INDEX_PRICE_FEATURE_COLUMNS as SILVER_INDEX_PRICE_FEATURE_COLUMNS
from application.dataset_contracts import SILVER_INDEX_PRICE_OBSERVED_COLUMNS as SILVER_INDEX_PRICE_OBSERVED_COLUMNS
from application.dataset_contracts import SILVER_IV_RV_FEATURE_COLUMNS as SILVER_IV_RV_FEATURE_COLUMNS
from application.dataset_contracts import SILVER_L2_FEATURE_COLUMNS as SILVER_L2_FEATURE_COLUMNS
from application.dataset_contracts import SILVER_L2_OBSERVED_COLUMNS as SILVER_L2_OBSERVED_COLUMNS
from application.dataset_contracts import (
    SILVER_OHLCV_COLUMNS,
    SILVER_TRADES_M1_FEATURE_COLUMNS,
    SILVER_TRADES_OBSERVED_COLUMNS,
)
from application.dataset_contracts import (
    SILVER_OPTION_SURFACE_FEATURE_COLUMNS as SILVER_OPTION_SURFACE_FEATURE_COLUMNS,
)
from application.dataset_contracts import (
    SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS as SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS,
)
from application.dataset_contracts import (
    SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS as SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS,
)
from application.dataset_contracts import (
    SILVER_RECENT_TRADE_SNAPSHOT_OBSERVED_COLUMNS as SILVER_RECENT_TRADE_SNAPSHOT_OBSERVED_COLUMNS,
)
from application.dataset_contracts import (
    SILVER_VOLATILITY_FEATURE_COLUMNS as SILVER_VOLATILITY_FEATURE_COLUMNS,
)
from application.dataset_contracts import (
    SILVER_VOLATILITY_OBSERVED_COLUMNS as SILVER_VOLATILITY_OBSERVED_COLUMNS,
)
from application.services import (
    silver_funding,
    silver_futures_summary,
    silver_index_price,
    silver_iv_rv,
    silver_l2,
    silver_open_interest,
    silver_options_surface,
    silver_options_ticker,
    silver_realized_volatility,
    silver_recent_trades,
    silver_trades,
    silver_volatility,
)

SILVER_FUNDING_FEATURE_COLUMNS = silver_funding.SILVER_FUNDING_FEATURE_COLUMNS
SILVER_FUNDING_OBSERVED_COLUMNS = silver_funding.SILVER_FUNDING_OBSERVED_COLUMNS
SILVER_OPEN_INTEREST_M1_FEATURE_COLUMNS = silver_open_interest.SILVER_OPEN_INTEREST_M1_FEATURE_COLUMNS
SILVER_OPEN_INTEREST_OBSERVED_COLUMNS = silver_open_interest.SILVER_OPEN_INTEREST_OBSERVED_COLUMNS
_build_trade_feature_frame = silver_trades.build_trade_feature_frame
_build_trade_observed_frame = silver_trades.build_trade_observed_frame
discover_volatility_snapshot_symbols = silver_volatility.discover_snapshot_symbols
discover_realized_volatility_symbols = silver_realized_volatility.discover_realized_volatility_symbols
discover_iv_rv_symbols = silver_iv_rv.discover_iv_rv_symbols
discover_index_price_symbols = silver_index_price.discover_index_price_symbols
discover_futures_summary_symbols = silver_futures_summary.discover_futures_summary_symbols
discover_options_ticker_symbols = silver_options_ticker.discover_options_ticker_symbols
discover_options_instrument_ticker_symbols = silver_options_ticker.discover_options_ticker_symbols
discover_options_surface_symbols = silver_options_surface.discover_options_surface_symbols
discover_l2_symbols = silver_l2.discover_l2_symbols
discover_recent_trade_symbols = silver_recent_trades.discover_recent_trade_symbols


def _funding_dependencies() -> silver_funding.FundingDependencies:
    return silver_funding.FundingDependencies(
        require_polars=_require_polars,
        discover_months=discover_months,
        bronze_month_files=_bronze_month_files,
        silver_month_path=_silver_month_path,
        silver_funding_feature_month_path=_silver_funding_feature_month_path,
        iso_utc=_iso_utc,
        report_factory=SilverBuildReport,
    )


def _open_interest_dependencies() -> silver_open_interest.OpenInterestDependencies:
    return silver_open_interest.OpenInterestDependencies(
        require_polars=_require_polars,
        discover_months=discover_months,
        bronze_month_files=_bronze_month_files,
        silver_month_path=_silver_month_path,
        silver_open_interest_feature_month_path=_silver_open_interest_feature_month_path,
        normalize_symbol_expr=_normalize_symbol_expr,
        iso_utc=_iso_utc,
        report_factory=SilverBuildReport,
    )


def _require_polars() -> Any:
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for silver-build. Install project dependencies.") from exc
    return pl


@dataclass(frozen=True)
class SilverBuildReport:
    """Aggregated silver build report for one symbol across processed months."""

    dataset: str
    exchange: str
    symbol: str
    timeframe: str
    period_start: str | None
    period_end: str | None
    months_processed: list[str]
    rows_in: int
    rows_out: int
    duplicates_removed: int
    invalid_ohlc_rows: int
    null_price_rows: int
    min_timestamp: str | None
    max_timestamp: str | None
    symbols: list[str]
    columns: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "months_processed": self.months_processed,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "duplicates_removed": self.duplicates_removed,
            "invalid_ohlc_rows": self.invalid_ohlc_rows,
            "null_price_rows": self.null_price_rows,
            "min_timestamp": self.min_timestamp,
            "max_timestamp": self.max_timestamp,
            "symbols": self.symbols,
            "columns": self.columns,
        }


def _silver_month_path(
    silver_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    month: str,
) -> Path:
    year = month.split("-", 1)[0]
    stem = f"{symbol}-{month}"
    return (
        Path(silver_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"year={year}"
        / f"month={month}"
        / f"{stem}.parquet"
    )


def _silver_funding_feature_month_path(
    silver_root: str,
    exchange: str,
    symbol: str,
    month: str,
) -> Path:
    year = month.split("-", 1)[0]
    stem = f"{symbol}-{month}"
    return (
        Path(silver_root)
        / "dataset_type=funding_1m_feature"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / f"year={year}"
        / f"month={month}"
        / f"{stem}.parquet"
    )


def _silver_open_interest_feature_month_path(
    silver_root: str,
    exchange: str,
    symbol: str,
    month: str,
) -> Path:
    year = month.split("-", 1)[0]
    stem = f"{symbol}-{month}"
    return (
        Path(silver_root)
        / "dataset_type=open_interest_1m_feature"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / f"year={year}"
        / f"month={month}"
        / f"{stem}.parquet"
    )


def _bronze_month_files(
    bronze_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    month: str,
    instrument_type: str | None = None,
) -> list[str]:
    instrument = instrument_type or ("perp" if market == "perps_ohlcv" else market)
    year = month.split("-", 1)[0]
    root = (
        Path(bronze_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    files = {
        *root.glob(f"year={year}/month={month}/date=*/data.parquet"),
        *root.glob(f"month={month}/date=*/data.parquet"),
    }
    return sorted(str(path) for path in files)


def discover_symbols(
    bronze_root: str,
    market: str,
    exchange: str,
    timeframe: str = "1m",
    instrument_type: str | None = None,
) -> list[str]:
    """Discover symbols available in bronze for selected market/exchange/timeframe."""

    instrument = instrument_type or ("perp" if market == "perps_ohlcv" else market)
    root = Path(bronze_root) / f"dataset_type={market}" / f"exchange={exchange}" / f"instrument_type={instrument}"
    if not root.exists():
        return []
    symbols: list[str] = []
    for path in root.glob("symbol=*/timeframe=*"):
        symbol_segment = path.parent.name
        tf_segment = path.name
        if not symbol_segment.startswith("symbol=") or not tf_segment.startswith("timeframe="):
            continue
        if tf_segment.split("=", 1)[1] != timeframe:
            continue
        symbols.append(symbol_segment.split("=", 1)[1])
    return sorted(set(symbols))


def discover_months(
    bronze_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    instrument_type: str | None = None,
) -> list[str]:
    """Discover available bronze months for one symbol."""

    instrument = instrument_type or ("perp" if market == "perps_ohlcv" else market)
    root = (
        Path(bronze_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not root.exists():
        return []
    months: set[str] = set()
    for path in root.glob("year=*/month=*"):
        name = path.name
        if name.startswith("month="):
            months.add(name.split("=", 1)[1])
    for path in root.glob("month=*"):
        name = path.name
        if name.startswith("month="):
            months.add(name.split("=", 1)[1])
    return sorted(months)


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_symbol_expr(pl: Any, col_name: str = "symbol") -> Any:
    return (
        pl.col(col_name)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all(r"[\s/]+", "-")
        .str.replace_all("_", "-")
    )


def build_silver_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build monthly silver parquet outputs and aggregated report for one symbol."""

    pl = _require_polars()
    months = discover_months(
        bronze_root=bronze_root,
        market=market,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    agg_invalid_ohlc_rows = 0
    agg_null_price_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = _bronze_month_files(
            bronze_root=bronze_root,
            market=market,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
        )
        if not files:
            continue
        frame = pl.scan_parquet(files).collect()
        rows_in = frame.height
        if rows_in == 0:
            continue

        null_price_expr = (
            pl.col("open_price").is_null()
            | pl.col("high_price").is_null()
            | pl.col("low_price").is_null()
            | pl.col("close_price").is_null()
        )
        invalid_ohlc_expr = (pl.col("high_price") < pl.max_horizontal("open_price", "close_price")) | (
            pl.col("low_price") > pl.min_horizontal("open_price", "close_price")
        )

        null_price_rows = frame.select(null_price_expr.cast(pl.Int64).sum().alias("count")).item()
        invalid_ohlc_rows = frame.select(
            (~null_price_expr & invalid_ohlc_expr).cast(pl.Int64).sum().alias("count")
        ).item()
        cleaned = frame.filter(~null_price_expr & ~invalid_ohlc_expr)
        deduped = (
            cleaned.sort(["open_time", "ingested_at"])
            .unique(
                subset=["exchange", "instrument_type", "symbol", "timeframe", "open_time"],
                keep="last",
                maintain_order=True,
            )
            .sort("open_time")
        )
        duplicates_removed = cleaned.height - deduped.height

        target = _silver_month_path(
            silver_root=silver_root,
            market=market,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        deduped.write_parquet(target)

        month_min = deduped.select(pl.col("open_time").min()).item()
        month_max = deduped.select(pl.col("open_time").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += rows_in
        agg_rows_out += deduped.height
        agg_duplicates_removed += int(duplicates_removed)
        agg_invalid_ohlc_rows += int(invalid_ohlc_rows)
        agg_null_price_rows += int(null_price_rows)

    report = SilverBuildReport(
        dataset=f"{market}_1m",
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        period_start=months[0] if months else None,
        period_end=months[-1] if months else None,
        months_processed=months,
        rows_in=agg_rows_in,
        rows_out=agg_rows_out,
        duplicates_removed=agg_duplicates_removed,
        invalid_ohlc_rows=agg_invalid_ohlc_rows,
        null_price_rows=agg_null_price_rows,
        min_timestamp=_iso_utc(min_timestamp),
        max_timestamp=_iso_utc(max_timestamp),
        symbols=[symbol],
        columns=SILVER_OHLCV_COLUMNS,
    )
    return report


def build_funding_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build monthly ``funding_observed`` silver outputs and aggregated report."""

    report = silver_funding.build_funding_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_funding_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("Funding observed builder returned an unexpected report type")
    return report


def build_funding_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    observed_timeframe: str = "8h",
    cutoff_time: datetime | None = None,
) -> SilverBuildReport:
    """Build monthly ``funding_1m_feature`` from ``funding_observed`` using backward asof joins.

    Args:
        silver_root: Path to silver layer root.
        exchange: Exchange identifier.
        symbol: Trading symbol.
        observed_timeframe: Funding observation timeframe (default: 8h).
        cutoff_time: Latest timestamp to include in calendar. Defaults to now (UTC).
            Prevents forward-carrying of funding data beyond current time.
    """

    report = silver_funding.build_funding_1m_feature_for_symbol(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        observed_timeframe=observed_timeframe,
        cutoff_time=cutoff_time,
        dependencies=_funding_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("Funding 1m feature builder returned an unexpected report type")
    return report


def build_open_interest_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build monthly ``open_interest_observed`` silver outputs from bronze Open Interest observations."""

    report = silver_open_interest.build_open_interest_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_open_interest_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("Open Interest observed builder returned an unexpected report type")
    return report


def build_open_interest_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    observed_timeframe: str = "1m",
    cutoff_time: datetime | None = None,
) -> SilverBuildReport:
    """Build monthly ``open_interest_1m_feature`` from ``open_interest_observed`` using backward asof join.

    Args:
        silver_root: Root path for silver datasets.
        exchange: Exchange identifier.
        symbol: Trading symbol.
        observed_timeframe: Input observation timeframe.
        cutoff_time: Latest timestamp to include in generated feature output.
            Defaults to now (UTC)."""

    report = silver_open_interest.build_open_interest_1m_feature_for_symbol(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        observed_timeframe=observed_timeframe,
        cutoff_time=cutoff_time,
        dependencies=_open_interest_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("Open Interest 1m feature builder returned an unexpected report type")
    return report


def build_perps_trades_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    observed_timeframe: str = "tick",
    observed_dataset_type: str = "perps_trades_observed",
    output_dataset_type: str = "perps_trades_1m_feature",
) -> SilverBuildReport:
    """Build monthly trade 1m features from observed tick-trade data."""

    pl = _require_polars()
    observed_root = (
        Path(silver_root)
        / f"dataset_type={observed_dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={observed_timeframe}"
    )
    months = sorted(
        {
            path.parent.name.split("=", 1)[1]
            for path in observed_root.glob("year=*/month=*/*.parquet")
            if path.parent.name.startswith("month=")
        }
    )
    agg_rows_in = 0
    agg_rows_out = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        year = month.split("-", 1)[0]
        month_file = observed_root / f"year={year}" / f"month={month}" / f"{symbol}-{month}.parquet"
        if not month_file.exists():
            continue
        frame = pl.read_parquet(month_file).sort("trade_time")
        rows_in = frame.height
        if rows_in == 0:
            continue

        feature = _build_trade_feature_frame(pl, frame, symbol=symbol)

        target = _silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=symbol,
            timeframe="1m",
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        feature.write_parquet(target)

        month_min = feature.select(pl.col("timestamp_m1").min()).item()
        month_max = feature.select(pl.col("timestamp_m1").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += rows_in
        agg_rows_out += feature.height

    return SilverBuildReport(
        dataset=output_dataset_type,
        exchange=exchange,
        symbol=symbol,
        timeframe="1m",
        period_start=months[0] if months else None,
        period_end=months[-1] if months else None,
        months_processed=months,
        rows_in=agg_rows_in,
        rows_out=agg_rows_out,
        duplicates_removed=0,
        invalid_ohlc_rows=0,
        null_price_rows=0,
        min_timestamp=_iso_utc(min_timestamp),
        max_timestamp=_iso_utc(max_timestamp),
        symbols=[symbol],
        columns=SILVER_TRADES_M1_FEATURE_COLUMNS,
    )


def build_perps_trades_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    instrument_type: str = "perp",
    timeframe: str = "tick",
    bronze_dataset_type: str = "perps_trades",
    output_dataset_type: str = "perps_trades_observed",
) -> SilverBuildReport:
    """Build monthly observed tick-trade dataset from bronze trade records."""

    pl = _require_polars()
    months = discover_months(
        bronze_root=bronze_root,
        market=bronze_dataset_type,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        instrument_type=instrument_type,
    )
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    agg_invalid_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = _bronze_month_files(
            bronze_root=bronze_root,
            market=bronze_dataset_type,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
            instrument_type=instrument_type,
        )
        if not files:
            continue
        frame = pl.scan_parquet(files).collect()
        rows_in = frame.height
        if rows_in == 0:
            continue
        observed, invalid_rows, cleaned_rows = _build_trade_observed_frame(pl, frame)
        duplicates_removed = cleaned_rows - observed.height
        target = _silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        observed.write_parquet(target)

        month_min = observed.select(pl.col("trade_time").min()).item()
        month_max = observed.select(pl.col("trade_time").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += rows_in
        agg_rows_out += observed.height
        agg_duplicates_removed += int(duplicates_removed)
        agg_invalid_rows += int(invalid_rows)

    return SilverBuildReport(
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
        min_timestamp=_iso_utc(min_timestamp),
        max_timestamp=_iso_utc(max_timestamp),
        symbols=[symbol],
        columns=SILVER_TRADES_OBSERVED_COLUMNS,
    )


def build_volatility_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    bronze_dataset_type: str,
    output_dataset_type: str,
) -> SilverBuildReport:
    """Build monthly volatility-observed silver outputs from bronze volatility datasets."""

    report = silver_volatility.build_volatility_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        bronze_dataset_type=bronze_dataset_type,
        output_dataset_type=output_dataset_type,
        dependencies=silver_volatility.VolatilityObservedDependencies(
            require_polars=_require_polars,
            discover_months=discover_months,
            bronze_month_files=_bronze_month_files,
            silver_month_path=_silver_month_path,
            normalize_symbol_expr=_normalize_symbol_expr,
            iso_utc=_iso_utc,
            report_factory=SilverBuildReport,
        ),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("volatility observed builder returned an unexpected report type")
    return report


def build_volatility_snapshot_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    bronze_dataset_type: str = "volatility_index_snapshot_1m",
    output_dataset_type: str = "volatility_index_snapshot_1m_observed",
    source: str = "rest_get_volatility_index_data",
) -> SilverBuildReport:
    """Build monthly snapshot volatility-observed silver outputs from live-loader bronze files."""

    report = silver_volatility.build_volatility_snapshot_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        bronze_dataset_type=bronze_dataset_type,
        output_dataset_type=output_dataset_type,
        source=source,
        dependencies=silver_volatility.VolatilityObservedDependencies(
            require_polars=_require_polars,
            discover_months=discover_months,
            bronze_month_files=_bronze_month_files,
            silver_month_path=_silver_month_path,
            normalize_symbol_expr=_normalize_symbol_expr,
            iso_utc=_iso_utc,
            report_factory=SilverBuildReport,
        ),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("volatility snapshot observed builder returned an unexpected report type")
    return report


def build_volatility_index_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build canonical IV 1m features from snapshot observations and historical fallback."""

    report = silver_volatility.build_volatility_index_1m_feature_for_symbol(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=silver_volatility.VolatilityFeatureDependencies(
            require_polars=_require_polars,
            silver_month_path=_silver_month_path,
            iso_utc=_iso_utc,
            report_factory=SilverBuildReport,
        ),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("volatility 1m feature builder returned an unexpected report type")
    return report


def build_realized_volatility_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build OHLCV-derived realized-volatility 1m features for one base symbol."""

    report = silver_realized_volatility.build_realized_volatility_1m_feature_for_symbol(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=silver_realized_volatility.RealizedVolatilityDependencies(
            require_polars=_require_polars,
            silver_month_path=_silver_month_path,
            iso_utc=_iso_utc,
            report_factory=SilverBuildReport,
        ),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("realized volatility 1m feature builder returned an unexpected report type")
    return report


def build_iv_rv_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build direct IV/RV 1m state features for one symbol."""

    report = silver_iv_rv.build_iv_rv_1m_feature_for_symbol(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=silver_iv_rv.IvRvDependencies(
            require_polars=_require_polars,
            silver_month_path=_silver_month_path,
            iso_utc=_iso_utc,
            report_factory=SilverBuildReport,
        ),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("IV/RV 1m feature builder returned an unexpected report type")
    return report


def _index_price_dependencies() -> silver_index_price.IndexPriceDependencies:
    return silver_index_price.IndexPriceDependencies(
        require_polars=_require_polars,
        silver_month_path=_silver_month_path,
        iso_utc=_iso_utc,
        report_factory=SilverBuildReport,
    )


def build_index_price_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build observed index-price Silver snapshots for one symbol."""

    report = silver_index_price.build_index_price_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_index_price_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("index-price observed builder returned an unexpected report type")
    return report


def build_index_price_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build minute-grid index-price features for one symbol."""

    report = silver_index_price.build_index_price_1m_feature_for_symbol(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_index_price_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("index-price 1m feature builder returned an unexpected report type")
    return report


def _futures_summary_dependencies() -> silver_futures_summary.FuturesSummaryDependencies:
    return silver_futures_summary.FuturesSummaryDependencies(
        require_polars=_require_polars,
        silver_month_path=_silver_month_path,
        iso_utc=_iso_utc,
        report_factory=SilverBuildReport,
    )


def build_futures_summary_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build observed futures-summary Silver snapshots for one currency."""

    report = silver_futures_summary.build_futures_summary_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_futures_summary_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("futures-summary observed builder returned an unexpected report type")
    return report


def build_futures_summary_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build freshness-aware futures-summary 1m features for one currency."""

    report = silver_futures_summary.build_futures_summary_1m_feature_for_symbol(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_futures_summary_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("futures-summary 1m feature builder returned an unexpected report type")
    return report


def _options_ticker_dependencies() -> silver_options_ticker.OptionsTickerDependencies:
    return silver_options_ticker.OptionsTickerDependencies(
        require_polars=_require_polars,
        silver_month_path=_silver_month_path,
        iso_utc=_iso_utc,
        report_factory=SilverBuildReport,
    )


def build_options_ticker_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build observed options-ticker Silver snapshots for one currency."""

    report = silver_options_ticker.build_options_ticker_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_options_ticker_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("options-ticker observed builder returned an unexpected report type")
    return report


def build_options_instrument_ticker_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build observed options-instrument-ticker Silver snapshots for one currency."""

    report = silver_options_ticker.build_options_instrument_ticker_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_options_ticker_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("options-instrument-ticker observed builder returned an unexpected report type")
    return report


def _options_surface_dependencies() -> silver_options_surface.OptionsSurfaceDependencies:
    return silver_options_surface.OptionsSurfaceDependencies(
        require_polars=_require_polars,
        silver_month_path=_silver_month_path,
        iso_utc=_iso_utc,
        report_factory=SilverBuildReport,
    )


def build_options_surface_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build minute-level option-surface features for one currency."""

    report = silver_options_surface.build_options_surface_1m_feature_for_symbol(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_options_surface_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("options-surface 1m feature builder returned an unexpected report type")
    return report


def _l2_dependencies() -> silver_l2.L2Dependencies:
    return silver_l2.L2Dependencies(
        require_polars=_require_polars,
        silver_month_path=_silver_month_path,
        iso_utc=_iso_utc,
        report_factory=SilverBuildReport,
    )


def build_perps_l2_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build validated observed perpetual L2 snapshots for one symbol."""

    report = silver_l2.build_l2_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_l2_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("perps-L2 observed builder returned an unexpected report type")
    return report


def build_perps_l2_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build minute-level perpetual L2 liquidity features for one symbol."""

    report = silver_l2.build_l2_1m_feature_for_symbol(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_l2_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("perps-L2 feature builder returned an unexpected report type")
    return report


def build_options_l2_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build validated observed options L2 snapshots for one currency."""

    report = silver_l2.build_l2_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        bronze_dataset_type="options_l2_snapshot_1m",
        output_dataset_type="options_l2_snapshot_1m_observed",
        instrument_type="option",
        dependencies=_l2_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("options-L2 observed builder returned an unexpected report type")
    return report


def build_options_l2_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build contract-level options L2 liquidity features for one currency."""

    report = silver_l2.build_l2_1m_feature_for_symbol(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        observed_dataset_type="options_l2_snapshot_1m_observed",
        output_dataset_type="options_l2_1m_feature",
        dependencies=_l2_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("options-L2 feature builder returned an unexpected report type")
    return report


def _recent_trade_dependencies() -> silver_recent_trades.RecentTradeDependencies:
    return silver_recent_trades.RecentTradeDependencies(
        require_polars=_require_polars,
        silver_month_path=_silver_month_path,
        iso_utc=_iso_utc,
        report_factory=SilverBuildReport,
    )


def build_recent_trade_snapshot_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "tick",
) -> SilverBuildReport:
    """Build snapshot-derived observed trades for one currency."""

    report = silver_recent_trades.build_recent_trade_snapshot_observed_for_symbol(
        bronze_root=bronze_root,
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        dependencies=_recent_trade_dependencies(),
    )
    if not isinstance(report, SilverBuildReport):
        raise TypeError("recent-trade snapshot builder returned an unexpected report type")
    return report
