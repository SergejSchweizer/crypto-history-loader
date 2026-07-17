"""Silver feature builder for OHLCV-derived realized volatility."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS

# QC-01: canonical annualization basis for crypto calendar-time volatility (365
# calendar days per year, expressed in minutes) shared by every annualized RV field.
_ANNUALIZATION_MINUTES_PER_YEAR = 365 * 24 * 60

# QC-01: raw RV window -> window length in minutes, used to scale each raw
# (non-annualized) RV window into an annualized volatility percentage point.
_RV_WINDOW_MINUTES = {
    "rv_5m": 5,
    "rv_15m": 15,
    "rv_1h": 60,
    "rv_4h": 240,
    "rv_1d": 1440,
    "rv_30d": 30 * 1440,
}


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
class RealizedVolatilityDependencies:
    """Shared Silver helpers required by realized-volatility transformations."""

    require_polars: Any
    silver_month_path: Any
    iso_utc: Any
    report_factory: SilverReportFactory


def _normalize_base_symbol(value: str) -> str:
    normalized = value.strip().upper().replace("_", "-").replace("/", "-")
    parts = [part for part in normalized.split("-") if part]
    if parts:
        return parts[0]
    return normalized


def discover_realized_volatility_symbols(
    *,
    silver_root: str,
    exchange: str,
    timeframe: str = "1m",
) -> list[str]:
    """Discover base symbols that have spot or perpetual OHLCV Silver inputs."""

    symbols: set[str] = set()
    for dataset_type in ("spot_ohlcv", "perps_ohlcv", "perp"):
        root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
        if not root.exists():
            continue
        for path in root.glob(f"symbol=*/timeframe={timeframe}"):
            symbol_segment = path.parent.name
            if symbol_segment.startswith("symbol="):
                symbols.add(_normalize_base_symbol(symbol_segment.split("=", 1)[1]))
    return sorted(symbols)


def _matching_symbol_dirs(
    *,
    silver_root: str,
    dataset_type: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[Path]:
    root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
    if not root.exists():
        return []
    matches: list[Path] = []
    for path in root.glob(f"symbol=*/timeframe={timeframe}"):
        symbol_segment = path.parent.name
        if not symbol_segment.startswith("symbol="):
            continue
        if _normalize_base_symbol(symbol_segment.split("=", 1)[1]) == symbol:
            matches.append(path)
    return sorted(matches)


def _month_file(path: Path, month: str) -> Path | None:
    year = month.split("-", 1)[0]
    files = sorted((path / f"year={year}" / f"month={month}").glob("*.parquet"))
    return files[0] if files else None


def _source_months(paths: list[Path]) -> set[str]:
    months: set[str] = set()
    for path in paths:
        for month_dir in path.glob("year=*/month=*"):
            if month_dir.name.startswith("month="):
                months.add(month_dir.name.split("=", 1)[1])
    return months


def _prepare_ohlcv_source(pl: Any, frame: Any, *, symbol: str, prefix: str) -> Any:
    return (
        frame.with_columns(
            [
                pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp_m1"),
                pl.lit(symbol).alias("symbol"),
                pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("open_price").cast(pl.Float64).alias(f"{prefix}_open"),
                pl.col("high_price").cast(pl.Float64).alias(f"{prefix}_high"),
                pl.col("low_price").cast(pl.Float64).alias(f"{prefix}_low"),
                pl.col("close_price").cast(pl.Float64).alias(f"{prefix}_close"),
            ]
        )
        .sort(["exchange", "symbol", "timestamp_m1"])
    )


def _read_source_month(pl: Any, paths: list[Path], *, month: str, symbol: str, prefix: str) -> Any | None:
    frames = []
    for path in paths:
        month_file = _month_file(path, month)
        if month_file is not None:
            frames.append(_prepare_ohlcv_source(pl, pl.read_parquet(month_file), symbol=symbol, prefix=prefix))
    if not frames:
        return None
    return pl.concat(frames, how="vertical_relaxed").unique(
        subset=["exchange", "symbol", "timestamp_m1"],
        keep="last",
        maintain_order=True,
    )


def _rolling_rv_expr(pl: Any, window_size: str) -> Any:
    squared_returns = pl.col("log_return").pow(2)
    return (
        pl.when(pl.col("log_return").is_not_null())
        .then(
            squared_returns.rolling_sum_by(
                "timestamp_m1",
                window_size=window_size,
                min_samples=1,
            )
            .over(["exchange", "symbol"])
            .sqrt()
        )
        .otherwise(None)
    )


def _rolling_zscore_expr(pl: Any, column: str, window_size: str) -> Any:
    rolling_mean = (
        pl.col(column)
        .rolling_mean_by("timestamp_m1", window_size=window_size, min_samples=2)
        .over(["exchange", "symbol"])
    )
    rolling_std = (
        pl.col(column)
        .rolling_std_by("timestamp_m1", window_size=window_size, min_samples=2)
        .over(["exchange", "symbol"])
    )
    return pl.when(rolling_std > 0.0).then((pl.col(column) - rolling_mean) / rolling_std).otherwise(None)


def _annualized_pct_expr(pl: Any, raw_column: str, window_minutes: int) -> Any:
    """Scale a raw (non-annualized) RV window into an annualized percentage point.

    QC-01: ``raw_column`` holds ``sqrt(sum(log_return^2))`` over ``window_minutes``
    of 1-minute observations, a non-annualized decimal volatility estimate. Scaling
    by ``sqrt(minutes_per_year / window_minutes)`` converts it to the same
    annualization basis and unit (percentage points) as the implied-volatility
    index, so it can be safely subtracted from or divided into IV values.
    """

    scale = (_ANNUALIZATION_MINUTES_PER_YEAR / window_minutes) ** 0.5 * 100.0
    return pl.col(raw_column) * scale


def build_realized_volatility_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    output_dataset_type: str = "realized_volatility_1m_feature",
    dependencies: RealizedVolatilityDependencies,
) -> object:
    """Build OHLCV-derived realized-volatility features for one base symbol.

    Args:
        silver_root: Root directory for Silver input and output parquet files.
        exchange: Exchange partition value.
        symbol: Base asset symbol, for example ``BTC``.
        timeframe: Input and output timeframe.
        output_dataset_type: Target feature dataset type.
        dependencies: Shared Silver helper functions supplied by the orchestration service.

    Returns:
        A Silver build report object created by ``dependencies.report_factory``.
    """

    pl = dependencies.require_polars()
    normalized_symbol = _normalize_base_symbol(symbol)
    spot_paths = _matching_symbol_dirs(
        silver_root=silver_root,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
    )
    perps_paths = _matching_symbol_dirs(
        silver_root=silver_root,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
    ) or _matching_symbol_dirs(
        silver_root=silver_root,
        dataset_type="perp",
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
    )
    months = sorted(_source_months(spot_paths) | _source_months(perps_paths))
    agg_rows_in = 0
    agg_rows_out = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        spot = _read_source_month(pl, spot_paths, month=month, symbol=normalized_symbol, prefix="spot")
        perps = _read_source_month(pl, perps_paths, month=month, symbol=normalized_symbol, prefix="perps")
        inputs = [frame for frame in (spot, perps) if frame is not None]
        if not inputs:
            continue
        rows_in = sum(frame.height for frame in inputs)
        if spot is None:
            frame = perps
        elif perps is None:
            frame = spot
        else:
            frame = spot.join(perps, on=["timestamp_m1", "exchange", "symbol"], how="full", coalesce=True)
        if frame is None:
            continue
        for column_name in (
            "spot_open",
            "spot_high",
            "spot_low",
            "spot_close",
            "perps_open",
            "perps_high",
            "perps_low",
            "perps_close",
        ):
            if column_name not in frame.columns:
                frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias(column_name))
        frame = frame.with_columns(
            [
                pl.coalesce([pl.col("perps_open"), pl.col("spot_open")]).alias("rv_open"),
                pl.coalesce([pl.col("perps_high"), pl.col("spot_high")]).alias("rv_high"),
                pl.coalesce([pl.col("perps_low"), pl.col("spot_low")]).alias("rv_low"),
                pl.coalesce([pl.col("perps_close"), pl.col("spot_close")]).alias("rv_close"),
                pl.col("spot_close").is_not_null().alias("spot_available"),
                pl.col("perps_close").is_not_null().alias("perps_available"),
            ]
        ).sort(["exchange", "symbol", "timestamp_m1"])
        previous_close = pl.col("rv_close").shift(1).over(["exchange", "symbol"])
        frame = (
            frame.with_columns(
                pl.when((pl.col("rv_close") > 0.0) & (previous_close > 0.0))
                .then((pl.col("rv_close") / previous_close).log())
                .otherwise(None)
                .alias("log_return")
            )
            .with_columns(
                [
                    _rolling_rv_expr(pl, "5m").alias("rv_5m"),
                    _rolling_rv_expr(pl, "15m").alias("rv_15m"),
                    _rolling_rv_expr(pl, "1h").alias("rv_1h"),
                    _rolling_rv_expr(pl, "4h").alias("rv_4h"),
                    _rolling_rv_expr(pl, "1d").alias("rv_1d"),
                    _rolling_rv_expr(pl, "30d").alias("rv_30d"),
                    (
                        (
                            (pl.col("rv_high") / pl.col("rv_low"))
                            .log()
                            .pow(2)
                            .rolling_sum_by(
                                "timestamp_m1",
                                window_size="1h",
                                min_samples=1,
                            )
                        )
                        / (4.0 * 0.6931471805599453)
                    )
                    .over(["exchange", "symbol"])
                    .sqrt()
                    .alias("parkinson_rv_1h"),
                    _rolling_zscore_expr(pl, "log_return", "1d").abs().alias("jump_proxy"),
                ]
            )
            .with_columns(
                [
                    _annualized_pct_expr(pl, raw_column, window_minutes).alias(f"{raw_column}_annualized_pct")
                    for raw_column, window_minutes in _RV_WINDOW_MINUTES.items()
                ]
            )
        )
        feature = frame.select(SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS)

        target = dependencies.silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
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
        columns=SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS,
    )
