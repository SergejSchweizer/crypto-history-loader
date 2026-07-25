"""Silver historical prediction features from repository-native market datasets."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from application.dataset_contracts import SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS

_SOURCE_FLOAT_COLUMNS = (
    "spot_close_price",
    "perps_close_price",
    "funding_rate_last_known",
    "open_interest",
    "perps_buy_volume",
    "perps_sell_volume",
    "perps_trade_count",
    "perps_quote_volume",
    "options_buy_volume",
    "options_sell_volume",
    "options_trade_count",
    "options_quote_volume",
)


@dataclass(frozen=True)
class HistoricalPredictionDependencies:
    """Shared Silver helpers required by historical prediction transformations."""

    require_polars: Callable[[], Any]
    silver_month_path: Callable[..., Path]
    iso_utc: Callable[[datetime | None], str | None]
    report_factory: Callable[..., object]


def _normalize_base_symbol(value: str) -> str:
    """Normalize Silver symbol variants to the base asset used by Gold joins."""

    upper = value.strip().upper().replace("_", "-").replace("/", "-")
    parts = [part for part in upper.split("-") if part]
    if parts:
        return parts[0]
    return upper


def _source_files(*, silver_root: str, dataset_type: str, exchange: str, symbol: str, timeframe: str) -> list[Path]:
    """Return Silver parquet files for symbol-equivalent source directories."""

    wanted = _normalize_base_symbol(symbol)
    root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
    if not root.exists():
        return []
    files: list[Path] = []
    for timeframe_dir in root.glob("symbol=*/timeframe=*"):
        symbol_segment = timeframe_dir.parent.name
        timeframe_segment = timeframe_dir.name
        if not symbol_segment.startswith("symbol=") or timeframe_segment != f"timeframe={timeframe}":
            continue
        candidate = symbol_segment.split("=", 1)[1]
        if _normalize_base_symbol(candidate) != wanted:
            continue
        files.extend(sorted(timeframe_dir.glob("**/*.parquet")))
    return sorted(set(files))


def discover_historical_prediction_symbols(*, silver_root: str, exchange: str, timeframe: str = "1m") -> list[str]:
    """Discover symbols with enough historical Silver sources for predictive features."""

    required_sources = ("spot_ohlcv", "perps_ohlcv")
    discovered: set[str] | None = None
    for dataset_type in required_sources:
        root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
        symbols: set[str] = set()
        if root.exists():
            for path in root.glob("symbol=*/timeframe=*"):
                if path.name == f"timeframe={timeframe}" and path.parent.name.startswith("symbol="):
                    symbols.add(_normalize_base_symbol(path.parent.name.split("=", 1)[1]))
        discovered = symbols if discovered is None else discovered.intersection(symbols)
    return sorted(discovered or set())


def _read_source(
    pl: Any,
    *,
    silver_root: str,
    dataset_type: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    timestamp_column: str,
    columns: list[Any],
) -> Any | None:
    files = _source_files(
        silver_root=silver_root,
        dataset_type=dataset_type,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
    if not files:
        return None
    return (
        pl.scan_parquet([str(path) for path in files])
        .with_columns(
            [
                pl.col(timestamp_column)
                .cast(pl.Datetime(time_unit="us", time_zone="UTC"))
                .dt.truncate("1m")
                .alias("timestamp_m1"),
                pl.col("exchange").cast(pl.Utf8).str.to_lowercase().alias("exchange"),
                pl.lit(_normalize_base_symbol(symbol)).alias("symbol"),
            ]
        )
        .select(["timestamp_m1", "exchange", "symbol", *columns])
        .collect()
        .unique(subset=["timestamp_m1", "exchange", "symbol"], keep="last", maintain_order=True)
        .sort("timestamp_m1")
    )


def _rolling_zscore(pl: Any, column: str, window_size: int, output: str) -> Any:
    mean_col = pl.col(column).rolling_mean(window_size=window_size, min_samples=2)
    std_col = pl.col(column).rolling_std(window_size=window_size, min_samples=2)
    return pl.when(std_col > 0.0).then((pl.col(column) - mean_col) / std_col).otherwise(None).alias(output)


def _rv_expr(pl: Any, column: str, window_size: int, output: str) -> Any:
    return (pl.col(column).pow(2).rolling_sum(window_size=window_size, min_samples=2).sqrt()).alias(output)


def _safe_ratio(pl: Any, numerator: Any, denominator: Any) -> Any:
    return pl.when(denominator.abs() > 0.0).then(numerator / denominator).otherwise(None)


def _merge_sources(pl: Any, sources: list[Any | None]) -> Any:
    frames = [source for source in sources if source is not None and source.height > 0]
    if not frames:
        return pl.DataFrame(schema={column: pl.Null for column in SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS})
    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.join(frame, on=["timestamp_m1", "exchange", "symbol"], how="full", coalesce=True)
    return merged.sort("timestamp_m1")


def _build_feature_frame(pl: Any, frame: Any) -> Any:
    """Build trailing historical prediction features without IV or volatility-index inputs."""

    for column in _SOURCE_FLOAT_COLUMNS:
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))

    with_returns = frame.with_columns(
        [
            (pl.col("spot_close_price") / pl.col("spot_close_price").shift(1)).log().alias("_spot_log_return"),
            (pl.col("perps_close_price") / pl.col("perps_close_price").shift(1)).log().alias("_perps_log_return"),
            (_safe_ratio(pl, pl.col("perps_close_price"), pl.col("spot_close_price")) - 1.0).alias("_basis"),
            _safe_ratio(
                pl,
                pl.col("perps_buy_volume") - pl.col("perps_sell_volume"),
                pl.col("perps_buy_volume") + pl.col("perps_sell_volume"),
            ).alias("_perps_trade_imbalance"),
            _safe_ratio(
                pl,
                pl.col("options_buy_volume") - pl.col("options_sell_volume"),
                pl.col("options_buy_volume") + pl.col("options_sell_volume"),
            ).alias("_options_trade_imbalance"),
        ]
    )
    feature = with_returns.with_columns(
        [
            pl.col("_spot_log_return").alias("historical_prediction_spot_log_return_1m"),
            pl.col("_perps_log_return").alias("historical_prediction_perps_log_return_1m"),
            _rv_expr(pl, "_spot_log_return", 15, "historical_prediction_spot_rv_15m"),
            _rv_expr(pl, "_spot_log_return", 60, "historical_prediction_spot_rv_1h"),
            _rv_expr(pl, "_spot_log_return", 1440, "historical_prediction_spot_rv_1d"),
            _rv_expr(pl, "_perps_log_return", 15, "historical_prediction_perps_rv_15m"),
            _rv_expr(pl, "_perps_log_return", 60, "historical_prediction_perps_rv_1h"),
            _rv_expr(pl, "_perps_log_return", 1440, "historical_prediction_perps_rv_1d"),
            pl.col("_basis").alias("historical_prediction_spot_perp_basis"),
            (pl.col("_basis") - pl.col("_basis").shift(1)).alias("historical_prediction_basis_change_1m"),
            _rolling_zscore(pl, "_basis", 60, "historical_prediction_basis_zscore_1h"),
            (pl.col("open_interest") - pl.col("open_interest").shift(1)).alias(
                "historical_prediction_open_interest_delta_1m"
            ),
            _safe_ratio(
                pl,
                pl.col("open_interest") - pl.col("open_interest").shift(1),
                pl.col("open_interest").shift(1),
            ).alias("historical_prediction_open_interest_pct_change_1m"),
            _rolling_zscore(pl, "open_interest", 60, "historical_prediction_open_interest_zscore_1h"),
            (pl.col("funding_rate_last_known") - pl.col("funding_rate_last_known").shift(1)).alias(
                "historical_prediction_funding_rate_change_1m"
            ),
            _rolling_zscore(pl, "funding_rate_last_known", 1440, "historical_prediction_funding_rate_zscore_1d"),
            (pl.col("funding_rate_last_known") - pl.col("_basis")).alias(
                "historical_prediction_funding_basis_divergence"
            ),
            pl.col("_perps_trade_imbalance").alias("historical_prediction_perps_trade_imbalance"),
            _rolling_zscore(pl, "perps_trade_count", 60, "historical_prediction_perps_trade_count_zscore_1h"),
            _rolling_zscore(pl, "perps_quote_volume", 60, "historical_prediction_perps_quote_volume_zscore_1h"),
            _safe_ratio(pl, pl.col("_perps_log_return").abs(), pl.col("perps_quote_volume")).alias(
                "historical_prediction_perps_price_impact_1m"
            ),
            pl.col("_options_trade_imbalance").alias("historical_prediction_options_trade_imbalance"),
            _rolling_zscore(pl, "options_trade_count", 60, "historical_prediction_options_trade_count_zscore_1h"),
            _rolling_zscore(pl, "options_quote_volume", 60, "historical_prediction_options_quote_volume_zscore_1h"),
            ((pl.col("_perps_log_return") > 0.0) & (pl.col("open_interest") > pl.col("open_interest").shift(1)))
            .cast(pl.Int64)
            .alias("historical_prediction_leverage_build_up_signal"),
            ((pl.col("_perps_log_return") < 0.0) & (pl.col("open_interest") > pl.col("open_interest").shift(1)))
            .cast(pl.Int64)
            .alias("historical_prediction_short_stress_signal"),
        ]
    )
    return (
        feature.with_columns(
            (
                pl.col("historical_prediction_perps_quote_volume_zscore_1h").fill_null(0.0)
                + pl.col("historical_prediction_options_quote_volume_zscore_1h").fill_null(0.0)
                + pl.col("historical_prediction_open_interest_zscore_1h").fill_null(0.0)
            ).alias("historical_prediction_flow_volatility_pressure")
        )
        .select(SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS)
        .sort("timestamp_m1")
    )


def build_historical_prediction_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    output_dataset_type: str = "historical_prediction_1m_feature",
    dependencies: HistoricalPredictionDependencies,
) -> object:
    """Build historical predictor features for IV/RV and regime research.

    The feature set uses only repository-native historical datasets: Spot OHLCV,
    Perpetual OHLCV, funding, open interest, perpetual trades, and option trades.
    It intentionally excludes volatility-index and IV/RV sources so the output is
    usable as ex-ante predictor state rather than as an IV label source.
    """

    pl = dependencies.require_polars()
    normalized_symbol = _normalize_base_symbol(symbol)
    sources = [
        _read_source(
            pl,
            silver_root=silver_root,
            dataset_type="spot_ohlcv",
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            timestamp_column="open_time",
            columns=[pl.col("close_price").cast(pl.Float64).alias("spot_close_price")],
        ),
        _read_source(
            pl,
            silver_root=silver_root,
            dataset_type="perps_ohlcv",
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            timestamp_column="open_time",
            columns=[pl.col("close_price").cast(pl.Float64).alias("perps_close_price")],
        ),
        _read_source(
            pl,
            silver_root=silver_root,
            dataset_type="funding_1m_feature",
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            timestamp_column="timestamp",
            columns=[pl.col("funding_rate_last_known").cast(pl.Float64)],
        ),
        _read_source(
            pl,
            silver_root=silver_root,
            dataset_type="open_interest_1m_feature",
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            timestamp_column="timestamp_m1",
            columns=[pl.col("open_interest").cast(pl.Float64)],
        ),
        _read_source(
            pl,
            silver_root=silver_root,
            dataset_type="perps_trades_1m_feature",
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            timestamp_column="timestamp_m1",
            columns=[
                pl.col("buy_volume").cast(pl.Float64).alias("perps_buy_volume"),
                pl.col("sell_volume").cast(pl.Float64).alias("perps_sell_volume"),
                pl.col("trade_count").cast(pl.Float64).alias("perps_trade_count"),
                pl.col("quote_volume").cast(pl.Float64).alias("perps_quote_volume"),
            ],
        ),
        _read_source(
            pl,
            silver_root=silver_root,
            dataset_type="options_trades_1m_feature",
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            timestamp_column="timestamp_m1",
            columns=[
                pl.col("buy_volume").cast(pl.Float64).alias("options_buy_volume"),
                pl.col("sell_volume").cast(pl.Float64).alias("options_sell_volume"),
                pl.col("trade_count").cast(pl.Float64).alias("options_trade_count"),
                pl.col("quote_volume").cast(pl.Float64).alias("options_quote_volume"),
            ],
        ),
    ]
    merged = _merge_sources(pl, sources)
    if merged.height == 0:
        return dependencies.report_factory(
            dataset=output_dataset_type,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            period_start=None,
            period_end=None,
            months_processed=[],
            rows_in=0,
            rows_out=0,
            duplicates_removed=0,
            invalid_ohlc_rows=0,
            null_price_rows=0,
            min_timestamp=None,
            max_timestamp=None,
            symbols=[normalized_symbol],
            columns=list(SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS),
        )

    feature = _build_feature_frame(pl, merged)
    months = sorted(
        {value.strftime("%Y-%m") for value in feature["timestamp_m1"].to_list() if isinstance(value, datetime)}
    )
    rows_out = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None
    for month in months:
        month_frame = feature.filter(pl.col("timestamp_m1").dt.strftime("%Y-%m") == month)
        if month_frame.height == 0:
            continue
        target = dependencies.silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        month_frame.write_parquet(target)
        rows_out += month_frame.height
        month_min = month_frame.select(pl.col("timestamp_m1").min()).item()
        month_max = month_frame.select(pl.col("timestamp_m1").max()).item()
        if isinstance(month_min, datetime):
            min_timestamp = month_min if min_timestamp is None else min(min_timestamp, month_min)
        if isinstance(month_max, datetime):
            max_timestamp = month_max if max_timestamp is None else max(max_timestamp, month_max)

    return dependencies.report_factory(
        dataset=output_dataset_type,
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
        period_start=dependencies.iso_utc(min_timestamp),
        period_end=dependencies.iso_utc(max_timestamp),
        months_processed=months,
        rows_in=merged.height,
        rows_out=rows_out,
        duplicates_removed=0,
        invalid_ohlc_rows=0,
        null_price_rows=0,
        min_timestamp=dependencies.iso_utc(min_timestamp),
        max_timestamp=dependencies.iso_utc(max_timestamp),
        symbols=[normalized_symbol],
        columns=list(SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS),
    )
