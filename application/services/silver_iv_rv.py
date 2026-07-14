"""Silver feature builder for IV/RV spread state."""

from __future__ import annotations

from bisect import bisect_left, bisect_right, insort
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_IV_RV_FEATURE_COLUMNS


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
class IvRvDependencies:
    """Shared Silver helpers required by IV/RV transformations."""

    require_polars: Any
    silver_month_path: Any
    iso_utc: Any
    report_factory: SilverReportFactory


def discover_iv_rv_symbols(
    *,
    silver_root: str,
    exchange: str,
    timeframe: str = "1m",
) -> list[str]:
    """Discover symbols that have both IV and RV feature inputs."""

    symbols_by_dataset: list[set[str]] = []
    for dataset_type in ("volatility_index_1m_feature", "realized_volatility_1m_feature"):
        root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
        symbols: set[str] = set()
        if not root.exists():
            return []
        for path in root.glob(f"symbol=*/timeframe={timeframe}"):
            symbol_segment = path.parent.name
            if symbol_segment.startswith("symbol="):
                symbols.add(symbol_segment.split("=", 1)[1].strip().upper())
        symbols_by_dataset.append(symbols)
    return sorted(set.intersection(*symbols_by_dataset)) if symbols_by_dataset else []


def _dataset_root(
    *,
    silver_root: str,
    dataset_type: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> Path:
    return (
        Path(silver_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )


def _discover_months(root: Path) -> set[str]:
    if not root.exists():
        return set()
    return {path.name.split("=", 1)[1] for path in root.glob("year=*/month=*") if path.name.startswith("month=")}


def _month_file(root: Path, month: str, symbol: str) -> Path | None:
    year = month.split("-", 1)[0]
    path = root / f"year={year}" / f"month={month}" / f"{symbol}-{month}.parquet"
    if path.exists():
        return path
    files = sorted((root / f"year={year}" / f"month={month}").glob("*.parquet"))
    return files[0] if files else None


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


def _rolling_percentile_30d(feature: Any, column: str) -> list[float | None]:
    ranks: list[float | None] = []
    current_group: tuple[str, str] | None = None
    window: deque[tuple[datetime, float]] = deque()
    sorted_values: list[float] = []

    for row in feature.iter_rows(named=True):
        group = (str(row["exchange"]), str(row["symbol"]))
        timestamp = row["timestamp_m1"]
        value = row[column]
        if not isinstance(timestamp, datetime) or not isinstance(value, int | float):
            ranks.append(None)
            continue
        if current_group != group:
            current_group = group
            window.clear()
            sorted_values.clear()

        cutoff = timestamp - timedelta(days=30)
        while window and window[0][0] < cutoff:
            _, old_value = window.popleft()
            old_index = bisect_left(sorted_values, old_value)
            if old_index < len(sorted_values):
                sorted_values.pop(old_index)

        # Closed trailing window: the rank includes the current row and never
        # requires future spread values.
        numeric_value = float(value)
        window.append((timestamp, numeric_value))
        insort(sorted_values, numeric_value)
        ranks.append(bisect_right(sorted_values, numeric_value) / len(sorted_values))

    return ranks


def build_iv_rv_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    iv_dataset_type: str = "volatility_index_1m_feature",
    rv_dataset_type: str = "realized_volatility_1m_feature",
    output_dataset_type: str = "iv_rv_1m_feature",
    dependencies: IvRvDependencies,
) -> object:
    """Build direct IV/RV state features for one symbol.

    Args:
        silver_root: Root directory for Silver input and output parquet files.
        exchange: Exchange partition value.
        symbol: Base asset symbol, for example ``BTC``.
        timeframe: Input and output timeframe.
        iv_dataset_type: Source IV feature dataset.
        rv_dataset_type: Source RV feature dataset.
        output_dataset_type: Target IV/RV feature dataset.
        dependencies: Shared Silver helper functions supplied by the orchestration service.

    Returns:
        A Silver build report object created by ``dependencies.report_factory``.
    """

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    iv_root = _dataset_root(
        silver_root=silver_root,
        dataset_type=iv_dataset_type,
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
    )
    rv_root = _dataset_root(
        silver_root=silver_root,
        dataset_type=rv_dataset_type,
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
    )
    months = sorted(_discover_months(iv_root) & _discover_months(rv_root))
    agg_rows_in = 0
    agg_rows_out = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        iv_path = _month_file(iv_root, month, normalized_symbol)
        rv_path = _month_file(rv_root, month, normalized_symbol)
        iv = (
            pl.read_parquet(iv_path).select(
                [
                    "timestamp_m1",
                    "exchange",
                    "symbol",
                    "iv_close",
                    "minutes_since_iv_observation",
                ]
            )
            if iv_path is not None
            else None
        )
        rv = (
            pl.read_parquet(rv_path).select(
                [
                    "timestamp_m1",
                    "exchange",
                    "symbol",
                    "rv_1h",
                    "rv_1d",
                ]
            )
            if rv_path is not None
            else None
        )
        if iv is None or rv is None:
            continue
        assert iv is not None and rv is not None
        agg_rows_in += iv.height + rv.height
        frame = iv.join(rv, on=["timestamp_m1", "exchange", "symbol"], how="inner")
        feature = (
            frame.with_columns(
                [
                    pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                    pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
                    pl.lit(normalized_symbol).alias("symbol"),
                    pl.col("iv_close").is_not_null().alias("iv_available"),
                    (pl.col("rv_1h").is_not_null() | pl.col("rv_1d").is_not_null()).alias("rv_available"),
                    pl.when(pl.col("rv_1h").is_not_null())
                    .then(0)
                    .otherwise(None)
                    .alias("minutes_since_rv_observation"),
                ]
            )
            .with_columns(
                [
                    (pl.col("iv_close") - pl.col("rv_1h")).alias("iv_minus_rv_1h"),
                    (pl.col("iv_close") - pl.col("rv_1d")).alias("iv_minus_rv_1d"),
                    pl.when(pl.col("rv_1h") > 0.0)
                    .then(pl.col("iv_close") / pl.col("rv_1h"))
                    .otherwise(None)
                    .alias("iv_rv_ratio_1h"),
                    pl.when(pl.col("rv_1d") > 0.0)
                    .then(pl.col("iv_close") / pl.col("rv_1d"))
                    .otherwise(None)
                    .alias("iv_rv_ratio_1d"),
                ]
            )
            .sort(["exchange", "symbol", "timestamp_m1"])
            .with_columns(_rolling_zscore_expr(pl, "iv_minus_rv_1d", "1d").alias("iv_rv_zscore_1d"))
        )
        feature = feature.with_columns(
            pl.Series("iv_rv_percentile_30d", _rolling_percentile_30d(feature, "iv_minus_rv_1d"))
        ).select(SILVER_IV_RV_FEATURE_COLUMNS)

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
        columns=SILVER_IV_RV_FEATURE_COLUMNS,
    )
