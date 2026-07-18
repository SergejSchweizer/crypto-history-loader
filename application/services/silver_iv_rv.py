"""Silver feature builder for IV/RV spread state."""

from __future__ import annotations

from bisect import bisect_left, bisect_right, insort
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_IV_RV_FEATURE_COLUMNS
from application.services.silver_monthly_lookback import (
    lookback_month_keys,
    month_end_exclusive,
    month_start,
)

# QC-02: widest rolling window used by this builder (`iv_rv_percentile_30d`), in
# days. Every month is calculated on a buffered frame that includes this much prior
# context so the rolling z-score and percentile are not reset by monthly storage
# partition boundaries.
_REQUIRED_LOOKBACK_DAYS = 30


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
        calculation_lookback_days: int | None = None,
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
    """Discover symbols that have IV or RV feature inputs."""

    symbols: set[str] = set()
    for dataset_type in ("volatility_index_1m_feature", "realized_volatility_1m_feature"):
        root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
        if not root.exists():
            continue
        for path in root.glob(f"symbol=*/timeframe={timeframe}"):
            symbol_segment = path.parent.name
            if symbol_segment.startswith("symbol="):
                symbols.add(symbol_segment.split("=", 1)[1].strip().upper())
    return sorted(symbols)


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


def _read_iv_rv_month(
    pl: Any,
    *,
    iv_root: Path,
    rv_root: Path,
    month: str,
    symbol: str,
) -> tuple[Any | None, int]:
    """Read and join one month of IV and RV rows.

    Returns the joined frame (or ``None`` if neither source has this month) and the
    raw row count before joining, used for Silver build reporting.
    """

    iv_path = _month_file(iv_root, month, symbol)
    rv_path = _month_file(rv_root, month, symbol)
    iv = (
        pl.read_parquet(iv_path).select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                "iv_close",
                # QC-01: annualized, 30-day-horizon IV alias used for the
                # unit-safe spread/ratio below.
                "iv_30d_annualized_pct",
                "minutes_since_iv_observation",
            ]
        )
        if iv_path is not None
        else None
    )
    rv = None
    if rv_path is not None:
        rv_frame = pl.read_parquet(rv_path)
        if "canonical_rv_source" not in rv_frame.columns:
            rv_frame = rv_frame.with_columns(pl.lit(None, dtype=pl.Utf8).alias("canonical_rv_source"))
        rv = rv_frame.select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                "canonical_rv_source",
                "rv_1h",
                "rv_1d",
                # QC-01: annualized, 30-day-horizon RV used for the unit-safe
                # spread/ratio below.
                "rv_30d_annualized_pct",
            ]
        )
    if iv is None and rv is None:
        return None, 0
    rows_in = (iv.height if iv is not None else 0) + (rv.height if rv is not None else 0)
    if iv is None:
        assert rv is not None
        frame = rv.with_columns(
            [
                pl.lit(None, dtype=pl.Float64).alias("iv_close"),
                pl.lit(None, dtype=pl.Float64).alias("iv_30d_annualized_pct"),
                pl.lit(None, dtype=pl.Int64).alias("minutes_since_iv_observation"),
            ]
        )
    elif rv is None:
        frame = iv.with_columns(
            [
                pl.lit(None, dtype=pl.Utf8).alias("canonical_rv_source"),
                pl.lit(None, dtype=pl.Float64).alias("rv_1h"),
                pl.lit(None, dtype=pl.Float64).alias("rv_1d"),
                pl.lit(None, dtype=pl.Float64).alias("rv_30d_annualized_pct"),
            ]
        )
    else:
        frame = iv.join(rv, on=["timestamp_m1", "exchange", "symbol"], how="full", coalesce=True)
    return frame, rows_in


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
    months = sorted(_discover_months(iv_root) | _discover_months(rv_root))
    agg_rows_in = 0
    agg_rows_out = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None
    # QC-02: cache joined month reads across iterations since lookback windows for
    # consecutive target months overlap heavily.
    month_cache: dict[str, Any] = {}
    rows_in_cache: dict[str, int] = {}

    def _cached_month(month_key: str) -> Any | None:
        if month_key not in month_cache:
            joined, rows_in = _read_iv_rv_month(
                pl,
                iv_root=iv_root,
                rv_root=rv_root,
                month=month_key,
                symbol=normalized_symbol,
            )
            month_cache[month_key] = joined
            rows_in_cache[month_key] = rows_in
        return month_cache[month_key]

    for month in months:
        target_start = month_start(month)
        target_end = month_end_exclusive(month)
        calculation_keys = [*lookback_month_keys(month, lookback_days=_REQUIRED_LOOKBACK_DAYS), month]

        buffered_frames = [month_frame for key in calculation_keys if (month_frame := _cached_month(key)) is not None]
        if month not in month_cache or month_cache[month] is None:
            continue
        agg_rows_in += rows_in_cache[month]
        if not buffered_frames:
            continue

        frame = pl.concat(buffered_frames, how="vertical_relaxed").sort(["exchange", "symbol", "timestamp_m1"])
        feature = (
            frame.with_columns(
                [
                    pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                    pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
                    pl.lit(normalized_symbol).alias("symbol"),
                    pl.col("canonical_rv_source").cast(pl.Utf8),
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
                    # Deprecated (QC-01): mixes annualized IV percentage points with
                    # non-annualized, sub-30-day RV; kept unchanged for backward
                    # compatibility with existing persisted artifacts.
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
                    # QC-01: unit- and horizon-compatible comparison. Both sides are
                    # annualized volatility percentage points over a 30-day horizon,
                    # so subtraction and division are financially interpretable.
                    (pl.col("iv_30d_annualized_pct") - pl.col("rv_30d_annualized_pct")).alias("iv_rv_spread_30d_pct"),
                    pl.when(pl.col("rv_30d_annualized_pct") > 0.0)
                    .then(pl.col("iv_30d_annualized_pct") / pl.col("rv_30d_annualized_pct"))
                    .otherwise(None)
                    .alias("iv_rv_ratio_30d"),
                ]
            )
            .sort(["exchange", "symbol", "timestamp_m1"])
            .with_columns(_rolling_zscore_expr(pl, "iv_minus_rv_1d", "1d").alias("iv_rv_zscore_1d"))
        )
        feature = feature.with_columns(
            pl.Series("iv_rv_percentile_30d", _rolling_percentile_30d(feature, "iv_minus_rv_1d"))
        )
        # QC-02: the buffered frame spans the lookback window plus the target month
        # so rolling functions above see the required trailing context; trim back to
        # only the target month's rows before writing.
        feature = feature.filter(
            (pl.col("timestamp_m1") >= target_start) & (pl.col("timestamp_m1") < target_end)
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
        calculation_lookback_days=_REQUIRED_LOOKBACK_DAYS,
    )
