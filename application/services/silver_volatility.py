"""Silver transformations for volatility dataset families."""

from __future__ import annotations

from bisect import bisect_left, bisect_right, insort
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_VOLATILITY_FEATURE_COLUMNS, SILVER_VOLATILITY_OBSERVED_COLUMNS


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
class VolatilityObservedDependencies:
    """Shared Silver helpers required by volatility observed transformations."""

    require_polars: Callable[[], Any]
    discover_months: Callable[..., list[str]]
    bronze_month_files: Callable[..., list[str]]
    silver_month_path: Callable[..., Path]
    normalize_symbol_expr: Callable[..., Any]
    iso_utc: Callable[[datetime | None], str | None]
    report_factory: SilverReportFactory


@dataclass(frozen=True)
class VolatilityFeatureDependencies:
    """Shared Silver helpers required by volatility feature transformations."""

    require_polars: Callable[[], Any]
    silver_month_path: Callable[..., Path]
    iso_utc: Callable[[datetime | None], str | None]
    report_factory: SilverReportFactory


def discover_snapshot_symbols(
    *,
    bronze_root: str,
    dataset_type: str,
    exchange: str,
) -> list[str]:
    """Discover volatility snapshot currencies from the live-loader Bronze layout."""

    root = Path(bronze_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
    if not root.exists():
        return []
    return sorted(
        path.name.split("=", 1)[1].upper()
        for path in root.glob("currency=*")
        if path.is_dir() and path.name.startswith("currency=")
    )


def _discover_snapshot_months(
    *,
    bronze_root: str,
    dataset_type: str,
    exchange: str,
    currency: str,
    source: str,
) -> list[str]:
    root = (
        Path(bronze_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"currency={currency}"
        / f"source={source}"
    )
    if not root.exists():
        return []
    months: set[str] = set()
    for path in root.glob("year=*/month=*"):
        year = path.parent.name.split("=", 1)[1]
        month = path.name.split("=", 1)[1]
        if len(month) == 2:
            months.add(f"{year}-{month}")
        else:
            months.add(month)
    return sorted(months)


def _snapshot_month_files(
    *,
    bronze_root: str,
    dataset_type: str,
    exchange: str,
    currency: str,
    source: str,
    month: str,
) -> list[str]:
    year, month_part = month.split("-", 1)
    root = (
        Path(bronze_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"currency={currency}"
        / f"source={source}"
        / f"year={year}"
        / f"month={month_part}"
    )
    return sorted(str(path) for path in root.glob("date=*/hour=*/data.parquet"))


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
    dependencies: VolatilityObservedDependencies,
) -> object:
    """Build observed Silver rows from live volatility-index snapshot Bronze files.

    Args:
        bronze_root: Root directory for Bronze input parquet files.
        silver_root: Root directory for Silver output parquet files.
        exchange: Exchange partition value.
        symbol: Currency symbol partition value, for example ``BTC`` or ``ETH``.
        timeframe: Silver output timeframe partition.
        bronze_dataset_type: Source Bronze snapshot dataset type.
        output_dataset_type: Target Silver observed dataset type.
        source: Live-loader source partition.
        dependencies: Shared Silver helper functions supplied by the orchestration service.

    Returns:
        A Silver build report object created by ``dependencies.report_factory``.
    """

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    months = _discover_snapshot_months(
        bronze_root=bronze_root,
        dataset_type=bronze_dataset_type,
        exchange=exchange,
        currency=normalized_symbol,
        source=source,
    )
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    agg_invalid_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = _snapshot_month_files(
            bronze_root=bronze_root,
            dataset_type=bronze_dataset_type,
            exchange=exchange,
            currency=normalized_symbol,
            source=source,
            month=month,
        )
        if not files:
            continue
        frame = pl.scan_parquet(files).collect()
        rows_in = frame.height
        if rows_in == 0:
            continue

        frame = frame.with_columns(
            [
                pl.col("timestamp").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp"),
                pl.col("close").cast(pl.Float64).alias("volatility_value"),
                pl.col("open").cast(pl.Float64).alias("volatility_open"),
                pl.col("high").cast(pl.Float64).alias("volatility_high"),
                pl.col("low").cast(pl.Float64).alias("volatility_low"),
                pl.col("close").cast(pl.Float64).alias("volatility_close"),
                dependencies.normalize_symbol_expr(pl, "currency").alias("symbol"),
                pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
                pl.lit("perp").alias("instrument_type"),
                pl.col("dataset_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("dataset_type"),
                pl.col("source").cast(pl.Utf8).alias("source_endpoint"),
                pl.col("snapshot_time")
                .cast(pl.Datetime(time_unit="us", time_zone="UTC"))
                .alias("volatility_source_timestamp"),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("ingested_at"),
            ]
        )

        invalid_expr = (
            pl.col("timestamp").is_null()
            | pl.col("symbol").is_null()
            | (pl.col("symbol").str.len_chars() == 0)
            | pl.col("volatility_close").is_null()
            | (~pl.col("volatility_close").is_finite())
            | (pl.col("volatility_close") < 0.0)
        )
        invalid_rows = frame.select(invalid_expr.cast(pl.Int64).sum().alias("count")).item()
        cleaned = frame.filter(~invalid_expr)
        observed = (
            cleaned.sort(["timestamp", "ingested_at"])
            .unique(
                subset=["exchange", "symbol", "dataset_type", "timestamp"],
                keep="last",
                maintain_order=True,
            )
            .sort(["exchange", "symbol", "timestamp"])
            .select(SILVER_VOLATILITY_OBSERVED_COLUMNS)
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
        columns=SILVER_VOLATILITY_OBSERVED_COLUMNS,
    )


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


def _discover_observed_months(
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


def _with_iv_change(
    pl: Any,
    feature: Any,
    *,
    minutes: int,
    output_column: str,
) -> Any:
    lookback_column = f"_{output_column}_lookback_timestamp"
    previous_column = f"_{output_column}_previous_close"
    lookup = feature.select(
        [
            "exchange",
            "symbol",
            pl.col("timestamp_m1").alias("_lookup_timestamp"),
            pl.col("iv_close").alias(previous_column),
        ]
    ).sort(["exchange", "symbol", "_lookup_timestamp"])
    joined = (
        feature.with_columns((pl.col("timestamp_m1") - pl.duration(minutes=minutes)).alias(lookback_column))
        .sort(["exchange", "symbol", lookback_column])
        .join_asof(
            lookup,
            left_on=lookback_column,
            right_on="_lookup_timestamp",
            by=["exchange", "symbol"],
            strategy="backward",
            check_sortedness=False,
        )
        .with_columns((pl.col("iv_close") - pl.col(previous_column)).alias(output_column))
        .drop([lookback_column, "_lookup_timestamp", previous_column])
    )
    return joined.sort(["exchange", "symbol", "timestamp_m1"])


def _rolling_zscore_expr(pl: Any, window_size: str) -> Any:
    rolling_mean = (
        pl.col("iv_close")
        .rolling_mean_by(
            "timestamp_m1",
            window_size=window_size,
            min_samples=2,
        )
        .over(["exchange", "symbol"])
    )
    rolling_std = (
        pl.col("iv_close")
        .rolling_std_by(
            "timestamp_m1",
            window_size=window_size,
            min_samples=2,
        )
        .over(["exchange", "symbol"])
    )
    return pl.when(rolling_std > 0.0).then((pl.col("iv_close") - rolling_mean) / rolling_std).otherwise(None)


def _rolling_percentile_30d(feature: Any) -> list[float | None]:
    ranks: list[float | None] = []
    current_group: tuple[str, str] | None = None
    window: deque[tuple[datetime, float]] = deque()
    sorted_values: list[float] = []

    for row in feature.iter_rows(named=True):
        group = (str(row["exchange"]), str(row["symbol"]))
        timestamp = row["timestamp_m1"]
        value = row["iv_close"]
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

        # The current observation is inserted before ranking so the percentile
        # uses a closed trailing window and never needs future rows.
        numeric_value = float(value)
        window.append((timestamp, numeric_value))
        insort(sorted_values, numeric_value)
        ranks.append(bisect_right(sorted_values, numeric_value) / len(sorted_values))

    return ranks


def build_volatility_index_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    snapshot_dataset_type: str = "volatility_index_snapshot_1m_observed",
    historical_dataset_type: str = "volatility_index_data_observed",
    output_dataset_type: str = "volatility_index_1m_feature",
    dependencies: VolatilityFeatureDependencies,
) -> object:
    """Build canonical IV minute features from snapshot rows with historical fallback.

    Args:
        silver_root: Root directory for Silver input and output parquet files.
        exchange: Exchange partition value.
        symbol: Currency symbol partition value.
        timeframe: Observation and feature timeframe.
        snapshot_dataset_type: Preferred fresh observed dataset.
        historical_dataset_type: Historical observed fallback dataset.
        output_dataset_type: Target feature dataset type.
        dependencies: Shared Silver helper functions supplied by the orchestration service.

    Returns:
        A Silver build report object created by ``dependencies.report_factory``.
    """

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    months = sorted(
        set(
            _discover_observed_months(
                silver_root=silver_root,
                dataset_type=snapshot_dataset_type,
                exchange=exchange,
                symbol=normalized_symbol,
                timeframe=timeframe,
            )
        )
        | set(
            _discover_observed_months(
                silver_root=silver_root,
                dataset_type=historical_dataset_type,
                exchange=exchange,
                symbol=normalized_symbol,
                timeframe=timeframe,
            )
        )
    )
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        inputs = []
        for dataset_type, source_priority in (
            (historical_dataset_type, 0),
            (snapshot_dataset_type, 1),
        ):
            path = _observed_month_file(
                silver_root=silver_root,
                dataset_type=dataset_type,
                exchange=exchange,
                symbol=normalized_symbol,
                timeframe=timeframe,
                month=month,
            )
            if path.exists():
                inputs.append(
                    pl.read_parquet(path).with_columns(
                        [
                            pl.lit(dataset_type).alias("iv_source_dataset"),
                            pl.lit(source_priority).alias("_source_priority"),
                        ]
                    )
                )
        if not inputs:
            continue

        frame = pl.concat(inputs, how="vertical_relaxed")
        rows_in = frame.height
        if rows_in == 0:
            continue

        selected = (
            frame.sort(["timestamp", "_source_priority", "ingested_at"])
            .unique(subset=["exchange", "symbol", "timestamp"], keep="last", maintain_order=True)
            .sort(["exchange", "symbol", "timestamp"])
        )
        duplicates_removed = rows_in - selected.height
        previous_close = pl.col("iv_close").shift(1).over(["exchange", "symbol"])
        feature = selected.with_columns(
            [
                pl.col("timestamp").alias("timestamp_m1"),
                pl.col("volatility_open").alias("iv_open"),
                pl.col("volatility_high").alias("iv_high"),
                pl.col("volatility_low").alias("iv_low"),
                pl.col("volatility_close").alias("iv_close"),
                pl.col("volatility_source_timestamp").alias("iv_source_timestamp"),
            ]
        ).with_columns(
            [
                (pl.col("iv_high") - pl.col("iv_low")).alias("iv_range"),
                # Log returns are only defined for strictly positive IV values;
                # nulls prevent zero or bad upstream data from creating infinities.
                pl.when((pl.col("iv_close") > 0.0) & (previous_close > 0.0))
                .then((pl.col("iv_close") / previous_close).log())
                .otherwise(None)
                .alias("iv_return_1m"),
                # QC-01: Deribit's volatility index is constructed as an annualized,
                # 30-day implied-volatility measure already expressed in percentage
                # points. This alias exposes that unit/horizon explicitly so IV/RV
                # comparisons never need to guess `iv_close` semantics.
                pl.col("iv_close").alias("iv_30d_annualized_pct"),
                _rolling_zscore_expr(pl, "1d").alias("iv_zscore_1d"),
                _rolling_zscore_expr(pl, "7d").alias("iv_zscore_7d"),
                pl.lit(0, dtype=pl.Int64).alias("minutes_since_iv_observation"),
                pl.lit(True).alias("iv_data_available"),
            ]
        )
        for minutes, output_column in (
            (5, "iv_change_5m"),
            (15, "iv_change_15m"),
            (60, "iv_change_1h"),
        ):
            feature = _with_iv_change(pl, feature, minutes=minutes, output_column=output_column)
        feature = feature.with_columns(pl.Series("iv_percentile_30d", _rolling_percentile_30d(feature))).select(
            SILVER_VOLATILITY_FEATURE_COLUMNS
        )

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
        columns=SILVER_VOLATILITY_FEATURE_COLUMNS,
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
    dependencies: VolatilityObservedDependencies,
) -> object:
    """Build monthly volatility-observed Silver outputs from Bronze volatility datasets.

    Args:
        bronze_root: Root directory for Bronze input parquet files.
        silver_root: Root directory for Silver output parquet files.
        exchange: Exchange partition value.
        symbol: Symbol partition value.
        timeframe: Timeframe partition value for both Bronze input and Silver output.
        bronze_dataset_type: Source Bronze volatility dataset type.
        output_dataset_type: Target Silver observed dataset type.
        dependencies: Shared Silver helper functions supplied by the orchestration service.

    Returns:
        A Silver build report object created by ``dependencies.report_factory``.
    """

    pl = dependencies.require_polars()
    months = dependencies.discover_months(
        bronze_root=bronze_root,
        market=bronze_dataset_type,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        instrument_type="perp",
    )
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    agg_invalid_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = dependencies.bronze_month_files(
            bronze_root=bronze_root,
            market=bronze_dataset_type,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
            instrument_type="perp",
        )
        if not files:
            continue
        frame = pl.scan_parquet(files).collect()
        rows_in = frame.height
        if rows_in == 0:
            continue

        # Older Bronze volatility_index_data partitions only persisted `value`;
        # use it as OHLC to keep historical files readable after schema expansion.
        ohlc_exprs = []
        for column_name in ("open", "high", "low", "close"):
            if column_name in frame.columns:
                ohlc_exprs.append(pl.col(column_name).fill_null(pl.col("value")).alias(column_name))
            else:
                ohlc_exprs.append(pl.col("value").alias(column_name))
        frame = frame.with_columns(ohlc_exprs)

        frame = frame.with_columns(
            [
                pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp"),
                pl.col("value").cast(pl.Float64).alias("volatility_value"),
                pl.col("open").cast(pl.Float64).alias("volatility_open"),
                pl.col("high").cast(pl.Float64).alias("volatility_high"),
                pl.col("low").cast(pl.Float64).alias("volatility_low"),
                pl.col("close").cast(pl.Float64).alias("volatility_close"),
                dependencies.normalize_symbol_expr(pl, "symbol").alias("symbol"),
                pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
                pl.col("instrument_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("instrument_type"),
                pl.col("dataset_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("dataset_type"),
                pl.col("source_endpoint").cast(pl.Utf8).alias("source_endpoint"),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("ingested_at"),
            ]
        )

        invalid_expr = (
            pl.col("timestamp").is_null()
            | pl.col("symbol").is_null()
            | (pl.col("symbol").str.len_chars() == 0)
            | pl.col("volatility_value").is_null()
            | (~pl.col("volatility_value").is_finite())
            | (pl.col("volatility_value") < 0.0)
        )
        invalid_rows = frame.select(invalid_expr.cast(pl.Int64).sum().alias("count")).item()
        cleaned = frame.filter(~invalid_expr)
        observed = (
            cleaned.sort(["timestamp", "ingested_at"])
            .unique(
                subset=["exchange", "symbol", "dataset_type", "timestamp"],
                keep="last",
                maintain_order=True,
            )
            .sort(["exchange", "symbol", "timestamp"])
            .with_columns(pl.col("timestamp").alias("volatility_source_timestamp"))
            .select(SILVER_VOLATILITY_OBSERVED_COLUMNS)
        )
        duplicates_removed = cleaned.height - observed.height

        target = dependencies.silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        observed.write_parquet(target)

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
        min_timestamp=dependencies.iso_utc(min_timestamp),
        max_timestamp=dependencies.iso_utc(max_timestamp),
        symbols=[symbol],
        columns=SILVER_VOLATILITY_OBSERVED_COLUMNS,
    )
