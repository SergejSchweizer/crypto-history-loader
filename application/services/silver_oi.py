"""Silver transformations for open-interest dataset families."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_OI_M1_FEATURE_COLUMNS, SILVER_OI_OBSERVED_COLUMNS

__all__ = [
    "OiDependencies",
    "SILVER_OI_M1_FEATURE_COLUMNS",
    "SILVER_OI_OBSERVED_COLUMNS",
    "SilverReportFactory",
    "build_oi_1m_feature_for_symbol",
    "build_oi_observed_for_symbol",
]


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
class OiDependencies:
    """Shared Silver helpers required by open-interest transformations."""

    require_polars: Callable[[], Any]
    discover_months: Callable[..., list[str]]
    bronze_month_files: Callable[..., list[str]]
    silver_month_path: Callable[..., Path]
    silver_oi_feature_month_path: Callable[..., Path]
    normalize_symbol_expr: Callable[..., Any]
    iso_utc: Callable[[datetime | None], str | None]
    report_factory: SilverReportFactory


def build_oi_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    dependencies: OiDependencies,
) -> object:
    """Build monthly ``oi_observed`` Silver outputs from Bronze OI observations."""

    pl = dependencies.require_polars()
    months = dependencies.discover_months(
        bronze_root=bronze_root,
        market="oi",
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
            market="oi",
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

        frame = frame.with_columns(
            [
                pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp"),
                pl.col("open_interest").cast(pl.Float64).alias("open_interest"),
                dependencies.normalize_symbol_expr(pl, "symbol").alias("symbol"),
                pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("ingested_at"),
                pl.col("source_endpoint").cast(pl.Utf8).alias("source_endpoint"),
            ]
        )
        if "oi_is_observed" in frame.columns:
            frame = frame.filter(pl.col("oi_is_observed").fill_null(False))

        invalid_expr = (
            pl.col("timestamp").is_null()
            | pl.col("symbol").is_null()
            | (pl.col("symbol").str.len_chars() == 0)
            | pl.col("open_interest").is_null()
            | (~pl.col("open_interest").is_finite())
            | (pl.col("open_interest") < 0.0)
        )
        invalid_rows = frame.select(invalid_expr.cast(pl.Int64).sum().alias("count")).item()
        cleaned = frame.filter(~invalid_expr)
        observed = (
            cleaned.unique(
                subset=["exchange", "symbol", "timestamp", "open_interest"],
                keep="last",
                maintain_order=True,
            )
            .sort(["exchange", "symbol", "timestamp"])
            .with_columns(pl.col("timestamp").alias("oi_source_timestamp"))
            .select(SILVER_OI_OBSERVED_COLUMNS)
        )
        duplicates_removed = cleaned.height - observed.height

        target = dependencies.silver_month_path(
            silver_root=silver_root,
            market="oi_observed",
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
        dataset="oi_observed",
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
        columns=SILVER_OI_OBSERVED_COLUMNS,
    )


def build_oi_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    observed_timeframe: str = "1m",
    cutoff_time: datetime | None = None,
    dependencies: OiDependencies,
) -> object:
    """Build monthly ``oi_1m_feature`` from ``oi_observed`` using backward asof join."""

    if cutoff_time is None:
        cutoff_time = datetime.now(UTC)

    pl = dependencies.require_polars()
    observed_root = (
        Path(silver_root)
        / "dataset_type=oi_observed"
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
        observed = pl.read_parquet(month_file).sort("timestamp")
        if observed.height == 0:
            continue

        month_start = datetime.fromisoformat(f"{month}-01T00:00:00+00:00")
        observed_max = observed.select(pl.col("timestamp").max()).item()
        if not isinstance(observed_max, datetime):
            continue
        month_end_exclusive = observed_max + timedelta(minutes=1)
        if cutoff_time < observed_max:
            month_end_exclusive = cutoff_time + timedelta(minutes=1)
        calendar = pl.DataFrame(
            {
                "timestamp_m1": pl.datetime_range(
                    start=month_start,
                    end=month_end_exclusive,
                    interval="1m",
                    closed="left",
                    time_zone="UTC",
                    eager=True,
                )
            }
        )
        right = observed.select(
            [
                pl.col("timestamp").alias("oi_source_timestamp"),
                pl.col("open_interest").alias("open_interest_observed"),
            ]
        )
        joined = calendar.join_asof(
            right.sort("oi_source_timestamp"),
            left_on="timestamp_m1",
            right_on="oi_source_timestamp",
            strategy="backward",
        )
        feature = (
            joined.with_columns(
                [
                    pl.lit(exchange).alias("exchange"),
                    pl.lit(symbol).alias("symbol"),
                    pl.col("open_interest_observed").alias("open_interest"),
                    (pl.col("timestamp_m1") == pl.col("oi_source_timestamp")).fill_null(False).alias("oi_is_observed"),
                    (pl.col("timestamp_m1") != pl.col("oi_source_timestamp")).fill_null(True).alias("oi_is_ffill"),
                    ((pl.col("timestamp_m1") - pl.col("oi_source_timestamp")).dt.total_minutes().cast(pl.Int64)).alias(
                        "minutes_since_oi_observation"
                    ),
                    ((pl.col("timestamp_m1") - pl.col("oi_source_timestamp")).dt.total_seconds().cast(pl.Int64)).alias(
                        "oi_observation_lag_sec"
                    ),
                ]
            )
            .select(SILVER_OI_M1_FEATURE_COLUMNS)
            .sort("timestamp_m1")
        )

        leakage_count = feature.filter(
            pl.col("oi_source_timestamp").is_not_null() & (pl.col("oi_source_timestamp") > pl.col("timestamp_m1"))
        ).height
        if leakage_count > 0:
            raise ValueError(f"OI leakage detected for {exchange}/{symbol}/{month}: {leakage_count} rows")

        target = dependencies.silver_oi_feature_month_path(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
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

        agg_rows_in += observed.height
        agg_rows_out += feature.height

    return dependencies.report_factory(
        dataset="oi_1m_feature",
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
        min_timestamp=dependencies.iso_utc(min_timestamp),
        max_timestamp=dependencies.iso_utc(max_timestamp),
        symbols=[symbol],
        columns=SILVER_OI_M1_FEATURE_COLUMNS,
    )
