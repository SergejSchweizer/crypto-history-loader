"""Silver transformations for funding dataset families."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_FUNDING_FEATURE_COLUMNS, SILVER_FUNDING_OBSERVED_COLUMNS

__all__ = [
    "FundingDependencies",
    "SILVER_FUNDING_FEATURE_COLUMNS",
    "SILVER_FUNDING_OBSERVED_COLUMNS",
    "SilverReportFactory",
    "build_funding_1m_feature_for_symbol",
    "build_funding_observed_for_symbol",
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
class FundingDependencies:
    """Shared Silver helpers required by funding transformations."""

    require_polars: Callable[[], Any]
    discover_months: Callable[..., list[str]]
    bronze_month_files: Callable[..., list[str]]
    silver_month_path: Callable[..., Path]
    silver_funding_feature_month_path: Callable[..., Path]
    iso_utc: Callable[[datetime | None], str | None]
    report_factory: SilverReportFactory


def build_funding_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    dependencies: FundingDependencies,
) -> object:
    """Build monthly ``funding_observed`` Silver outputs and aggregated report."""

    pl = dependencies.require_polars()
    months = dependencies.discover_months(
        bronze_root=bronze_root,
        market="funding",
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        instrument_type="perp",
    )
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    agg_invalid_rows = 0
    agg_null_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = dependencies.bronze_month_files(
            bronze_root=bronze_root,
            market="funding",
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
                pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("funding_time"),
                pl.col("funding_rate").cast(pl.Float64),
                pl.col("symbol").cast(pl.Utf8).alias("symbol"),
                pl.col("exchange").cast(pl.Utf8).alias("exchange"),
                pl.col("instrument_type").cast(pl.Utf8).alias("instrument_type"),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
            ]
        )
        frame = frame.filter(pl.col("instrument_type") == "perp")

        null_rate_expr = pl.col("funding_rate").is_null()
        invalid_rate_expr = (~null_rate_expr) & (
            ~pl.col("funding_rate").is_finite() | (pl.col("funding_rate").abs() > 1.0)
        )
        null_rows = frame.select(null_rate_expr.cast(pl.Int64).sum().alias("count")).item()
        invalid_rows = frame.select(invalid_rate_expr.cast(pl.Int64).sum().alias("count")).item()
        cleaned = frame.filter(~null_rate_expr & ~invalid_rate_expr)

        observed = (
            cleaned.group_by(["exchange", "symbol", "funding_time"], maintain_order=True)
            .agg(
                [
                    pl.col("funding_rate").last(),
                    pl.col("instrument_type").last(),
                    pl.col("ingested_at").min().alias("ingested_at_min"),
                    pl.col("ingested_at").max().alias("ingested_at_max"),
                    pl.len().cast(pl.Int64).alias("source_row_count"),
                ]
            )
            .with_columns(
                [
                    pl.col("symbol").str.split("-").list.first().alias("base_asset"),
                    pl.lit(8).cast(pl.Int64).alias("funding_interval_hours"),
                    pl.lit(datetime.now(UTC))
                    .cast(pl.Datetime(time_unit="us", time_zone="UTC"))
                    .alias("silver_built_at"),
                    pl.lit("ok").alias("data_quality_status"),
                ]
            )
            .select(
                [
                    "funding_time",
                    "exchange",
                    "symbol",
                    "base_asset",
                    "instrument_type",
                    "funding_rate",
                    "funding_interval_hours",
                    "ingested_at_min",
                    "ingested_at_max",
                    "source_row_count",
                    "silver_built_at",
                    "data_quality_status",
                ]
            )
            .sort("funding_time")
        )

        duplicates_removed = cleaned.height - observed.height
        target = dependencies.silver_month_path(
            silver_root=silver_root,
            market="funding_observed",
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        observed.write_parquet(target)

        month_min = observed.select(pl.col("funding_time").min()).item()
        month_max = observed.select(pl.col("funding_time").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += rows_in
        agg_rows_out += observed.height
        agg_duplicates_removed += int(duplicates_removed)
        agg_invalid_rows += int(invalid_rows)
        agg_null_rows += int(null_rows)

    return dependencies.report_factory(
        dataset="funding_observed",
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
        null_price_rows=agg_null_rows,
        min_timestamp=dependencies.iso_utc(min_timestamp),
        max_timestamp=dependencies.iso_utc(max_timestamp),
        symbols=[symbol],
        columns=SILVER_FUNDING_OBSERVED_COLUMNS,
    )


def build_funding_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    observed_timeframe: str = "8h",
    cutoff_time: datetime | None = None,
    dependencies: FundingDependencies,
) -> object:
    """Build monthly ``funding_1m_feature`` from observed funding using backward asof joins."""

    if cutoff_time is None:
        cutoff_time = datetime.now(UTC)

    pl = dependencies.require_polars()
    observed_root = (
        Path(silver_root)
        / "dataset_type=funding_observed"
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
        observed = pl.read_parquet(month_file).sort("funding_time")
        if observed.height == 0 or month == "9999-12":
            continue

        month_start = datetime.fromisoformat(f"{month}-01T00:00:00+00:00")
        observed_max = observed.select(pl.col("funding_time").max()).item()
        if not isinstance(observed_max, datetime):
            continue
        month_end_exclusive = observed_max + timedelta(minutes=1)
        if cutoff_time < observed_max:
            month_end_exclusive = cutoff_time + timedelta(minutes=1)
        calendar = pl.DataFrame(
            {
                "timestamp": pl.datetime_range(
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
                pl.col("funding_time"),
                pl.col("funding_rate").alias("funding_rate_last_known"),
                pl.col("funding_time").alias("funding_observed_at"),
            ]
        )
        joined = calendar.join_asof(
            right,
            left_on="timestamp",
            right_on="funding_time",
            strategy="backward",
        )
        feature = (
            joined.with_columns(
                [
                    pl.lit(exchange).alias("exchange"),
                    pl.lit(symbol).alias("symbol"),
                    ((pl.col("timestamp") - pl.col("funding_observed_at")).dt.total_minutes().cast(pl.Int64)).alias(
                        "minutes_since_funding"
                    ),
                    (pl.col("timestamp") == pl.col("funding_observed_at"))
                    .fill_null(False)
                    .alias("is_funding_observation_minute"),
                    pl.col("funding_observed_at").is_not_null().alias("funding_data_available"),
                ]
            )
            .select(
                [
                    "timestamp",
                    "exchange",
                    "symbol",
                    "funding_rate_last_known",
                    "funding_observed_at",
                    "minutes_since_funding",
                    "is_funding_observation_minute",
                    "funding_data_available",
                ]
            )
            .sort("timestamp")
        )

        leakage_count = feature.filter(
            pl.col("funding_observed_at").is_not_null() & (pl.col("funding_observed_at") > pl.col("timestamp"))
        ).height
        if leakage_count > 0:
            raise ValueError(f"Funding leakage detected for {exchange}/{symbol}/{month}: {leakage_count} rows")

        target = dependencies.silver_funding_feature_month_path(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        feature.write_parquet(target)

        month_min = feature.select(pl.col("timestamp").min()).item()
        month_max = feature.select(pl.col("timestamp").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += observed.height
        agg_rows_out += feature.height

    return dependencies.report_factory(
        dataset="funding_1m_feature",
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
        columns=SILVER_FUNDING_FEATURE_COLUMNS,
    )
