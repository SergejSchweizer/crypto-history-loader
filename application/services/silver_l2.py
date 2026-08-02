"""Normalize L2 snapshots and derive deterministic minute-level liquidity features."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import SILVER_L2_FEATURE_COLUMNS, SILVER_L2_OBSERVED_COLUMNS
from application.services.silver_partition_manifest import (
    load_current_manifest,
    publish_partition_atomically,
    source_fingerprint,
)

DEPTH_BANDS_BPS = (10, 50)
_L2_OBSERVED_CONTRACT_VERSION = "silver-l2-observed/v1"
_L2_FEATURE_CONTRACT_VERSION = "silver-l2-feature/v1"


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
class L2Dependencies:
    """Shared Silver helpers required by L2 transformations."""

    require_polars: Any
    silver_month_path: Any
    iso_utc: Any
    report_factory: SilverReportFactory


def discover_l2_symbols(
    *,
    bronze_root: str,
    exchange: str,
    dataset_type: str = "perps_l2_snapshot_1m",
    instrument_type: str = "perp",
) -> list[str]:
    """Discover symbols in one Bronze L2 dataset family."""

    root = (
        Path(bronze_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
    )
    if not root.exists():
        return []
    return sorted(
        path.name.split("=", 1)[1].upper()
        for path in root.glob("symbol=*")
        if path.is_dir() and path.name.startswith("symbol=")
    )


def _symbol_root(
    *,
    bronze_root: str,
    dataset_type: str,
    exchange: str,
    instrument_type: str,
    symbol: str,
) -> Path:
    return (
        Path(bronze_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"symbol={symbol}"
    )


def _months(root: Path) -> list[str]:
    if not root.exists():
        return []
    months: set[str] = set()
    for path in root.glob("**/year=*/month=*"):
        year = path.parent.name.split("=", 1)[1]
        month = path.name.split("=", 1)[1]
        months.add(f"{year}-{month}" if len(month) == 2 else month)
    return sorted(months)


def _month_files(root: Path, month: str) -> list[str]:
    year, month_part = month.split("-", 1)
    return sorted(str(path) for path in root.glob(f"**/year={year}/month={month_part}/date=*/hour=*/data.parquet"))


def _collect_files(pl: Any, files: list[str]) -> Any:
    try:
        return pl.scan_parquet(files).collect()
    except Exception as exc:
        if exc.__class__.__name__ != "SchemaError":
            raise
    return pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")


def _book_valid_expr(pl: Any, column: str, *, descending: bool) -> Any:
    prices = pl.col(column).list.eval(pl.element().struct.field("price"))
    amounts = pl.col(column).list.eval(pl.element().struct.field("amount"))
    return (
        pl.col(column).is_not_null()
        & prices.list.eval(pl.element().is_finite() & (pl.element() > 0.0)).list.all()
        & amounts.list.eval(pl.element().is_finite() & (pl.element() >= 0.0)).list.all()
        & (prices == prices.list.sort(descending=descending))
    )


def _observed_frame(pl: Any, frame: Any, normalized_symbol: str, instrument_type: str) -> tuple[Any, int, int]:
    bids_valid = _book_valid_expr(pl, "bids", descending=True)
    asks_valid = _book_valid_expr(pl, "asks", descending=False)
    best_bid = pl.col("bids").list.get(0, null_on_oob=True).struct.field("price")
    best_bid_size = pl.col("bids").list.get(0, null_on_oob=True).struct.field("amount")
    best_ask = pl.col("asks").list.get(0, null_on_oob=True).struct.field("price")
    best_ask_size = pl.col("asks").list.get(0, null_on_oob=True).struct.field("amount")
    instrument_name = (
        (
            pl.coalesce([pl.col("instrument_name"), pl.col("symbol")])
            if "instrument_name" in frame.columns
            else pl.col("symbol")
        )
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.to_uppercase()
    )
    if instrument_type == "option":
        underlying = instrument_name.str.extract(r"^([A-Z0-9]+)-[0-9]{1,2}[A-Z]{3}[0-9]{2}-[0-9.]+-[CP]$", 1)
        expiry = instrument_name.str.extract(r"^[A-Z0-9]+-([0-9]{1,2}[A-Z]{3}[0-9]{2})-[0-9.]+-[CP]$", 1).str.strptime(
            pl.Date, "%d%b%y", strict=False
        )
        strike = instrument_name.str.extract(r"^[A-Z0-9]+-[0-9]{1,2}[A-Z]{3}[0-9]{2}-([0-9.]+)-[CP]$", 1).cast(
            pl.Float64, strict=False
        )
        option_type = instrument_name.str.extract(r"^[A-Z0-9]+-[0-9]{1,2}[A-Z]{3}[0-9]{2}-[0-9.]+-([CP])$", 1)
    else:
        underlying = pl.lit(normalized_symbol.removesuffix("-PERPETUAL"), dtype=pl.Utf8)
        expiry = pl.lit(None, dtype=pl.Date)
        strike = pl.lit(None, dtype=pl.Float64)
        option_type = pl.lit(None, dtype=pl.Utf8)
    normalized = frame.with_columns(
        [
            pl.col("event_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp"),
            pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
            pl.lit(normalized_symbol).alias("symbol"),
            pl.col("instrument_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
            instrument_name.alias("instrument_name"),
            underlying.alias("underlying"),
            expiry.alias("expiry"),
            strike.alias("strike"),
            option_type.alias("option_type"),
            best_bid.cast(pl.Float64).alias("best_bid_price"),
            best_bid_size.cast(pl.Float64).alias("best_bid_size"),
            best_ask.cast(pl.Float64).alias("best_ask_price"),
            best_ask_size.cast(pl.Float64).alias("best_ask_size"),
            pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
            pl.col("source").cast(pl.Utf8).alias("source_endpoint"),
        ]
    )
    crossed = (
        pl.col("best_bid_price").is_not_null()
        & pl.col("best_ask_price").is_not_null()
        & (pl.col("best_bid_price") >= pl.col("best_ask_price"))
    )
    invalid = pl.col("timestamp").is_null() | (~bids_valid.fill_null(False)) | (~asks_valid.fill_null(False)) | crossed
    if instrument_type == "option":
        invalid = (
            invalid
            | pl.col("underlying").is_null()
            | pl.col("expiry").is_null()
            | pl.col("strike").is_null()
            | (pl.col("strike") <= 0.0)
            | (~pl.col("option_type").is_in(["C", "P"]))
        )
    invalid_rows = int(normalized.select(invalid.cast(pl.Int64).sum()).item())
    cleaned = normalized.filter(~invalid)
    observed = (
        cleaned.sort(["timestamp", "ingested_at"])
        .unique(subset=["exchange", "instrument_name", "timestamp"], keep="last", maintain_order=True)
        .sort(["exchange", "symbol", "instrument_name", "timestamp"])
        .select(SILVER_L2_OBSERVED_COLUMNS)
    )
    return observed, invalid_rows, cleaned.height - observed.height


def _depth_within_bps(row: dict[str, Any], *, side: str, bps: int) -> float | None:
    mid = row.get("mid_price")
    levels = row.get(side)
    if not isinstance(mid, int | float) or not isinstance(levels, list):
        return None
    threshold = float(mid) * (1.0 - bps / 10_000.0 if side == "bids" else 1.0 + bps / 10_000.0)
    total = 0.0
    for level in levels:
        if not isinstance(level, dict):
            continue
        price = level.get("price")
        amount = level.get("amount")
        if not isinstance(price, int | float) or not isinstance(amount, int | float):
            continue
        inside = float(price) >= threshold if side == "bids" else float(price) <= threshold
        if inside:
            total += float(amount)
    return total


def _feature_frame(pl: Any, observed: Any) -> tuple[Any, int]:
    latest = (
        observed.with_columns(pl.col("timestamp").dt.truncate("1m").alias("timestamp_m1"))
        .sort(["exchange", "instrument_name", "timestamp_m1", "timestamp", "ingested_at"])
        .unique(subset=["exchange", "instrument_name", "timestamp_m1"], keep="last", maintain_order=True)
    )
    quote_available = pl.col("best_bid_price").is_not_null() & pl.col("best_ask_price").is_not_null()
    quote_age_seconds = (
        (pl.col("ingested_at") - pl.col("timestamp")).dt.total_milliseconds().cast(pl.Float64) / 1000.0
    ).clip(lower_bound=0.0)
    feature = latest.with_columns(
        [
            quote_available.alias("quote_available"),
            pl.when(quote_available)
            .then((pl.col("best_bid_price") + pl.col("best_ask_price")) / 2.0)
            .otherwise(None)
            .alias("mid_price"),
            pl.when(quote_available)
            .then(pl.col("best_ask_price") - pl.col("best_bid_price"))
            .otherwise(None)
            .alias("spread"),
            pl.col("best_bid_size").alias("top_bid_size"),
            pl.col("best_ask_size").alias("top_ask_size"),
            quote_age_seconds.alias("quote_age_seconds"),
        ]
    ).with_columns(
        [
            pl.when((pl.col("top_bid_size") + pl.col("top_ask_size")) > 0.0)
            .then((pl.col("top_bid_size") - pl.col("top_ask_size")) / (pl.col("top_bid_size") + pl.col("top_ask_size")))
            .otherwise(None)
            .alias("top_of_book_imbalance"),
            (pl.col("quote_age_seconds") > 60.0).alias("stale_quote"),
            (pl.col("quote_age_seconds") / 60.0).floor().cast(pl.Int64).alias("minutes_since_l2_observation"),
        ]
    )
    depth_columns = []
    for bps in DEPTH_BANDS_BPS:
        for side in ("bids", "asks"):
            depth_columns.append(
                pl.struct([side, "mid_price"])
                .map_elements(
                    lambda row, side=side, bps=bps: _depth_within_bps(row, side=side, bps=bps),
                    return_dtype=pl.Float64,
                )
                .alias(f"{side[:-1]}_depth_{bps}bps")
            )
    feature = (
        feature.with_columns(depth_columns)
        .sort(["exchange", "symbol", "instrument_name", "timestamp_m1"])
        .select(SILVER_L2_FEATURE_COLUMNS)
    )
    return feature, observed.height - latest.height


def _observed_months(*, silver_root: str, dataset_type: str, exchange: str, symbol: str, timeframe: str) -> list[str]:
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


def build_l2_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    bronze_dataset_type: str = "perps_l2_snapshot_1m",
    output_dataset_type: str = "perps_l2_snapshot_1m_observed",
    instrument_type: str = "perp",
    dependencies: L2Dependencies,
) -> object:
    """Build validated observed L2 snapshots for one symbol."""

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    root = _symbol_root(
        bronze_root=bronze_root,
        dataset_type=bronze_dataset_type,
        exchange=exchange,
        instrument_type=instrument_type,
        symbol=normalized_symbol,
    )
    months = _months(root)
    rows_in = 0
    rows_out = 0
    duplicates_removed = 0
    invalid_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None
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
            builder_contract_version=_L2_OBSERVED_CONTRACT_VERSION,
        )
        cached = load_current_manifest(
            parquet_path=target,
            expected_input_fingerprint=fingerprint,
            expected_builder_contract_version=_L2_OBSERVED_CONTRACT_VERSION,
        )
        if cached is not None:
            processed.append(month)
            rows_out += cached.row_count
            continue
        frame = _collect_files(pl, files)
        rows_in += frame.height
        observed, month_invalid, month_duplicates = _observed_frame(pl, frame, normalized_symbol, instrument_type)
        invalid_rows += month_invalid
        duplicates_removed += month_duplicates
        if observed.height == 0:
            continue
        publish_partition_atomically(
            frame=observed,
            parquet_path=target,
            input_fingerprint=fingerprint,
            source_schema=source_schema,
            sort_keys=("timestamp",),
            deduplication_keys=("exchange", "symbol", "instrument_type", "timestamp"),
            builder_contract_version=_L2_OBSERVED_CONTRACT_VERSION,
        )
        processed.append(month)
        rows_out += observed.height
        month_min = observed.select(pl.col("timestamp").min()).item()
        month_max = observed.select(pl.col("timestamp").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

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
        min_timestamp=dependencies.iso_utc(min_timestamp),
        max_timestamp=dependencies.iso_utc(max_timestamp),
        symbols=[normalized_symbol],
        columns=SILVER_L2_OBSERVED_COLUMNS,
    )


def build_l2_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    observed_dataset_type: str = "perps_l2_snapshot_1m_observed",
    output_dataset_type: str = "perps_l2_1m_feature",
    dependencies: L2Dependencies,
) -> object:
    """Build one deterministic L2 liquidity row per observed minute."""

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    months = _observed_months(
        silver_root=silver_root,
        dataset_type=observed_dataset_type,
        exchange=exchange,
        symbol=normalized_symbol,
        timeframe=timeframe,
    )
    rows_in = 0
    rows_out = 0
    duplicates_removed = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None
    processed: list[str] = []

    for month in months:
        source = dependencies.silver_month_path(
            silver_root=silver_root,
            market=observed_dataset_type,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            month=month,
        )
        if not source.exists():
            continue
        observed = pl.read_parquet(source).select(SILVER_L2_OBSERVED_COLUMNS)
        rows_in += observed.height
        feature, month_duplicates = _feature_frame(pl, observed)
        duplicates_removed += month_duplicates
        if feature.height == 0:
            continue
        target = dependencies.silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            month=month,
        )
        source_schema = dict(pl.scan_parquet(str(source)).collect_schema())
        fingerprint = source_fingerprint(
            bronze_root=Path(silver_root),
            source_files=[str(source)],
            source_schema=source_schema,
            exchange=exchange,
            symbol=normalized_symbol,
            timeframe=timeframe,
            builder_contract_version=_L2_FEATURE_CONTRACT_VERSION,
        )
        publish_partition_atomically(
            frame=feature,
            parquet_path=target,
            input_fingerprint=fingerprint,
            source_schema=source_schema,
            sort_keys=("exchange", "symbol", "timestamp_m1"),
            deduplication_keys=("exchange", "symbol", "timestamp_m1"),
            builder_contract_version=_L2_FEATURE_CONTRACT_VERSION,
        )
        processed.append(month)
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
        period_start=processed[0] if processed else None,
        period_end=processed[-1] if processed else None,
        months_processed=processed,
        rows_in=rows_in,
        rows_out=rows_out,
        duplicates_removed=duplicates_removed,
        invalid_ohlc_rows=0,
        null_price_rows=0,
        min_timestamp=dependencies.iso_utc(min_timestamp),
        max_timestamp=dependencies.iso_utc(max_timestamp),
        symbols=[normalized_symbol],
        columns=SILVER_L2_FEATURE_COLUMNS,
    )
