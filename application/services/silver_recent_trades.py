"""Normalize recent-trade snapshots and reconcile them with historical trade Silver data."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from application.dataset_contracts import (
    SILVER_RECENT_TRADE_SNAPSHOT_OBSERVED_COLUMNS,
    SILVER_TRADES_OBSERVED_COLUMNS,
)
from application.services.silver_partition_manifest import (
    load_current_manifest,
    publish_partition_atomically,
    source_fingerprint,
)

_RECENT_TRADES_CONTRACT_VERSION = "silver-recent-trades-observed/v1"


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
class RecentTradeDependencies:
    """Shared Silver helpers required by recent-trade transformations."""

    require_polars: Any
    silver_month_path: Any
    iso_utc: Any
    report_factory: SilverReportFactory


def discover_recent_trade_symbols(
    *, bronze_root: str, exchange: str, dataset_type: str = "recent_trade_snapshot_1m"
) -> list[str]:
    """Discover currencies available in recent-trade Bronze partitions."""

    root = Path(bronze_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
    if not root.exists():
        return []
    return sorted(
        {
            path.name.split("=", 1)[1].upper()
            for path in root.glob("instrument_type=*/currency=*")
            if path.is_dir() and path.name.startswith("currency=")
        }
    )


def _currency_root(*, bronze_root: str, dataset_type: str, exchange: str) -> Path:
    return Path(bronze_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"


def _months(root: Path, symbol: str) -> list[str]:
    months: set[str] = set()
    for path in root.glob(f"instrument_type=*/currency={symbol}/**/year=*/month=*"):
        year = path.parent.name.split("=", 1)[1]
        month = path.name.split("=", 1)[1]
        months.add(f"{year}-{month}" if len(month) == 2 else month)
    return sorted(months)


def _month_files(root: Path, symbol: str, month: str) -> list[str]:
    year, month_part = month.split("-", 1)
    return sorted(
        str(path)
        for path in root.glob(
            f"instrument_type=*/currency={symbol}/**/year={year}/month={month_part}/date=*/hour=*/data.parquet"
        )
    )


def _collect_files(pl: Any, files: list[str]) -> Any:
    try:
        return pl.scan_parquet(files).collect()
    except Exception as exc:
        if exc.__class__.__name__ != "SchemaError":
            raise
    return pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")


def _normalized_frame(pl: Any, frame: Any, normalized_symbol: str) -> tuple[Any, int, int]:
    instrument_name = pl.col("instrument_name").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    instrument_type = pl.col("instrument_type").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    source_trade_id = (
        pl.when(pl.col("trade_id").cast(pl.Utf8).str.strip_chars().str.len_chars() > 0)
        .then(pl.col("trade_id").cast(pl.Utf8).str.strip_chars())
        .otherwise(None)
    )
    option_expiry = instrument_name.str.extract(
        r"^[A-Z0-9]+-([0-9]{1,2}[A-Z]{3}[0-9]{2})-[0-9.]+-[CP]$", 1
    ).str.strptime(pl.Date, "%d%b%y", strict=False)
    future_expiry = instrument_name.str.extract(r"^[A-Z0-9]+-([0-9]{1,2}[A-Z]{3}[0-9]{2})$", 1).str.strptime(
        pl.Date, "%d%b%y", strict=False
    )
    typed = frame.with_columns(
        [
            pl.col("exchange_timestamp").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("trade_time"),
            pl.col("snapshot_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("snapshot_timestamp"),
            pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
            pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase(),
            pl.lit(normalized_symbol).alias("symbol"),
            instrument_type.alias("instrument_type"),
            instrument_name.alias("instrument_name"),
            instrument_name.str.extract(r"^([A-Z0-9]+)-", 1).alias("underlying"),
            pl.when(instrument_type == "option")
            .then(option_expiry)
            .when(instrument_type == "future")
            .then(future_expiry)
            .otherwise(None)
            .alias("expiry"),
            pl.when(instrument_type == "option")
            .then(
                instrument_name.str.extract(r"^[A-Z0-9]+-[0-9]{1,2}[A-Z]{3}[0-9]{2}-([0-9.]+)-[CP]$", 1).cast(
                    pl.Float64, strict=False
                )
            )
            .otherwise(None)
            .alias("strike"),
            pl.when(instrument_type == "option")
            .then(instrument_name.str.extract(r"^[A-Z0-9]+-[0-9]{1,2}[A-Z]{3}[0-9]{2}-[0-9.]+-([CP])$", 1))
            .otherwise(None)
            .alias("option_type"),
            source_trade_id.alias("trade_id"),
            source_trade_id.is_not_null().alias("trade_id_is_source"),
            pl.col("price").cast(pl.Float64),
            pl.col("amount").cast(pl.Float64).alias("quantity"),
            pl.when(pl.col("direction").cast(pl.Utf8).str.to_lowercase().is_in(["buy", "sell"]))
            .then(pl.col("direction").cast(pl.Utf8).str.to_lowercase())
            .otherwise(pl.lit("unknown"))
            .alias("side"),
            pl.lit(True).alias("snapshot_derived"),
            pl.col("source").cast(pl.Utf8).alias("source_endpoint"),
        ]
    )
    invalid = (
        pl.col("trade_time").is_null()
        | pl.col("snapshot_timestamp").is_null()
        | pl.col("instrument_name").is_null()
        | pl.col("underlying").is_null()
        | pl.col("price").is_null()
        | (~pl.col("price").is_finite())
        | (pl.col("price") <= 0.0)
        | pl.col("quantity").is_null()
        | (~pl.col("quantity").is_finite())
        | (pl.col("quantity") <= 0.0)
        | (
            (pl.col("instrument_type") == "option")
            & (pl.col("expiry").is_null() | pl.col("strike").is_null() | (~pl.col("option_type").is_in(["C", "P"])))
        )
    )
    invalid_rows = int(typed.select(invalid.cast(pl.Int64).sum()).item())
    cleaned = typed.filter(~invalid).with_columns(
        pl.when(pl.col("trade_id_is_source"))
        .then(pl.concat_str([pl.lit("id:"), pl.col("trade_id")]))
        .otherwise(
            pl.concat_str(
                [
                    pl.lit("fallback:"),
                    pl.col("exchange"),
                    pl.col("instrument_name"),
                    pl.col("trade_time").cast(pl.Utf8),
                    pl.col("price").cast(pl.Utf8),
                    pl.col("quantity").cast(pl.Utf8),
                    pl.col("side"),
                ],
                separator="|",
            )
        )
        .alias("deduplication_key")
    )
    observed = (
        cleaned.sort(["deduplication_key", "snapshot_timestamp", "ingested_at"])
        .unique(subset=["exchange", "deduplication_key"], keep="last", maintain_order=True)
        .sort(["trade_time", "instrument_name", "deduplication_key"])
        .select(SILVER_RECENT_TRADE_SNAPSHOT_OBSERVED_COLUMNS)
    )
    return observed, invalid_rows, cleaned.height - observed.height


def _reference_paths(*, silver_root: str, exchange: str, symbol: str, timeframe: str, month: str) -> list[Path]:
    paths: list[Path] = []
    year = month[:4]
    for dataset_type, reference_symbol in (
        ("perps_trades_observed", f"{symbol}-PERPETUAL"),
        ("options_trades_observed", symbol),
    ):
        path = (
            Path(silver_root)
            / f"dataset_type={dataset_type}"
            / f"exchange={exchange}"
            / f"symbol={reference_symbol}"
            / f"timeframe={timeframe}"
            / f"year={year}"
            / f"month={month}"
            / f"{reference_symbol}-{month}.parquet"
        )
        if path.exists():
            paths.append(path)
    return paths


def _write_reconciliation(*, pl: Any, observed: Any, target: Path, reference_paths: list[Path], month: str) -> None:
    report: dict[str, object] = {
        "dataset": "recent_trade_snapshot_1m_observed",
        "month": month,
        "coverage_type": "snapshot_derived_not_full_history",
        "reference_datasets": sorted({path.parts[-7].split("=", 1)[1] for path in reference_paths}),
        "reference_rows": 0,
        "source_trade_id_rows": int(observed["trade_id_is_source"].sum()),
        "fallback_key_rows": int((~observed["trade_id_is_source"]).sum()),
        "overlapping_trade_ids": 0,
        "recent_only_source_trade_ids": int(observed["trade_id_is_source"].sum()),
        "field_mismatch_counts": {"price": 0, "quantity": 0, "side": 0},
    }
    if reference_paths:
        references = pl.concat(
            [pl.read_parquet(path).select(SILVER_TRADES_OBSERVED_COLUMNS) for path in reference_paths],
            how="diagonal_relaxed",
        ).unique(subset=["exchange", "trade_id"], keep="last")
        source_rows = observed.filter(pl.col("trade_id_is_source"))
        overlap = source_rows.join(references, on=["exchange", "trade_id"], how="inner", suffix="_reference")
        mismatch_counts = {
            field: int(
                overlap.select(
                    (pl.col(field) != pl.col(f"{field}_reference")).fill_null(False).cast(pl.Int64).sum()
                ).item()
            )
            for field in ("price", "quantity", "side")
        }
        report.update(
            {
                "reference_rows": references.height,
                "overlapping_trade_ids": overlap.height,
                "recent_only_source_trade_ids": source_rows.height - overlap.height,
                "field_mismatch_counts": mismatch_counts,
            }
        )
    target.with_suffix(".reconciliation.json").write_text(
        json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
    )


def build_recent_trade_snapshot_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "tick",
    bronze_dataset_type: str = "recent_trade_snapshot_1m",
    output_dataset_type: str = "recent_trade_snapshot_1m_observed",
    dependencies: RecentTradeDependencies,
) -> object:
    """Build deduplicated snapshot-derived observed trades for one currency."""

    pl = dependencies.require_polars()
    normalized_symbol = symbol.upper()
    root = _currency_root(bronze_root=bronze_root, dataset_type=bronze_dataset_type, exchange=exchange)
    months = _months(root, normalized_symbol)
    rows_in = 0
    rows_out = 0
    duplicates_removed = 0
    invalid_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None
    processed: list[str] = []

    for month in months:
        files = _month_files(root, normalized_symbol, month)
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
            builder_contract_version=_RECENT_TRADES_CONTRACT_VERSION,
        )
        cached = load_current_manifest(
            parquet_path=target,
            expected_input_fingerprint=fingerprint,
            expected_builder_contract_version=_RECENT_TRADES_CONTRACT_VERSION,
        )
        if cached is not None:
            processed.append(month)
            rows_out += cached.row_count
            continue
        frame = _collect_files(pl, files)
        rows_in += frame.height
        observed, month_invalid, month_duplicates = _normalized_frame(pl, frame, normalized_symbol)
        invalid_rows += month_invalid
        duplicates_removed += month_duplicates
        if observed.height == 0:
            continue
        publish_partition_atomically(
            frame=observed,
            parquet_path=target,
            input_fingerprint=fingerprint,
            source_schema=source_schema,
            sort_keys=("trade_time",),
            deduplication_keys=("exchange", "symbol", "trade_time", "trade_id"),
            builder_contract_version=_RECENT_TRADES_CONTRACT_VERSION,
        )
        _write_reconciliation(
            pl=pl,
            observed=observed,
            target=target,
            reference_paths=_reference_paths(
                silver_root=silver_root,
                exchange=exchange,
                symbol=normalized_symbol,
                timeframe="tick",
                month=month,
            ),
            month=month,
        )
        processed.append(month)
        rows_out += observed.height
        month_min = observed.select(pl.col("trade_time").min()).item()
        month_max = observed.select(pl.col("trade_time").max()).item()
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
        columns=SILVER_RECENT_TRADE_SNAPSHOT_OBSERVED_COLUMNS,
    )
