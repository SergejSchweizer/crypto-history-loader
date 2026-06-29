"""Polars dataframe readers for Bronze parquet lake data."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from ingestion.lake_datasets import OI_DATASET_TYPE
from ingestion.lake_layout import dataset_data_files

OHLCV_COLUMNS = [
    "exchange",
    "instrument_type",
    "symbol",
    "timeframe",
    "open_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "dataset_type",
    "run_id",
    "source_endpoint",
]


def load_combined_dataframe_from_lake(
    lake_root: str,
    exchanges: list[str] | None = None,
    symbols: list[str] | None = None,
    timeframes: list[str] | None = None,
    instrument_types: list[str] | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int | None = None,
    include_open_interest: bool = False,
) -> Any:
    """Load combined spot/perp OHLCV rows from the Bronze lake as a Polars DataFrame.

    Args:
        lake_root: Root directory containing Bronze parquet partitions.
        exchanges: Optional exchange partition filter, matched case-insensitively.
        symbols: Optional symbol partition filter, matched in uppercase form.
        timeframes: Optional timeframe partition filter, matched case-insensitively.
        instrument_types: Optional instrument type partition filter, matched case-insensitively.
        start_time: Optional inclusive lower bound for ``open_time``.
        end_time: Optional inclusive upper bound for ``open_time``.
        limit: Optional maximum number of sorted OHLCV rows to return.
        include_open_interest: Whether to left-join matching open-interest values.

    Returns:
        A Polars DataFrame with the stable OHLCV export columns, plus open-interest
        columns when requested.

    Raises:
        RuntimeError: Polars is not installed.
        ValueError: ``limit`` is zero or negative.
    """

    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")

    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for dataframe export. Install project dependencies.") from exc

    filters = _PartitionFilters.from_values(
        exchanges=exchanges,
        symbols=symbols,
        timeframes=timeframes,
        instrument_types=instrument_types,
    )
    data_files = sorted([*dataset_data_files(lake_root, "spot"), *dataset_data_files(lake_root, "perp")])

    frames = _load_filtered_frames(
        pl=pl,
        data_files=data_files,
        filters=filters,
        start_time=start_time,
        end_time=end_time,
        normalize_price_columns=True,
    )
    dataframe = _combined_ohlcv_frame(pl=pl, frames=frames, limit=limit)

    if include_open_interest:
        dataframe = _join_open_interest(
            pl=pl,
            dataframe=dataframe,
            lake_root=lake_root,
            filters=filters,
            start_time=start_time,
            end_time=end_time,
        )

    return dataframe


class _PartitionFilters:
    """Normalized path-partition filters for lake dataframe reads."""

    def __init__(
        self,
        exchange_filter: set[str] | None,
        symbol_filter: set[str] | None,
        timeframe_filter: set[str] | None,
        instrument_filter: set[str] | None,
    ) -> None:
        self.exchange_filter = exchange_filter
        self.symbol_filter = symbol_filter
        self.timeframe_filter = timeframe_filter
        self.instrument_filter = instrument_filter

    @classmethod
    def from_values(
        cls,
        exchanges: list[str] | None,
        symbols: list[str] | None,
        timeframes: list[str] | None,
        instrument_types: list[str] | None,
    ) -> _PartitionFilters:
        """Create normalized partition filters from user-facing selector values."""

        return cls(
            exchange_filter={item.lower() for item in exchanges} if exchanges else None,
            symbol_filter={item.upper() for item in symbols} if symbols else None,
            timeframe_filter={item.lower() for item in timeframes} if timeframes else None,
            instrument_filter={item.lower() for item in instrument_types} if instrument_types else None,
        )

    def matches(self, partition_values: dict[str, str]) -> bool:
        """Return whether a partition path satisfies all configured selectors."""

        exchange_value = partition_values.get("exchange", "")
        instrument_value = partition_values.get("instrument_type", "")
        symbol_value = partition_values.get("symbol", "")
        timeframe_value = partition_values.get("timeframe", "")

        if self.exchange_filter is not None and exchange_value.lower() not in self.exchange_filter:
            return False
        if self.instrument_filter is not None and instrument_value.lower() not in self.instrument_filter:
            return False
        if self.symbol_filter is not None and symbol_value.upper() not in self.symbol_filter:
            return False
        return not (self.timeframe_filter is not None and timeframe_value.lower() not in self.timeframe_filter)


def _load_filtered_frames(
    pl: Any,
    data_files: list[Path],
    filters: _PartitionFilters,
    start_time: datetime | None,
    end_time: datetime | None,
    normalize_price_columns: bool,
) -> list[Any]:
    frames: list[Any] = []
    for data_file in data_files:
        if not filters.matches(_partition_values(data_file)):
            continue

        frame = pl.read_parquet(str(data_file))
        if normalize_price_columns:
            frame = _normalize_price_columns(pl=pl, frame=frame)
        frame = _filter_open_time(pl=pl, frame=frame, start_time=start_time, end_time=end_time)
        if frame.height == 0:
            continue
        frames.append(frame)
    return frames


def _partition_values(data_file: Path) -> dict[str, str]:
    partition_values: dict[str, str] = {}
    for segment in data_file.parts:
        if "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        partition_values[key] = value
    return partition_values


def _normalize_price_columns(pl: Any, frame: Any) -> Any:
    rename_map = {
        "open_price": "open",
        "high_price": "high",
        "low_price": "low",
        "close_price": "close",
    }
    for source_col, target_col in rename_map.items():
        if target_col not in frame.columns and source_col in frame.columns:
            frame = frame.with_columns(pl.col(source_col).alias(target_col))
    return frame


def _filter_open_time(pl: Any, frame: Any, start_time: datetime | None, end_time: datetime | None) -> Any:
    if start_time is not None:
        frame = frame.filter(pl.col("open_time") >= start_time)
    if end_time is not None:
        frame = frame.filter(pl.col("open_time") <= end_time)
    return frame


def _combined_ohlcv_frame(pl: Any, frames: list[Any], limit: int | None) -> Any:
    if not frames:
        return pl.DataFrame(schema={name: pl.Null for name in OHLCV_COLUMNS})

    dataframe = pl.concat(frames, how="diagonal_relaxed")
    dataframe = dataframe.sort(by=["open_time", "exchange", "instrument_type", "symbol", "timeframe"])
    for col_name in OHLCV_COLUMNS:
        if col_name not in dataframe.columns:
            dataframe = dataframe.with_columns(pl.lit(None).alias(col_name))
    dataframe = dataframe.select(OHLCV_COLUMNS)
    if limit is not None:
        dataframe = dataframe.head(limit)
    return dataframe


def _join_open_interest(
    pl: Any,
    dataframe: Any,
    lake_root: str,
    filters: _PartitionFilters,
    start_time: datetime | None,
    end_time: datetime | None,
) -> Any:
    oi_frames = _load_filtered_frames(
        pl=pl,
        data_files=dataset_data_files(lake_root, OI_DATASET_TYPE),
        filters=filters,
        start_time=start_time,
        end_time=end_time,
        normalize_price_columns=False,
    )
    if not oi_frames:
        return dataframe.with_columns([pl.lit(None).alias("open_interest"), pl.lit(None).alias("open_interest_value")])

    oi_frame = (
        pl.concat(oi_frames, how="diagonal_relaxed")
        .sort(by=["open_time"])
        .unique(
            subset=["exchange", "instrument_type", "symbol", "timeframe", "open_time"],
            keep="last",
        )
    )
    oi_frame = oi_frame.select(
        [
            "exchange",
            "instrument_type",
            "symbol",
            "timeframe",
            "open_time",
            "open_interest",
            "open_interest_value",
        ]
    )
    return dataframe.join(
        oi_frame,
        on=["exchange", "instrument_type", "symbol", "timeframe", "open_time"],
        how="left",
    )
