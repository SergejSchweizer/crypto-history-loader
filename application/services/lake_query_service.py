"""Application-facing query helpers for parquet lake reads."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ingestion.lake_dataframe import load_combined_dataframe_from_lake
from ingestion.lake_queries import latest_open_time_in_lake as _latest_open_time_in_lake
from ingestion.lake_queries import latest_open_time_in_lake_by_dataset as _latest_open_time_in_lake_by_dataset
from ingestion.lake_queries import open_times_in_lake as _open_times_in_lake
from ingestion.lake_queries import open_times_in_lake_by_dataset as _open_times_in_lake_by_dataset


def open_times_in_lake(**kwargs: Any) -> list[datetime]:
    """Return persisted OHLCV open times from the lake adapter."""

    return _open_times_in_lake(**kwargs)


def open_times_in_lake_by_dataset(**kwargs: Any) -> list[datetime]:
    """Return persisted open times for a specific lake dataset."""

    return _open_times_in_lake_by_dataset(**kwargs)


def latest_open_time_in_lake(**kwargs: Any) -> datetime | None:
    """Return the latest persisted OHLCV open time from the lake adapter."""

    return _latest_open_time_in_lake(**kwargs)


def latest_open_time_in_lake_by_dataset(**kwargs: Any) -> datetime | None:
    """Return the latest persisted open time for a specific lake dataset."""

    return _latest_open_time_in_lake_by_dataset(**kwargs)


def load_combined_ohlcv_dataframe(
    *,
    lake_root: str,
    exchanges: list[str] | None,
    symbols: list[str] | None,
    timeframes: list[str] | None,
    instrument_types: list[str] | None,
    start_time: datetime,
    end_time: datetime,
) -> Any:
    """Load combined OHLCV rows for descriptive statistics exports."""

    return load_combined_dataframe_from_lake(
        lake_root=lake_root,
        exchanges=exchanges,
        symbols=symbols,
        timeframes=timeframes,
        instrument_types=instrument_types,
        start_time=start_time,
        end_time=end_time,
        include_open_interest=False,
    )
