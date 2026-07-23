"""Read-side metadata queries for Bronze parquet lake partitions."""

from __future__ import annotations

from datetime import date, datetime
from importlib import import_module
from pathlib import Path
from typing import Any, cast

from ingestion.lake_datasets import ohlcv_dataset_type_for_market
from ingestion.lake_layout import date_from_partition_path, partition_data_files, partition_empty_minute_files


def open_times_in_lake_by_dataset(
    lake_root: str,
    dataset_type: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[datetime]:
    """Return stored open_time values for selected dataset/instrument/timeframe.

    Args:
        lake_root: Root directory for Bronze parquet partitions.
        dataset_type: Bronze dataset_type partition label.
        market: Instrument type partition label.
        exchange: Exchange partition label.
        symbol: Symbol partition label.
        timeframe: Timeframe partition label.

    Returns:
        Sorted unique open_time values read in parquet batches.

    Raises:
        RuntimeError: PyArrow is unavailable.
    """

    pq = _require_pyarrow_parquet()
    partition_root = _series_partition_root(
        lake_root=lake_root,
        dataset_type=dataset_type,
        market=market,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
    if not partition_root.exists():
        return []

    values: list[datetime] = []
    for data_file in partition_data_files(partition_root):
        parquet_file = pq.ParquetFile(data_file)
        for batch in parquet_file.iter_batches(columns=["open_time"], batch_size=10_000):
            for row in batch.to_pylist():
                value = row.get("open_time")
                if isinstance(value, datetime):
                    values.append(value)
    return sorted(set(values))


def open_time_minutes_in_lake_by_dataset(
    lake_root: str,
    dataset_type: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[datetime]:
    """Return sorted unique UTC minute buckets containing at least one trade.

    Args:
        lake_root: Root directory for Bronze parquet partitions.
        dataset_type: Bronze dataset_type partition label.
        market: Instrument type partition label.
        exchange: Exchange partition label.
        symbol: Symbol partition label.
        timeframe: Timeframe partition label.

    Returns:
        Sorted unique ``open_time`` values truncated to minute precision.

    Raises:
        RuntimeError: Polars is unavailable.
    """

    partition_root = _series_partition_root(
        lake_root=lake_root,
        dataset_type=dataset_type,
        market=market,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
    if not partition_root.exists():
        return []

    data_files = [str(path) for path in partition_data_files(partition_root)]
    if not data_files:
        return []

    pl = _require_polars()
    frame = (
        pl.scan_parquet(data_files)
        .select(pl.col("open_time").dt.truncate("1m").alias("open_time"))
        .unique()
        .sort("open_time")
        .collect()
    )
    return [value for value in frame["open_time"].to_list() if isinstance(value, datetime)]


def empty_trade_minutes_in_lake_by_dataset(
    lake_root: str,
    dataset_type: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[datetime]:
    """Return sorted UTC minutes confirmed empty by successful trade fetches.

    Args:
        lake_root: Root directory for Bronze parquet partitions.
        dataset_type: Bronze dataset_type partition label.
        market: Instrument type partition label.
        exchange: Exchange partition label.
        symbol: Symbol partition label.
        timeframe: Timeframe partition label.

    Returns:
        Sorted unique ``minute`` values from ``empty_minutes.parquet`` sidecars.

    Raises:
        RuntimeError: Polars is unavailable.
    """

    partition_root = _series_partition_root(
        lake_root=lake_root,
        dataset_type=dataset_type,
        market=market,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
    if not partition_root.exists():
        return []

    empty_files = [str(path) for path in partition_empty_minute_files(partition_root)]
    if not empty_files:
        return []

    pl = _require_polars()
    frame = (
        pl.scan_parquet(empty_files)
        .filter(pl.col("status") == "confirmed_empty")
        .select(pl.col("minute"))
        .unique()
        .sort("minute")
        .collect()
    )
    return [value for value in frame["minute"].to_list() if isinstance(value, datetime)]


def partition_dates_in_lake_by_dataset(
    lake_root: str,
    dataset_type: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[date]:
    """Return stored daily partition dates for one dataset/instrument/timeframe.

    Args:
        lake_root: Root directory for Bronze parquet partitions.
        dataset_type: Bronze dataset_type partition label.
        market: Instrument type partition label.
        exchange: Exchange partition label.
        symbol: Symbol partition label.
        timeframe: Timeframe partition label.

    Returns:
        Sorted unique dates parsed from partition paths.
    """

    partition_root = _series_partition_root(
        lake_root=lake_root,
        dataset_type=dataset_type,
        market=market,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
    if not partition_root.exists():
        return []

    values: set[date] = set()
    for data_file in partition_data_files(partition_root):
        partition_date = date_from_partition_path(data_file)
        if partition_date is not None:
            values.add(partition_date)
    return sorted(values)


def open_time_bounds_in_lake_by_dataset(
    lake_root: str,
    dataset_type: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> dict[date, tuple[datetime, datetime]]:
    """Return per-partition open_time min/max bounds for a dataset selection.

    Args:
        lake_root: Root directory for Bronze parquet partitions.
        dataset_type: Bronze dataset_type partition label.
        market: Instrument type partition label.
        exchange: Exchange partition label.
        symbol: Symbol partition label.
        timeframe: Timeframe partition label.

    Returns:
        Mapping from partition date to inclusive open_time bounds.

    Raises:
        RuntimeError: PyArrow is unavailable.
    """

    pq = _require_pyarrow_parquet()
    partition_root = _series_partition_root(
        lake_root=lake_root,
        dataset_type=dataset_type,
        market=market,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
    if not partition_root.exists():
        return {}

    bounds: dict[date, tuple[datetime, datetime]] = {}
    for data_file in partition_data_files(partition_root):
        partition_date = date_from_partition_path(data_file)
        if partition_date is None:
            continue
        parquet_file = cast(Any, pq.ParquetFile(data_file))
        file_min, file_max = _open_time_bounds_for_parquet_file(parquet_file)
        if file_min is None or file_max is None:
            continue
        current = bounds.get(partition_date)
        if current is None:
            bounds[partition_date] = (file_min, file_max)
        else:
            bounds[partition_date] = (min(current[0], file_min), max(current[1], file_max))
    return bounds


def open_times_in_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[datetime]:
    """Return all stored OHLCV open_time values for one instrument/timeframe."""

    return open_times_in_lake_by_dataset(
        lake_root=lake_root,
        dataset_type=ohlcv_dataset_type_for_market(market),
        market=market,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )


def latest_open_time_in_lake_by_dataset(
    lake_root: str,
    dataset_type: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> datetime | None:
    """Return latest stored open_time for one dataset/instrument/timeframe.

    Args:
        lake_root: Root directory for Bronze parquet partitions.
        dataset_type: Bronze dataset_type partition label.
        market: Instrument type partition label.
        exchange: Exchange partition label.
        symbol: Symbol partition label.
        timeframe: Timeframe partition label.

    Returns:
        Latest open_time from the newest partition file, or None when missing.

    Raises:
        RuntimeError: PyArrow is unavailable.
    """

    pq = _require_pyarrow_parquet()
    partition_root = _series_partition_root(
        lake_root=lake_root,
        dataset_type=dataset_type,
        market=market,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
    if not partition_root.exists():
        return None

    data_files = partition_data_files(partition_root)
    if not data_files:
        return None
    latest_file = data_files[-1]
    latest_open_time: datetime | None = None
    parquet_file = pq.ParquetFile(latest_file)
    for batch in parquet_file.iter_batches(columns=["open_time"], batch_size=10_000):
        for row in batch.to_pylist():
            value = row.get("open_time")
            if isinstance(value, datetime) and (latest_open_time is None or value > latest_open_time):
                latest_open_time = value
    return latest_open_time


def latest_open_time_in_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> datetime | None:
    """Return latest stored OHLCV open_time for one series."""

    return latest_open_time_in_lake_by_dataset(
        lake_root=lake_root,
        dataset_type=ohlcv_dataset_type_for_market(market),
        market=market,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )


def _series_partition_root(
    *,
    lake_root: str,
    dataset_type: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> Path:
    """Return the root path for one dataset/exchange/instrument/symbol/timeframe series."""

    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )


def _open_time_bounds_for_parquet_file(parquet_file: Any) -> tuple[datetime | None, datetime | None]:
    """Read open_time min/max with metadata stats first, then fall back to the column."""

    min_value: datetime | None = None
    max_value: datetime | None = None
    open_time_index = parquet_file.schema_arrow.get_field_index("open_time")
    if open_time_index >= 0:
        metadata = parquet_file.metadata
        for row_group_index in range(metadata.num_row_groups):
            stats = metadata.row_group(row_group_index).column(open_time_index).statistics
            if stats is None or stats.min is None or stats.max is None:
                min_value = None
                max_value = None
                break
            if isinstance(stats.min, datetime) and isinstance(stats.max, datetime):
                min_value = stats.min if min_value is None else min(min_value, stats.min)
                max_value = stats.max if max_value is None else max(max_value, stats.max)
        if min_value is not None and max_value is not None:
            return min_value, max_value

    for batch in parquet_file.iter_batches(columns=["open_time"], batch_size=10_000):
        for row in batch.to_pylist():
            value = row.get("open_time")
            if not isinstance(value, datetime):
                continue
            min_value = value if min_value is None else min(min_value, value)
            max_value = value if max_value is None else max(max_value, value)
    return min_value, max_value


def _require_pyarrow_parquet() -> Any:
    """Load PyArrow parquet module required for lake metadata queries."""

    try:
        return import_module("pyarrow.parquet")
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc


def _require_polars() -> Any:
    """Load Polars required for efficient lake timestamp aggregation."""

    try:
        return import_module("polars")
    except ImportError as exc:
        raise RuntimeError("polars is required for parquet lake timestamp aggregation.") from exc
