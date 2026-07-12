"""Bronze parquet lake read helpers for source-shaped records."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ingestion.funding import FundingPoint
from ingestion.lake_datasets import OPEN_INTEREST_DATASET_TYPE, ohlcv_dataset_type_for_market
from ingestion.lake_layout import partition_data_files
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot_ohlcv import SpotCandle


def load_spot_ohlcv_candles_from_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[SpotCandle]:
    """Load all stored candles for one exchange/symbol/timeframe from parquet lake.

    Uses batched parquet reads to avoid materializing complete files in memory.

    Args:
        lake_root: Root directory of the Bronze parquet lake.
        market: Instrument type and OHLCV dataset family, such as ``spot_ohlcv`` or ``perp``.
        exchange: Exchange partition value.
        symbol: Symbol partition value.
        timeframe: Timeframe partition value.

    Returns:
        Deduplicated candles sorted by ``open_time``.

    Raises:
        RuntimeError: If ``pyarrow`` is unavailable.
    """

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    partition_root = (
        Path(lake_root)
        / f"dataset_type={ohlcv_dataset_type_for_market(market)}"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not partition_root.exists():
        return []

    candles_by_open_time: dict[datetime, SpotCandle] = {}
    for data_file in partition_data_files(partition_root):
        parquet_file = pq.ParquetFile(data_file)
        for batch in parquet_file.iter_batches(batch_size=10_000):
            for row in batch.to_pylist():
                open_time = row.get("open_time")
                close_time = row.get("close_time")
                if not isinstance(open_time, datetime) or not isinstance(close_time, datetime):
                    continue
                quote_volume_raw = row.get("quote_volume")
                candles_by_open_time[open_time] = SpotCandle(
                    exchange=str(row.get("exchange", exchange)),
                    symbol=str(row.get("symbol", symbol)),
                    interval=str(row.get("timeframe", timeframe)),
                    open_time=open_time,
                    close_time=close_time,
                    open_price=float(row.get("open_price", row.get("open", 0.0))),
                    high_price=float(row.get("high_price", row.get("high", 0.0))),
                    low_price=float(row.get("low_price", row.get("low", 0.0))),
                    close_price=float(row.get("close_price", row.get("close", 0.0))),
                    volume=float(row.get("volume", 0.0)),
                    quote_volume=None if quote_volume_raw is None else float(quote_volume_raw),
                    trade_count=int(row.get("trade_count", 0)),
                )
    return [candles_by_open_time[key] for key in sorted(candles_by_open_time)]


def load_open_interest_from_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[OpenInterestPoint]:
    """Load all stored open-interest rows for one exchange/symbol/timeframe from parquet lake.

    Args:
        lake_root: Root directory of the Bronze parquet lake.
        market: Instrument type partition value.
        exchange: Exchange partition value.
        symbol: Symbol partition value.
        timeframe: Timeframe partition value.

    Returns:
        Deduplicated open-interest points sorted by ``open_time``.

    Raises:
        RuntimeError: If ``pyarrow`` is unavailable.
    """

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    partition_root = (
        Path(lake_root)
        / f"dataset_type={OPEN_INTEREST_DATASET_TYPE}"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not partition_root.exists():
        return []

    items_by_open_time: dict[datetime, OpenInterestPoint] = {}
    for data_file in partition_data_files(partition_root):
        parquet_file = pq.ParquetFile(data_file)
        for batch in parquet_file.iter_batches(batch_size=10_000):
            for row in batch.to_pylist():
                open_time = row.get("open_time")
                close_time = row.get("close_time")
                if not isinstance(open_time, datetime) or not isinstance(close_time, datetime):
                    continue
                items_by_open_time[open_time] = OpenInterestPoint(
                    exchange=str(row.get("exchange", exchange)),
                    symbol=str(row.get("symbol", symbol)),
                    interval=str(row.get("timeframe", timeframe)),
                    open_time=open_time,
                    close_time=close_time,
                    open_interest=float(row.get("open_interest", 0.0)),
                    open_interest_value=float(row.get("open_interest_value", 0.0)),
                )
    return [items_by_open_time[key] for key in sorted(items_by_open_time)]


def load_funding_from_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[FundingPoint]:
    """Load all stored funding rows for one exchange/symbol/timeframe from parquet lake.

    Args:
        lake_root: Root directory of the Bronze parquet lake.
        market: Instrument type partition value.
        exchange: Exchange partition value.
        symbol: Symbol partition value.
        timeframe: Timeframe partition value.

    Returns:
        Deduplicated funding points sorted by ``open_time``.

    Raises:
        RuntimeError: If ``pyarrow`` is unavailable.
    """

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    partition_root = (
        Path(lake_root)
        / "dataset_type=funding"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not partition_root.exists():
        return []

    items_by_open_time: dict[datetime, FundingPoint] = {}
    for data_file in partition_data_files(partition_root):
        parquet_file = pq.ParquetFile(data_file)
        for batch in parquet_file.iter_batches(batch_size=10_000):
            for row in batch.to_pylist():
                open_time = row.get("open_time")
                close_time = row.get("close_time")
                if not isinstance(open_time, datetime) or not isinstance(close_time, datetime):
                    continue
                items_by_open_time[open_time] = FundingPoint(
                    exchange=str(row.get("exchange", exchange)),
                    symbol=str(row.get("symbol", symbol)),
                    interval=str(row.get("timeframe", timeframe)),
                    open_time=open_time,
                    close_time=close_time,
                    funding_rate=float(row.get("funding_rate", 0.0)),
                    index_price=float(row.get("index_price", 0.0)),
                    mark_price=float(row.get("mark_price", 0.0)),
                )
    return [items_by_open_time[key] for key in sorted(items_by_open_time)]
