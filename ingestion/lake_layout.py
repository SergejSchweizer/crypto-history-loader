"""Partition layout helpers for the parquet lake adapter."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import cast

DatasetType = str
PartitionKey = tuple[str, str, str, str, str]


def partition_path(lake_root: str, dataset_type: DatasetType, key: PartitionKey) -> Path:
    """Return destination path for one parquet partition."""

    exchange, instrument_type, symbol, timeframe, date_partition = key
    month_partition = date_partition[:7]
    year_partition = month_partition[:4]
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"year={year_partition}"
        / f"month={month_partition}"
        / f"date={date_partition}"
    )


def partition_data_files(partition_root: Path) -> list[Path]:
    """Return bronze partition parquet files, supporting current and previous layouts."""

    files = {
        *partition_root.glob("year=*/month=*/date=*/data.parquet"),
        *partition_root.glob("month=*/date=*/data.parquet"),
    }
    return sorted(files)


def dataset_data_files(lake_root: str, dataset_type: str) -> list[Path]:
    """Return dataset parquet files across current and previous bronze layouts."""

    root = Path(lake_root)
    files = {
        *root.glob(
            f"dataset_type={dataset_type}/exchange=*/instrument_type=*/"
            "symbol=*/timeframe=*/year=*/month=*/date=*/data.parquet"
        ),
        *root.glob(
            f"dataset_type={dataset_type}/exchange=*/instrument_type=*/symbol=*/timeframe=*/month=*/date=*/data.parquet"
        ),
    }
    return sorted(files)


def date_from_partition_path(path: Path) -> date | None:
    """Extract the date partition from a parquet file path."""

    for part in path.parts:
        if not part.startswith("date="):
            continue
        try:
            return date.fromisoformat(part.split("=", 1)[1])
        except ValueError:
            return None
    return None


def partition_key_from_parquet_path(file_path: Path) -> tuple[str, PartitionKey] | None:
    """Parse dataset_type and partition key from a bronze parquet file path."""

    dataset_type: str | None = None
    exchange: str | None = None
    instrument_type: str | None = None
    symbol: str | None = None
    timeframe: str | None = None
    date_partition: str | None = None

    for part in file_path.parts:
        if part.startswith("dataset_type="):
            dataset_type = part.split("=", 1)[1]
        elif part.startswith("exchange="):
            exchange = part.split("=", 1)[1]
        elif part.startswith("instrument_type="):
            instrument_type = part.split("=", 1)[1]
        elif part.startswith("symbol="):
            symbol = part.split("=", 1)[1]
        elif part.startswith("timeframe="):
            timeframe = part.split("=", 1)[1]
        elif part.startswith("date="):
            date_partition = part.split("=", 1)[1]

    if not all([dataset_type, exchange, instrument_type, symbol, timeframe, date_partition]):
        return None
    return (
        cast(str, dataset_type),
        (
            cast(str, exchange),
            cast(str, instrument_type),
            cast(str, symbol),
            cast(str, timeframe),
            cast(str, date_partition),
        ),
    )
