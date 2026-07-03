"""Tests for parquet lake partition layout helpers."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from ingestion.lake_layout import (
    dataset_data_files,
    date_from_partition_path,
    partition_data_files,
    partition_key_from_parquet_path,
    partition_path,
)


def test_partition_path_uses_year_month_date_layout() -> None:
    key = ("deribit", "spot_ohlcv", "BTCUSDT", "1m", "2026-04-27")

    result = partition_path("lake/bronze", "spot_ohlcv", key)

    assert str(result).endswith(
        "dataset_type=spot_ohlcv/exchange=deribit/instrument_type=spot_ohlcv/"
        "symbol=BTCUSDT/timeframe=1m/year=2026/month=2026-04/date=2026-04-27"
    )


def test_partition_data_files_supports_current_and_previous_layouts(tmp_path: Path) -> None:
    root = tmp_path / "dataset_type=spot_ohlcv" / "exchange=deribit"
    current = root / "year=2026" / "month=2026-04" / "date=2026-04-27" / "data.parquet"
    previous = root / "month=2026-04" / "date=2026-04-28" / "data.parquet"
    for path in (current, previous):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")

    assert partition_data_files(root) == [previous, current]


def test_dataset_data_files_supports_current_and_previous_layouts(tmp_path: Path) -> None:
    current = (
        tmp_path
        / "dataset_type=spot_ohlcv"
        / "exchange=deribit"
        / "instrument_type=spot_ohlcv"
        / "symbol=BTCUSDT"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-04"
        / "date=2026-04-27"
        / "data.parquet"
    )
    previous = (
        tmp_path
        / "dataset_type=spot_ohlcv"
        / "exchange=deribit"
        / "instrument_type=spot_ohlcv"
        / "symbol=BTCUSDT"
        / "timeframe=1m"
        / "month=2026-04"
        / "date=2026-04-28"
        / "data.parquet"
    )
    ignored = tmp_path / "dataset_type=funding" / "exchange=deribit" / "data.parquet"
    for path in (current, previous, ignored):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")

    assert dataset_data_files(str(tmp_path), "spot_ohlcv") == [previous, current]


def test_date_and_partition_key_parsing() -> None:
    path = (
        Path("lake/bronze")
        / "dataset_type=perps_trades"
        / "exchange=deribit"
        / "instrument_type=perp"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=tick"
        / "year=2026"
        / "month=2026-04"
        / "date=2026-04-27"
        / "data.parquet"
    )

    assert date_from_partition_path(path) == date(2026, 4, 27)
    assert partition_key_from_parquet_path(path) == (
        "perps_trades",
        ("deribit", "perp", "BTC-PERPETUAL", "tick", "2026-04-27"),
    )


def test_partition_key_parsing_rejects_invalid_paths() -> None:
    assert date_from_partition_path(Path("date=not-a-date") / "data.parquet") is None
    assert partition_key_from_parquet_path(Path("dataset_type=spot_ohlcv") / "data.parquet") is None
