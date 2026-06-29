"""Tests for Bronze lake dataframe readers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.lake import save_open_interest_parquet_lake, save_spot_candles_parquet_lake
from ingestion.lake_dataframe import load_combined_dataframe_from_lake
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle


def test_load_combined_dataframe_limit_must_be_positive(tmp_path: Path) -> None:
    """Reject invalid dataframe export limits before scanning lake files."""

    with pytest.raises(ValueError, match="limit must be positive"):
        load_combined_dataframe_from_lake(lake_root=str(tmp_path), limit=0)


def test_load_combined_dataframe_applies_filters_and_open_interest(tmp_path: Path) -> None:
    """Filter OHLCV partitions and join matching open-interest rows."""

    candle = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )
    filtered_out = SpotCandle(
        exchange="deribit",
        symbol="ETHUSDT",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 1, 59, 999000, tzinfo=UTC),
        open_price=200.0,
        high_price=201.0,
        low_price=199.0,
        close_price=200.5,
        volume=20.0,
        quote_volume=2000.0,
        trade_count=20,
    )
    save_spot_candles_parquet_lake({"deribit": {"BTCUSDT": [candle]}}, "perp", str(tmp_path))
    save_spot_candles_parquet_lake({"deribit": {"ETHUSDT": [filtered_out]}}, "spot", str(tmp_path))

    open_interest = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_interest=123.0,
        open_interest_value=456.0,
    )
    save_open_interest_parquet_lake({"deribit": {"BTCUSDT": [open_interest]}}, "perp", str(tmp_path))

    frame = load_combined_dataframe_from_lake(
        lake_root=str(tmp_path),
        exchanges=["DERIBIT"],
        symbols=["btcusdt"],
        instrument_types=["PERP"],
        timeframes=["1M"],
        start_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        end_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        include_open_interest=True,
    )

    assert frame.height == 1
    assert frame.get_column("symbol").to_list() == ["BTCUSDT"]
    assert float(frame.select("open").item()) == 100.0
    assert float(frame.select("close").item()) == 100.5
    assert float(frame.select("open_interest").item()) == 123.0
