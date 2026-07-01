"""Tests for Bronze parquet lake save APIs."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.lake_bronze_writes import save_spot_candles_parquet_lake
from ingestion.spot import SpotCandle


def test_save_spot_candles_parquet_lake_writes_partition_from_bronze_writer(tmp_path: Path) -> None:
    """Bronze writer module should own public save behavior, not only the lake facade."""

    candle = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )

    files = save_spot_candles_parquet_lake(
        {"deribit": {"BTCUSDT": [candle]}},
        market="spot",
        lake_root=str(tmp_path),
    )

    assert len(files) == 1
    path = Path(files[0])
    rows = pq.ParquetFile(path).read().to_pylist()
    assert rows[0]["dataset_type"] == "spot"
    assert rows[0]["symbol"] == "BTCUSDT"
    assert "/dataset_type=spot/" in files[0]
    assert "/date=2026-04-27/" in files[0]
