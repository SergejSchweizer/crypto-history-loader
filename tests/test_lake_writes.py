"""Tests for Bronze parquet write helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.lake_layout import partition_path
from ingestion.lake_writes import merge_and_deduplicate_rows, require_pyarrow, write_partition_file


def _spot_ohlcv_row(open_time: datetime, close: float) -> dict[str, object]:
    return {
        "exchange": "deribit",
        "dataset_type": "spot_ohlcv",
        "instrument_type": "spot_ohlcv",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "open_time": open_time,
        "close": close,
    }


def test_merge_and_deduplicate_rows_keeps_latest_record_and_sorts_by_open_time() -> None:
    first_time = datetime(2026, 4, 27, 10, 0, tzinfo=UTC)
    second_time = datetime(2026, 4, 27, 11, 0, tzinfo=UTC)

    merged = merge_and_deduplicate_rows(
        existing=[
            _spot_ohlcv_row(open_time=second_time, close=101.0),
            _spot_ohlcv_row(open_time=first_time, close=100.0),
        ],
        new=[_spot_ohlcv_row(open_time=first_time, close=102.0)],
    )

    assert [row["open_time"] for row in merged] == [first_time, second_time]
    assert merged[0]["close"] == 102.0


def test_write_partition_file_rewrites_existing_partition_without_duplicate_keys(tmp_path: Path) -> None:
    pa, parquet = require_pyarrow()
    lake_root = str(tmp_path / "bronze")
    key = ("deribit", "spot_ohlcv", "BTCUSDT", "1m", "2026-04-27")
    first_time = datetime(2026, 4, 27, 10, 0, tzinfo=UTC)
    second_time = datetime(2026, 4, 27, 10, 1, tzinfo=UTC)

    first_path = write_partition_file(
        pa=pa,
        pq=parquet,
        lake_root=lake_root,
        dataset_type="spot_ohlcv",
        run_id="run-1",
        key=key,
        rows=[_spot_ohlcv_row(open_time=first_time, close=100.0)],
    )
    second_path = write_partition_file(
        pa=pa,
        pq=parquet,
        lake_root=lake_root,
        dataset_type="spot_ohlcv",
        run_id="run-2",
        key=key,
        rows=[_spot_ohlcv_row(open_time=first_time, close=101.0), _spot_ohlcv_row(open_time=second_time, close=102.0)],
    )

    expected_path = partition_path(lake_root=lake_root, dataset_type="spot_ohlcv", key=key) / "data.parquet"
    rows = pq.ParquetFile(expected_path).read().to_pylist()

    assert first_path == second_path == str(expected_path.resolve())
    assert [row["open_time"] for row in rows] == [first_time, second_time]
    assert [row["close"] for row in rows] == [101.0, 102.0]
