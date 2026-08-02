"""Tests for Bronze lake read helper exports."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion import lake, lake_reads


def test_lake_read_helpers_remain_available_from_compatibility_module() -> None:
    """Existing ``ingestion.lake`` read imports must continue to point at the adapter."""

    assert lake.load_spot_ohlcv_candles_from_lake is lake_reads.load_spot_ohlcv_candles_from_lake
    assert lake.load_open_interest_from_lake is lake_reads.load_open_interest_from_lake
    assert lake.load_funding_from_lake is lake_reads.load_funding_from_lake


def _write_rows(tmp_path: Path, dataset_type: str, market: str, rows: list[dict[str, object]]) -> None:
    partition = (
        tmp_path
        / f"dataset_type={dataset_type}"
        / "exchange=deribit"
        / f"instrument_type={market}"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-08"
        / "date=2026-08-01"
    )
    partition.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), partition / "data.parquet")


def test_read_helpers_build_sorted_source_records_and_ignore_invalid_times(tmp_path: Path) -> None:
    """Read adapters keep only valid timestamps and let later rows replace duplicate times."""

    first = datetime(2026, 8, 1, 0, 0, tzinfo=UTC)
    second = datetime(2026, 8, 1, 0, 1, tzinfo=UTC)
    _write_rows(
        tmp_path,
        "spot_ohlcv",
        "spot_ohlcv",
        [
            {
                "open_time": second,
                "close_time": second,
                "open_price": 2,
                "high_price": 3,
                "low_price": 1,
                "close_price": 2,
                "volume": 4,
            },
            {
                "open_time": first,
                "close_time": first,
                "open_price": 1,
                "high_price": 2,
                "low_price": 0,
                "close_price": 9,
                "volume": 3,
                "quote_volume": None,
            },
            {"open_time": None, "close_time": first},
        ],
    )
    _write_rows(
        tmp_path,
        "open_interest",
        "perp",
        [
            {"open_time": second, "close_time": second, "open_interest": 20, "open_interest_value": 200},
            {"open_time": first, "close_time": first, "open_interest": 10, "open_interest_value": 100},
            {"open_time": None, "close_time": first},
        ],
    )
    _write_rows(
        tmp_path,
        "funding",
        "perp",
        [
            {"open_time": second, "close_time": second, "funding_rate": 0.002, "index_price": 102, "mark_price": 103},
            {"open_time": first, "close_time": first, "funding_rate": 0.001, "index_price": 100, "mark_price": 101},
            {"open_time": first, "close_time": first, "funding_rate": 0.003, "index_price": 104, "mark_price": 105},
        ],
    )

    candles = lake_reads.load_spot_ohlcv_candles_from_lake(str(tmp_path), "spot_ohlcv", "deribit", "BTC", "1m")
    open_interest = lake_reads.load_open_interest_from_lake(str(tmp_path), "perp", "deribit", "BTC", "1m")
    funding = lake_reads.load_funding_from_lake(str(tmp_path), "perp", "deribit", "BTC", "1m")

    assert [item.open_time for item in candles] == [first, second]
    assert candles[0].close_price == 9.0
    assert candles[0].quote_volume is None
    assert [item.open_interest for item in open_interest] == [10.0, 20.0]
    assert [item.funding_rate for item in funding] == [0.003, 0.002]


def test_read_helpers_return_empty_when_requested_partition_is_absent(tmp_path: Path) -> None:
    """Absent Bronze partitions are a normal empty-input condition for replay runs."""

    assert lake_reads.load_spot_ohlcv_candles_from_lake(str(tmp_path), "spot_ohlcv", "deribit", "BTC", "1m") == []
    assert lake_reads.load_open_interest_from_lake(str(tmp_path), "perp", "deribit", "BTC", "1m") == []
    assert lake_reads.load_funding_from_lake(str(tmp_path), "perp", "deribit", "BTC", "1m") == []
