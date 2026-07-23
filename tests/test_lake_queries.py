"""Tests for Bronze parquet lake query helpers."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.lake import save_spot_ohlcv_candles_parquet_lake, save_trades_parquet_lake
from ingestion.lake_queries import (
    empty_trade_minutes_in_lake_by_dataset,
    latest_open_time_in_lake,
    open_time_bounds_in_lake_by_dataset,
    open_times_in_lake,
    partition_dates_in_lake_by_dataset,
)
from ingestion.lake_writes import write_empty_trade_minutes
from ingestion.spot_ohlcv import SpotCandle
from ingestion.trades import TradeTick


def _candle(open_time: datetime) -> SpotCandle:
    return SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=open_time,
        close_time=open_time.replace(second=59, microsecond=999000),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=12,
    )


def _trade(trade_id: str, trade_time: datetime) -> TradeTick:
    return TradeTick(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        instrument_type="perp",
        trade_id=trade_id,
        trade_time=trade_time,
        price=100.0,
        quantity=1.0,
        side="buy",
        is_maker=False,
        source_endpoint="public_trades",
    )


def test_open_times_and_latest_open_time_read_saved_ohlcv_rows(tmp_path: Path) -> None:
    first = _candle(datetime(2026, 4, 27, 10, 0, tzinfo=UTC))
    second = _candle(datetime(2026, 5, 1, 0, 0, tzinfo=UTC))
    save_spot_ohlcv_candles_parquet_lake({"deribit": {"BTCUSDT": [second, first, first]}}, "spot_ohlcv", str(tmp_path))

    values = open_times_in_lake(
        lake_root=str(tmp_path),
        market="spot_ohlcv",
        exchange="deribit",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    latest = latest_open_time_in_lake(
        lake_root=str(tmp_path),
        market="spot_ohlcv",
        exchange="deribit",
        symbol="BTCUSDT",
        timeframe="1m",
    )

    assert values == [first.open_time, second.open_time]
    assert latest == second.open_time


def test_partition_dates_and_bounds_read_trade_partitions(tmp_path: Path) -> None:
    early = _trade("early", datetime(2026, 4, 27, 0, 0, 5, tzinfo=UTC))
    late = _trade("late", datetime(2026, 4, 28, 23, 59, 5, tzinfo=UTC))
    save_trades_parquet_lake({"deribit": {"BTC-PERPETUAL": [late, early]}}, market="perp", lake_root=str(tmp_path))

    dates = partition_dates_in_lake_by_dataset(
        lake_root=str(tmp_path),
        dataset_type="perps_trades",
        market="perp",
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="tick",
    )
    bounds = open_time_bounds_in_lake_by_dataset(
        lake_root=str(tmp_path),
        dataset_type="perps_trades",
        market="perp",
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="tick",
    )

    assert dates == [date(2026, 4, 27), date(2026, 4, 28)]
    assert bounds[date(2026, 4, 27)] == (early.trade_time, early.trade_time)
    assert bounds[date(2026, 4, 28)] == (late.trade_time, late.trade_time)


def test_open_time_bounds_falls_back_when_parquet_stats_are_unavailable(tmp_path: Path) -> None:
    partition = (
        tmp_path
        / "dataset_type=spot_ohlcv"
        / "exchange=deribit"
        / "instrument_type=spot_ohlcv"
        / "symbol=BTCUSDT"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-04"
        / "date=2026-04-27"
    )
    partition.mkdir(parents=True)
    first = datetime(2026, 4, 27, 10, 0, tzinfo=UTC)
    second = datetime(2026, 4, 27, 10, 1, tzinfo=UTC)
    table = pa.Table.from_pylist([{"open_time": first}, {"open_time": second}])
    pq.write_table(table, partition / "data.parquet", write_statistics=False)

    bounds = open_time_bounds_in_lake_by_dataset(
        lake_root=str(tmp_path),
        dataset_type="spot_ohlcv",
        market="spot_ohlcv",
        exchange="deribit",
        symbol="BTCUSDT",
        timeframe="1m",
    )

    assert bounds == {date(2026, 4, 27): (first, second)}


def test_empty_trade_minutes_roundtrip_and_deduplicate(tmp_path: Path) -> None:
    start_ms = int(datetime(2026, 4, 27, 10, 0, 30, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 27, 10, 2, 15, tzinfo=UTC).timestamp() * 1000)

    first_write = write_empty_trade_minutes(
        lake_root=str(tmp_path),
        dataset_type="perps_trades",
        exchange="deribit",
        instrument_type="perp",
        symbol="BTC-PERPETUAL",
        timeframe="tick",
        start_open_ms=start_ms,
        end_open_ms=end_ms,
        checked_at=datetime(2026, 7, 23, 12, 0, tzinfo=UTC),
    )
    second_write = write_empty_trade_minutes(
        lake_root=str(tmp_path),
        dataset_type="perps_trades",
        exchange="deribit",
        instrument_type="perp",
        symbol="BTC-PERPETUAL",
        timeframe="tick",
        start_open_ms=start_ms,
        end_open_ms=end_ms,
        checked_at=datetime(2026, 7, 23, 12, 1, tzinfo=UTC),
    )

    minutes = empty_trade_minutes_in_lake_by_dataset(
        lake_root=str(tmp_path),
        dataset_type="perps_trades",
        market="perp",
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="tick",
    )

    assert len(first_write) == 1
    assert second_write == first_write
    assert minutes == [
        datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        datetime(2026, 4, 27, 10, 1, tzinfo=UTC),
        datetime(2026, 4, 27, 10, 2, tzinfo=UTC),
    ]
