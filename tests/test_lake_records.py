"""Tests for Bronze lake record and partition mapping helpers."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.lake_datasets import bronze_trade_dataset_type_for_market
from ingestion.lake_records import candle_partition_key, candle_record, trade_partition_key, trade_record
from ingestion.spot_ohlcv import SpotCandle
from ingestion.trades import OptionTradeTick, TradeTick


def _sample_candle() -> SpotCandle:
    return SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=110.0,
        low_price=90.0,
        close_price=105.0,
        volume=15.0,
        quote_volume=1500.0,
        trade_count=42,
    )


def test_candle_partition_and_record_mapping() -> None:
    """OHLCV rows should keep the existing Bronze partition and source-shaped fields."""

    candle = _sample_candle()
    ingested_at = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)

    assert candle_partition_key(candle, "spot_ohlcv") == ("deribit", "spot_ohlcv", "BTCUSDT", "1m", "2026-04-27")

    row = candle_record(candle, "spot_ohlcv", run_id="run-1", ingested_at=ingested_at)

    assert row["dataset_type"] == "spot_ohlcv"
    assert row["instrument_type"] == "spot_ohlcv"
    assert row["event_time"] == candle.open_time
    assert row["ingested_at"] == ingested_at
    assert row["origin_payload"] == {
        "exchange": "deribit",
        "symbol": "BTCUSDT",
        "interval": "1m",
        "open_time": candle.open_time,
        "close_time": candle.close_time,
        "open_price": 100.0,
        "high_price": 110.0,
        "low_price": 90.0,
        "close_price": 105.0,
        "volume": 15.0,
        "quote_volume": 1500.0,
        "trade_count": 42,
    }


def test_bronze_trade_dataset_type_uses_plural_name_for_options() -> None:
    """Bronze option trade datasets should use the plural bronze name."""

    assert bronze_trade_dataset_type_for_market("option") == "options_trades"
    assert bronze_trade_dataset_type_for_market("perp") == "perps_trades"


def test_trade_record_mapping_includes_option_fields_only_for_options() -> None:
    """Trade rows should preserve perp and option-specific Bronze schemas."""

    trade_time = datetime(2026, 5, 1, 0, 0, 1, tzinfo=UTC)
    ingested_at = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)
    perp = TradeTick(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        instrument_type="perp",
        trade_id="x1",
        trade_time=trade_time,
        price=100.0,
        quantity=1.0,
        side="buy",
        is_maker=True,
        source_endpoint="public_trades",
    )
    option = OptionTradeTick(
        exchange="deribit",
        symbol="BTC",
        instrument_type="option",
        trade_id="o1",
        trade_time=trade_time,
        price=10.0,
        quantity=2.0,
        side="sell",
        is_maker=False,
        source_endpoint="public_trades",
        instrument_name="BTC-1JAN26-100000-C",
        expiry="2026-01-01",
        strike=100000.0,
        option_type="call",
    )

    assert trade_partition_key(perp, "perp") == ("deribit", "perp", "BTC-PERPETUAL", "tick", "2026-05-01")
    perp_row = trade_record(perp, "perp", run_id="run-1", ingested_at=ingested_at)
    option_row = trade_record(option, "option", run_id="run-1", ingested_at=ingested_at)

    assert perp_row["dataset_type"] == "perps_trades"
    assert "instrument_name" not in perp_row
    assert option_row["dataset_type"] == "options_trades"
    assert option_row["instrument_name"] == "BTC-1JAN26-100000-C"
    assert option_row["option_type"] == "call"
