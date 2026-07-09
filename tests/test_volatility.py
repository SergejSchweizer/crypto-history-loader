"""Tests for volatility ingestion contracts."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.volatility import (
    fetch_volatility_index_range,
    normalize_volatility_timeframe,
)


def test_normalize_volatility_timeframe_accepts_aliases() -> None:
    assert normalize_volatility_timeframe("deribit", "M1") == "1m"
    assert normalize_volatility_timeframe("deribit", "1h") == "1h"


def test_fetch_volatility_index_range_parses_points(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def _fake_fetch_range(**kwargs: object):
        assert kwargs["currency"] == "BTC"
        return [{"timestamp": 1_000, "open": 12.0, "high": 13.0, "low": 11.0, "close": 12.5, "index_value": 12.5}]

    monkeypatch.setattr("ingestion.volatility.deribit_volatility.fetch_volatility_index_data_range", _fake_fetch_range)
    rows = fetch_volatility_index_range(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="1m",
        start_open_ms=1,
        end_open_ms=10,
        market="perp",
    )
    assert len(rows) == 1
    assert rows[0].dataset_type == "volatility_index"
    assert rows[0].value == 12.5
    assert rows[0].open_value == 12.0
    assert rows[0].high_value == 13.0
    assert rows[0].low_value == 11.0
    assert rows[0].close_value == 12.5


def test_fetch_volatility_index_range_parses_symbol_variants(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def _fake_fetch_range(**kwargs: object):
        assert kwargs["currency"] == "ETH"
        return [{"timestamp": 2_000, "open": 75.0, "high": 76.0, "low": 74.5, "close": 75.2, "index_value": 75.2}]

    monkeypatch.setattr("ingestion.volatility.deribit_volatility.fetch_volatility_index_data_range", _fake_fetch_range)
    rows = fetch_volatility_index_range(
        exchange="deribit",
        symbol="ETHUSDT",
        interval="1m",
        start_open_ms=1,
        end_open_ms=10,
        market="perp",
    )
    assert len(rows) == 1
    assert rows[0].dataset_type == "volatility_index"
    assert rows[0].open_time == datetime.fromtimestamp(2, tz=UTC)
