"""Tests for volatility ingestion contracts."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ingestion.http_client import HttpClientError
from ingestion.volatility import (
    _canonical_currency,
    deribit_volatility_resolution,
    fetch_volatility_index_all_history,
    fetch_volatility_index_range,
    normalize_volatility_timeframe,
    volatility_interval_to_milliseconds,
)


def test_normalize_volatility_timeframe_accepts_aliases() -> None:
    assert normalize_volatility_timeframe("deribit", "M1") == "1m"
    assert normalize_volatility_timeframe("deribit", "1h") == "1h"
    assert deribit_volatility_resolution("1m") == "60"


def test_fetch_volatility_index_range_parses_points(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def _fake_fetch_range(**kwargs: object):
        assert kwargs["currency"] == "BTC"
        assert kwargs["resolution"] == "60"
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


def test_volatility_timeframe_validation_and_unit_conversions() -> None:
    """Unsupported exchanges and intervals must fail explicitly."""

    for function, args in [
        (normalize_volatility_timeframe, ("binance", "1m")),
        (normalize_volatility_timeframe, ("deribit", "2m")),
        (volatility_interval_to_milliseconds, ("binance", "1m")),
        (volatility_interval_to_milliseconds, ("deribit", "bad")),
        (deribit_volatility_resolution, ("bad",)),
    ]:
        with pytest.raises(ValueError):
            function(*args)
    assert volatility_interval_to_milliseconds("deribit", "5m") == 300_000
    assert volatility_interval_to_milliseconds("deribit", "2h") == 7_200_000
    assert volatility_interval_to_milliseconds("deribit", "1d") == 86_400_000
    assert deribit_volatility_resolution("4h") == "14400"
    assert deribit_volatility_resolution("1d") == "1D"


def test_volatility_fetch_all_history_handles_market_errors_and_chunk_callback(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """History fetches should handle unsupported markets, API errors, and streaming callbacks."""

    assert fetch_volatility_index_all_history("deribit", "BTC", "1m", "spot") == []
    monkeypatch.setattr(
        "ingestion.volatility.deribit_volatility.fetch_volatility_index_data_all",
        lambda **_kwargs: (_ for _ in ()).throw(HttpClientError("offline")),
    )
    assert fetch_volatility_index_all_history("deribit", "BTC", "1m", "perp") == []

    chunks: list[object] = []
    monkeypatch.setattr(
        "ingestion.volatility.deribit_volatility.fetch_volatility_index_data_all",
        lambda **_kwargs: [{"timestamp": 1_000, "index_value": 42.0}],
    )
    assert (
        fetch_volatility_index_all_history("deribit", "BTC-PERPETUAL", "m1", "perp", on_history_chunk=chunks.extend)
        == []
    )
    assert len(chunks) == 1


def test_volatility_range_returns_empty_for_unsupported_market_and_http_error(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Range reads should not call the exchange for unsupported or unavailable data."""

    assert fetch_volatility_index_range("deribit", "BTC", "1m", 0, 1, "spot") == []
    monkeypatch.setattr(
        "ingestion.volatility.deribit_volatility.fetch_volatility_index_data_range",
        lambda **_kwargs: (_ for _ in ()).throw(HttpClientError("offline")),
    )
    assert fetch_volatility_index_range("deribit", "BTC", "1m", 0, 1, "perp") == []


@pytest.mark.parametrize(
    ("symbol", "currency"),
    [
        ("BTC-PERPETUAL", "BTC"),
        ("SOL_USDC-PERPETUAL", "SOL_USDC"),
        ("SOL_USDC", "SOL"),
        ("ETHUSDC", "ETH"),
        ("BTCUSDT", "BTC"),
        ("ETHUSD", "ETH"),
        ("sol", "SOL"),
    ],
)
def test_canonical_currency_normalizes_deribit_symbol_forms(symbol: str, currency: str) -> None:
    """Volatility endpoints use the currency root for every supported instrument spelling."""

    assert _canonical_currency(symbol) == currency


def test_canonical_currency_rejects_empty_symbol() -> None:
    """The exchange request cannot be constructed from an empty currency."""

    with pytest.raises(ValueError, match="cannot be empty"):
        _canonical_currency("  ")
