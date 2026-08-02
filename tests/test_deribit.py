"""Tests for the Deribit candle adapter contract."""

from __future__ import annotations

import pytest

import ingestion.exchanges.deribit as deribit


def test_deribit_normalizers_cover_aliases_and_invalid_values() -> None:
    """Public candle inputs normalize deterministically and reject unsupported values."""

    assert deribit.list_supported_intervals() == ("1m",)
    assert deribit.max_limit() == 5000
    assert deribit.normalize_timeframe("M1") == "1m"
    assert deribit.normalize_timeframe("mn1") == "1m"
    assert deribit.to_deribit_resolution("1m") == "1"
    assert deribit.to_deribit_resolution("2h") == "120"
    assert deribit.interval_to_milliseconds("2h") == 7_200_000
    assert deribit.normalize_symbol("BTC", "perp") == "BTC-PERPETUAL"
    assert deribit.normalize_symbol("ETHUSD", "spot_ohlcv") == "ETH_USDC"
    with pytest.raises(ValueError):
        deribit.normalize_timeframe(" ")
    with pytest.raises(ValueError):
        deribit.normalize_symbol("DOGE", "perp")
    with pytest.raises(ValueError):
        deribit.normalize_symbol("BTC", "option")
    with pytest.raises(ValueError):
        deribit.to_deribit_resolution("invalid")


def test_fetch_chart_page_validates_payload_and_maps_response(monkeypatch: pytest.MonkeyPatch) -> None:
    """The exchange payload is validated before it becomes common candle rows."""

    monkeypatch.setattr(deribit, "get_json", lambda *_args, **_kwargs: {"result": {"status": "no_data"}})
    assert deribit._fetch_chart_page("BTC-PERPETUAL", "1", 60_000, 0, 60_000) == []

    monkeypatch.setattr(deribit, "get_json", lambda *_args, **_kwargs: {"result": {"status": "bad"}})
    with pytest.raises(ValueError, match="chart status"):
        deribit._fetch_chart_page("BTC-PERPETUAL", "1", 60_000, 0, 60_000)

    monkeypatch.setattr(
        deribit,
        "get_json",
        lambda *_args, **_kwargs: {
            "result": {
                "status": "ok",
                "ticks": [60_000],
                "open": [1.0],
                "high": [2.0],
                "low": [0.5],
                "close": [1.5],
                "volume": [3.0],
            }
        },
    )
    assert deribit._fetch_chart_page("BTC-PERPETUAL", "1", 60_000, 0, 60_000) == [
        [60_000, "1.0", "2.0", "0.5", "1.5", "3.0", 119_999, None, 0, "0", "0", "0"]
    ]


def test_deribit_candle_pagination_deduplicates_and_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backward and forward pagination produce ordered, unique open-time rows."""

    monkeypatch.setattr(deribit, "_utc_now_ms", lambda: 180_000)
    pages = iter([[[60_000], [120_000]], []])
    monkeypatch.setattr(deribit, "_fetch_chart_page", lambda **_kwargs: next(pages))
    assert deribit.fetch_klines("BTC", "perp", "1m", 2) == [[60_000], [120_000]]

    range_pages = iter([[[0], [60_000], [60_000]], []])
    monkeypatch.setattr(deribit, "_fetch_chart_page", lambda **_kwargs: next(range_pages))
    assert deribit.fetch_klines_range("BTC", "perp", "1m", 0, 60_000) == [[0], [60_000]]
    assert deribit.fetch_klines_range("BTC", "perp", "1m", 2, 1) == []
