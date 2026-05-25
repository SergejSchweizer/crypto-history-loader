"""Tests for Deribit volatility adapters."""

from __future__ import annotations

from ingestion.exchanges import deribit_volatility


def test_fetch_volatility_index_data_range_parses_rows(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def _fake_get_json(url: str, params: dict[str, object]):
        assert "get_volatility_index_data" in url
        assert params["currency"] == "BTC"
        return {"result": {"continuation": False, "data": [[1000, 12.3], [2000, 14.0], [2000, 13.5], [3000, 15.0]]}}

    monkeypatch.setattr(deribit_volatility, "get_json", _fake_get_json)
    rows = deribit_volatility.fetch_volatility_index_data_range(
        currency="btc",
        start_open_ms=1000,
        end_open_ms=2500,
        resolution="1m",
    )
    assert rows == [
        {"timestamp": 1000, "index_value": 12.3},
        {"timestamp": 2000, "index_value": 13.5},
    ]


def test_fetch_volatility_index_data_range_continuation(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[int] = []

    def _fake_get_json(url: str, params: dict[str, object]):
        assert "get_volatility_index_data" in url
        calls.append(int(params["start_timestamp"]))
        if len(calls) == 1:
            return {"result": {"continuation": True, "data": [[1000, 80.0], [2000, 81.0]]}}
        return {"result": {"continuation": False, "data": [[3000, 82.0]]}}

    monkeypatch.setattr(deribit_volatility, "get_json", _fake_get_json)
    rows = deribit_volatility.fetch_volatility_index_data_range(
        currency="ETH",
        start_open_ms=1000,
        end_open_ms=3000,
        resolution="1m",
    )
    assert [row["timestamp"] for row in rows] == [1000, 2000, 3000]
    assert calls == [1000, 2001]
