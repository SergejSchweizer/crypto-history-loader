"""Tests for application-facing lake query helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import polars as pl

from application.services import lake_query_service


def test_load_combined_ohlcv_dataframe_hides_open_interest_flag(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Stats exports should query OHLCV data without exposing lake adapter flags to API code."""

    captured: dict[str, Any] = {}
    expected = pl.DataFrame([{"open": 1.0}])

    def _fake_load_combined_dataframe_from_lake(**kwargs: Any) -> pl.DataFrame:
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(
        lake_query_service,
        "load_combined_dataframe_from_lake",
        _fake_load_combined_dataframe_from_lake,
    )

    start_time = datetime(2026, 1, 1, tzinfo=UTC)
    end_time = datetime(2026, 1, 2, tzinfo=UTC)
    frame = lake_query_service.load_combined_ohlcv_dataframe(
        lake_root="lake/bronze",
        exchanges=["deribit"],
        symbols=["BTC"],
        timeframes=["1m"],
        instrument_types=["spot_ohlcv"],
        start_time=start_time,
        end_time=end_time,
    )

    assert frame is expected
    assert captured == {
        "lake_root": "lake/bronze",
        "exchanges": ["deribit"],
        "symbols": ["BTC"],
        "timeframes": ["1m"],
        "instrument_types": ["spot_ohlcv"],
        "start_time": start_time,
        "end_time": end_time,
        "include_open_interest": False,
    }
