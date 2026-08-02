"""Tests for application-facing lake query helpers."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import polars as pl

from application.services import lake_query_service


def test_load_combined_ohlcv_dataframe_hides_open_interest_flag(monkeypatch: Any) -> None:
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


def test_lake_query_service_delegates_all_query_variants(monkeypatch: Any) -> None:
    """The application adapter must forward keyword arguments without changing results."""

    expected_times = [datetime(2026, 1, 1, tzinfo=UTC)]
    expected_latest = expected_times[0]
    calls: list[tuple[str, dict[str, object]]] = []

    def record(name: str, result: object) -> Callable[..., object]:
        def _record(**kwargs: object) -> object:
            calls.append((name, kwargs))
            return result

        return _record

    monkeypatch.setattr(lake_query_service, "_open_times_in_lake", record("times", expected_times))
    monkeypatch.setattr(lake_query_service, "_open_times_in_lake_by_dataset", record("dataset_times", expected_times))
    monkeypatch.setattr(lake_query_service, "_latest_open_time_in_lake", record("latest", expected_latest))
    monkeypatch.setattr(
        lake_query_service,
        "_latest_open_time_in_lake_by_dataset",
        record("dataset_latest", expected_latest),
    )

    kwargs = {"lake_root": "lake", "symbol": "BTC"}
    assert lake_query_service.open_times_in_lake(**kwargs) == expected_times
    assert lake_query_service.open_times_in_lake_by_dataset(**kwargs) == expected_times
    assert lake_query_service.latest_open_time_in_lake(**kwargs) == expected_latest
    assert lake_query_service.latest_open_time_in_lake_by_dataset(**kwargs) == expected_latest
    assert [name for name, _kwargs in calls] == ["times", "dataset_times", "latest", "dataset_latest"]
