"""Tests for volatility symbol-level fetch planning."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from application.services.fetch_volatility_symbol import fetch_symbol_volatility
from ingestion.volatility import VolatilityPoint


def test_fetch_symbol_volatility_tail_uses_dataset_type_and_latest_open_time() -> None:
    latest_open_time = datetime(2026, 4, 27, 10, 0, tzinfo=UTC)
    end_open_ms = int(datetime(2026, 4, 27, 10, 2, tzinfo=UTC).timestamp() * 1000)
    reader_calls: list[str] = []
    range_calls: list[tuple[int, int]] = []

    def _latest_reader(**kwargs: object) -> datetime:
        reader_calls.append(str(kwargs["dataset_type"]))
        return latest_open_time

    def _range_fetcher(**kwargs: object) -> list[VolatilityPoint]:
        range_calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return []

    rows = fetch_symbol_volatility(
        exchange="deribit",
        market="perp",
        symbol="btc",
        timeframe="1m",
        lake_root="lake/bronze",
        dataset_type="volatility_index_data",
        timeframe_normalizer=lambda **_kwargs: "1m",
        interval_ms_resolver=lambda **_kwargs: 60_000,
        now_open_resolver=lambda **_kwargs: end_open_ms,
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called for tail deltas"),
        range_fetcher=_range_fetcher,
        latest_open_time_reader=_latest_reader,
        tail_delta_only=True,
    )

    assert rows == []
    assert reader_calls == ["volatility_index_data"]
    assert range_calls == [(int(datetime(2026, 4, 27, 10, 1, tzinfo=UTC).timestamp() * 1000), end_open_ms)]


def test_fetch_symbol_volatility_full_gap_keeps_range_unsplit() -> None:
    start_ms = int(datetime(2026, 4, 27, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 29, 0, 0, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[VolatilityPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return []

    rows = fetch_symbol_volatility(
        exchange="deribit",
        market="perp",
        symbol="btc",
        timeframe="1m",
        lake_root="lake/bronze",
        dataset_type="volatility_index_data",
        open_times_reader=lambda **_kwargs: [datetime(2026, 4, 26, 23, 59, tzinfo=UTC)],
        timeframe_normalizer=lambda **_kwargs: "1m",
        interval_ms_resolver=lambda **_kwargs: 60_000,
        now_open_resolver=lambda **_kwargs: end_ms,
        ranges_builder=lambda **_kwargs: [(start_ms, end_ms)],
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called when open_times exist"),
        range_fetcher=_range_fetcher,
    )

    assert rows == []
    assert calls == [(start_ms, end_ms)]


def _point(*, open_time: datetime) -> VolatilityPoint:
    return VolatilityPoint(
        exchange="deribit",
        symbol="BTC",
        interval="1m",
        open_time=open_time,
        close_time=open_time,
        value=1.0,
        source_endpoint="public_get_volatility_index_data",
        dataset_type="volatility_index_data",
    )
