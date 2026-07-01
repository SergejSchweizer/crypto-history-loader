"""Tests for open-interest symbol-level fetch planning."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from application.services.fetch_open_interest_symbol import fetch_symbol_open_interest
from ingestion.open_interest import OpenInterestPoint


def test_fetch_symbol_open_interest_tail_latest_none_uses_start_bound_range_fetch() -> None:
    start_bound_ms = int(datetime(2026, 4, 27, 10, 0, tzinfo=UTC).timestamp() * 1000)
    end_open_ms = int(datetime(2026, 4, 27, 10, 2, tzinfo=UTC).timestamp() * 1000)
    point = _point(open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC))
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[OpenInterestPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [point]

    rows = fetch_symbol_open_interest(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="1m",
        lake_root="lake/bronze",
        timeframe_normalizer=lambda **_kwargs: "1m",
        symbol_normalizer=lambda **_kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **_kwargs: 60_000,
        now_open_resolver=lambda **_kwargs: end_open_ms,
        latest_open_time_reader=lambda **_kwargs: None,
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called when start bound is set"),
        range_fetcher=_range_fetcher,
        tail_delta_only=True,
        start_open_ms_bound=start_bound_ms,
    )

    assert calls == [(start_bound_ms, end_open_ms)]
    assert rows == [point]


def test_fetch_symbol_open_interest_full_gap_uses_day_windows() -> None:
    start_ms = int(datetime(2026, 4, 27, 23, 59, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 28, 0, 1, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[OpenInterestPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return []

    rows = fetch_symbol_open_interest(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="1m",
        lake_root="lake/bronze",
        open_times_reader=lambda **_kwargs: [datetime(2026, 4, 27, 23, 58, tzinfo=UTC)],
        timeframe_normalizer=lambda **_kwargs: "1m",
        symbol_normalizer=lambda **_kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **_kwargs: 60_000,
        now_open_resolver=lambda **_kwargs: end_ms,
        ranges_builder=lambda **_kwargs: [(start_ms, end_ms)],
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called when open_times exist"),
        range_fetcher=_range_fetcher,
    )

    assert rows == []
    assert calls == [
        (start_ms, int(datetime(2026, 4, 27, 23, 59, 59, 999000, tzinfo=UTC).timestamp() * 1000)),
        (int(datetime(2026, 4, 28, 0, 0, tzinfo=UTC).timestamp() * 1000), end_ms),
    ]


def _point(*, open_time: datetime) -> OpenInterestPoint:
    return OpenInterestPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="1m",
        open_time=open_time,
        close_time=open_time,
        open_interest=10.0,
        open_interest_value=20.0,
    )
