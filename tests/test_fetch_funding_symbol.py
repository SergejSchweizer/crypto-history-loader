"""Tests for funding symbol-level fetch planning."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from application.services.fetch_funding_symbol import fetch_symbol_funding
from ingestion.funding import FundingPoint


def test_fetch_symbol_funding_tail_latest_none_uses_start_bound_range_fetch() -> None:
    start_bound_ms = int(datetime(2026, 4, 27, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_open_ms = int(datetime(2026, 4, 27, 8, 0, tzinfo=UTC).timestamp() * 1000)
    point = _point(open_time=datetime(2026, 4, 27, 0, 0, tzinfo=UTC))
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[FundingPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [point]

    rows = fetch_symbol_funding(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="8h",
        lake_root="lake/bronze",
        timeframe_normalizer=lambda **_kwargs: "8h",
        symbol_normalizer=lambda **_kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **_kwargs: 8 * 60 * 60 * 1000,
        now_open_resolver=lambda **_kwargs: end_open_ms,
        latest_open_time_reader=lambda **_kwargs: None,
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called when start bound is set"),
        range_fetcher=_range_fetcher,
        tail_delta_only=True,
        start_open_ms_bound=start_bound_ms,
    )

    assert calls == [(start_bound_ms, end_open_ms)]
    assert rows == [point]


def test_fetch_symbol_funding_full_gap_keeps_sparse_range_unsplit() -> None:
    start_ms = int(datetime(2026, 4, 27, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 29, 0, 0, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[FundingPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return []

    rows = fetch_symbol_funding(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="8h",
        lake_root="lake/bronze",
        open_times_reader=lambda **_kwargs: [datetime(2026, 4, 26, 16, 0, tzinfo=UTC)],
        timeframe_normalizer=lambda **_kwargs: "8h",
        symbol_normalizer=lambda **_kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **_kwargs: 8 * 60 * 60 * 1000,
        now_open_resolver=lambda **_kwargs: end_ms,
        ranges_builder=lambda **_kwargs: [(start_ms, end_ms)],
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called when open_times exist"),
        range_fetcher=_range_fetcher,
    )

    assert rows == []
    assert calls == [(start_ms, end_ms)]


def _point(*, open_time: datetime) -> FundingPoint:
    return FundingPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="8h",
        open_time=open_time,
        close_time=open_time,
        funding_rate=0.001,
        index_price=1.0,
        mark_price=1.0,
    )
