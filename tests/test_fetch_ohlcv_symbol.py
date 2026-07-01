"""Tests for OHLCV symbol-level fetch planning."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from application.services.fetch_ohlcv_symbol import fetch_symbol_candles
from ingestion.spot import SpotCandle


def test_fetch_symbol_candles_tail_delta_only_uses_latest_open_time() -> None:
    """Tail-delta OHLCV fetches should resume from the persisted latest open time."""

    interval_ms = 60_000
    latest_open_time = datetime(2026, 4, 27, 10, 3, tzinfo=UTC)
    end_open_ms = int(datetime(2026, 4, 27, 10, 5, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[SpotCandle]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return []

    rows = fetch_symbol_candles(
        exchange="deribit",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
        symbol_normalizer=lambda **_kwargs: "BTCUSDT",
        interval_ms_resolver=lambda **_kwargs: interval_ms,
        now_open_resolver=lambda **_kwargs: end_open_ms,
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be used for tail deltas"),
        range_fetcher=_range_fetcher,
        latest_open_time_reader=lambda **_kwargs: latest_open_time,
        tail_delta_only=True,
    )

    assert rows == []
    expected_start_ms = int(datetime(2026, 4, 27, 10, 4, tzinfo=UTC).timestamp() * 1000)
    assert calls == [(expected_start_ms, end_open_ms)]


def test_fetch_symbol_candles_bootstrap_with_start_bound_uses_day_range_fetch() -> None:
    """Bootstrap with an explicit start bound should avoid unbounded history fetches."""

    start_bound_ms = int(datetime(2022, 4, 29, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_open_ms = int(datetime(2022, 4, 29, 0, 1, tzinfo=UTC).timestamp() * 1000)
    candle = _candle(open_time=datetime(2022, 4, 29, 0, 0, tzinfo=UTC))
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[SpotCandle]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [candle]

    rows = fetch_symbol_candles(
        exchange="deribit",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
        open_times_reader=lambda **_kwargs: [],
        symbol_normalizer=lambda **_kwargs: "BTCUSDT",
        interval_ms_resolver=lambda **_kwargs: 60_000,
        now_open_resolver=lambda **_kwargs: end_open_ms,
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called with start bound"),
        range_fetcher=_range_fetcher,
        start_open_ms_bound=start_bound_ms,
    )

    assert calls == [(start_bound_ms, end_open_ms)]
    assert rows == [candle]


def test_fetch_symbol_candles_full_gap_fill_includes_head_gap_from_start_bound() -> None:
    """Full-gap OHLCV fetches should backfill the head gap created by explicit bounds."""

    start_bound_ms = int(datetime(2022, 4, 29, 0, 0, tzinfo=UTC).timestamp() * 1000)
    existing_first = datetime(2022, 4, 29, 0, 3, tzinfo=UTC)
    end_open_ms = int(datetime(2022, 4, 29, 0, 5, tzinfo=UTC).timestamp() * 1000)
    interval_ms = 60_000
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[SpotCandle]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return []

    rows = fetch_symbol_candles(
        exchange="deribit",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
        open_times_reader=lambda **_kwargs: [existing_first],
        symbol_normalizer=lambda **_kwargs: "BTCUSDT",
        interval_ms_resolver=lambda **_kwargs: interval_ms,
        now_open_resolver=lambda **_kwargs: end_open_ms,
        ranges_builder=lambda **_kwargs: [],
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called when open_times exist"),
        range_fetcher=_range_fetcher,
        start_open_ms_bound=start_bound_ms,
    )

    assert rows == []
    assert calls == [(start_bound_ms, int(existing_first.timestamp() * 1000) - interval_ms)]


def _candle(*, open_time: datetime) -> SpotCandle:
    return SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=open_time,
        close_time=open_time,
        open_price=1.0,
        high_price=1.0,
        low_price=1.0,
        close_price=1.0,
        volume=1.0,
        quote_volume=1.0,
        trade_count=1,
    )
