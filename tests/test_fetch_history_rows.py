"""Tests for fetch history row-bound filtering helpers."""

from __future__ import annotations

from datetime import UTC, datetime

from application.services.fetch_history_rows import (
    fetch_bootstrap_history_rows_with_start_bound,
    fetch_bounded_daily_rows_with_start_bound,
    filter_chunk_callback,
    filter_rows_by_start_bound,
    row_open_time_ms,
)
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot_ohlcv import SpotCandle


def _spot_ohlcv(open_time: datetime, close: float) -> SpotCandle:
    return SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=open_time,
        close_time=open_time,
        open_price=close,
        high_price=close,
        low_price=close,
        close_price=close,
        volume=1.0,
        quote_volume=1.0,
        trade_count=1,
    )


def _oi(open_time: datetime, value: float) -> OpenInterestPoint:
    return OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=open_time,
        close_time=open_time,
        open_interest=value,
        open_interest_value=value,
    )


def test_filter_rows_and_chunk_callback_by_start_bound() -> None:
    start_bound_ms = int(datetime(2026, 4, 27, 10, 1, tzinfo=UTC).timestamp() * 1000)
    row_old = _oi(datetime(2026, 4, 27, 10, 0, tzinfo=UTC), 1.0)
    row_new = _oi(datetime(2026, 4, 27, 10, 1, tzinfo=UTC), 2.0)

    assert row_open_time_ms(row_new) == start_bound_ms
    assert filter_rows_by_start_bound([row_old, row_new], start_bound_ms) == [row_new]

    seen: list[int] = []
    cb = filter_chunk_callback(lambda rows: seen.append(len(rows)), start_bound_ms)
    assert cb is not None
    cb([row_old, row_new])
    assert seen == [1]


def test_fetch_bounded_daily_rows_with_start_bound_dedupes_by_open_time() -> None:
    ts0 = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    ts1 = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)

    def _range_fetcher(**kwargs: object) -> list[SpotCandle]:
        start_open_ms = int(kwargs["start_open_ms"])
        if start_open_ms == 1:
            return [_spot_ohlcv(ts0, 1.0), _spot_ohlcv(ts0, 2.0)]
        return [_spot_ohlcv(ts1, 3.0)]

    rows = fetch_bounded_daily_rows_with_start_bound(
        day_windows=[(1, 2), (3, 4)],
        range_fetcher=_range_fetcher,
        fetch_kwargs={},
        on_history_chunk=None,
    )

    assert [row.close_price for row in rows] == [2.0, 3.0]


def test_fetch_bootstrap_history_rows_with_start_bound_filters_chunks_and_rows() -> None:
    ts0 = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    ts1 = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
    start_bound_ms = int(ts1.timestamp() * 1000)
    observed_chunk_lengths: list[int] = []

    def _history_fetcher(**kwargs: object) -> list[SpotCandle]:
        callback = kwargs["on_history_chunk"]
        assert callable(callback)
        callback([_spot_ohlcv(ts0, 1.0), _spot_ohlcv(ts1, 2.0)])
        return [_spot_ohlcv(ts0, 1.0), _spot_ohlcv(ts1, 2.0), _spot_ohlcv(ts1, 3.0)]

    rows = fetch_bootstrap_history_rows_with_start_bound(
        history_fetcher=_history_fetcher,
        fetch_kwargs={},
        on_history_chunk=lambda chunk: observed_chunk_lengths.append(len(chunk)),
        start_open_ms_bound=start_bound_ms,
    )

    assert observed_chunk_lengths == [1]
    assert len(rows) == 1
    assert rows[0].open_time == ts1
    assert rows[0].close_price == 3.0
