"""Tests for trade symbol-level fetch planning."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any, cast

import pytest

from application.services.fetch_trade_symbol import fetch_symbol_trades
from ingestion.http_client import HttpClientError
from ingestion.trades import TradeTick


def test_fetch_symbol_trades_bootstrap_with_start_bound_uses_trade_range_fetch() -> None:
    """Bounded bootstrap should fetch trade windows instead of unbounded history."""

    start_bound = datetime(2022, 4, 29, 0, 0, tzinfo=UTC)
    end_open_time = datetime(2022, 4, 29, 0, 1, 30, tzinfo=UTC)
    start_bound_ms = int(start_bound.timestamp() * 1000)
    end_open_ms = int(end_open_time.timestamp() * 1000)
    calls: list[tuple[int, int]] = []
    tick = _trade_tick(trade_id="a", trade_time=datetime(2022, 4, 29, 0, 0, 10, tzinfo=UTC))

    def _range_fetcher(**kwargs: object) -> list[TradeTick]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [tick]

    rows = fetch_symbol_trades(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        lake_root="lake/bronze",
        partition_dates_reader=lambda **_kwargs: [],
        symbol_normalizer=lambda **_kwargs: "BTC-PERPETUAL",
        now_open_resolver=lambda **_kwargs: end_open_ms,
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called with start bound"),
        range_fetcher=_range_fetcher,
        start_open_ms_bound=start_bound_ms,
    )

    assert calls == [(start_bound_ms, end_open_ms)]
    assert rows == [tick]


def test_fetch_symbol_trades_uses_partition_dates_instead_of_open_time_scan() -> None:
    """Trade gap planning should use partition-level coverage, not tick scans."""

    end_open_ms = int(datetime(2022, 4, 29, 0, 3, tzinfo=UTC).timestamp() * 1000)
    partition_calls: list[object] = []

    rows = fetch_symbol_trades(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        lake_root="lake/bronze",
        open_times_reader=lambda **_kwargs: pytest.fail("trade fetch should not scan tick open_time values"),
        partition_dates_reader=lambda **kwargs: partition_calls.append(kwargs) or [date(2022, 4, 29)],
        partition_open_time_bounds_reader=lambda **_kwargs: {},
        open_time_minutes_reader=lambda **_kwargs: [
            datetime(2022, 4, 29, 0, 0, tzinfo=UTC),
            datetime(2022, 4, 29, 0, 1, tzinfo=UTC),
            datetime(2022, 4, 29, 0, 2, tzinfo=UTC),
            datetime(2022, 4, 29, 0, 3, tzinfo=UTC),
        ],
        empty_minutes_reader=lambda **_kwargs: [],
        symbol_normalizer=lambda **_kwargs: "BTC-PERPETUAL",
        now_open_resolver=lambda **_kwargs: end_open_ms,
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called when partitions exist"),
        range_fetcher=lambda **_kwargs: pytest.fail("range_fetcher should not be called for covered trade day"),
        empty_minutes_writer=lambda **_kwargs: [],
    )

    assert rows == []
    assert len(partition_calls) == 1


def test_fetch_symbol_trades_continues_after_recoverable_trade_window_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Recoverable window failures should not discard successful trade windows."""

    start_bound = datetime(2022, 4, 29, 0, 0, tzinfo=UTC)
    end_open_time = datetime(2022, 4, 29, 0, 16, tzinfo=UTC)
    start_bound_ms = int(start_bound.timestamp() * 1000)
    end_open_ms = int(end_open_time.timestamp() * 1000)
    first_window_end_ms = int(datetime(2022, 4, 29, 0, 14, 59, 999000, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []
    tick = _trade_tick(trade_id="recovered", trade_time=datetime(2022, 4, 30, 0, 0, 1, tzinfo=UTC))

    def _range_fetcher(**kwargs: object) -> list[TradeTick]:
        start_open_ms = int(cast(Any, kwargs["start_open_ms"]))
        end_ms = int(cast(Any, kwargs["end_open_ms"]))
        calls.append((start_open_ms, end_ms))
        if end_ms == first_window_end_ms:
            raise HttpClientError("Connection error for x: timed out")
        return [tick]

    with caplog.at_level(logging.WARNING):
        rows = fetch_symbol_trades(
            exchange="deribit",
            market="perp",
            symbol="BTC",
            lake_root="lake/bronze",
            partition_dates_reader=lambda **_kwargs: [],
            symbol_normalizer=lambda **_kwargs: "BTC-PERPETUAL",
            now_open_resolver=lambda **_kwargs: end_open_ms,
            history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called with start bound"),
            range_fetcher=_range_fetcher,
            start_open_ms_bound=start_bound_ms,
        )

    assert len(calls) == 2
    assert rows == [tick]
    assert "Trade-window fetch failed" in caplog.text
    assert "Trade bootstrap completed with failed trade windows" in caplog.text


def test_fetch_symbol_trades_tail_bootstraps_from_history_and_filters_start_bound() -> None:
    """Tail mode should use history only when no stored timestamp exists."""

    bound = int(datetime(2022, 4, 29, 0, 1, tzinfo=UTC).timestamp() * 1000)
    before = _trade_tick(trade_id="before", trade_time=datetime(2022, 4, 29, 0, 0, 10, tzinfo=UTC))
    after = _trade_tick(trade_id="after", trade_time=datetime(2022, 4, 29, 0, 2, 10, tzinfo=UTC))
    chunks: list[list[TradeTick]] = []
    rows = fetch_symbol_trades(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        lake_root="lake/bronze",
        tail_delta_only=True,
        latest_open_time_reader=lambda **_kwargs: None,
        now_open_resolver=lambda **_kwargs: bound + 60_000,
        history_fetcher=lambda **kwargs: kwargs["on_history_chunk"]([before, after]) or [before, after],
        on_history_chunk=chunks.append,
        start_open_ms_bound=bound,
    )
    assert rows == [after]
    assert chunks == [[after]]


def test_fetch_symbol_trades_tail_requires_reader_and_handles_empty_delta() -> None:
    """Tail mode should validate its reader and stop when the lake is current."""

    with pytest.raises(ValueError, match="latest_open_time_reader"):
        fetch_symbol_trades(
            exchange="deribit",
            market="perp",
            symbol="BTC",
            lake_root="lake/bronze",
            tail_delta_only=True,
            now_open_resolver=lambda **_kwargs: 100,
        )
    assert (
        fetch_symbol_trades(
            exchange="deribit",
            market="perp",
            symbol="BTC",
            lake_root="lake/bronze",
            tail_delta_only=True,
            latest_open_time_reader=lambda **_kwargs: datetime.fromtimestamp(1, tz=UTC),
            now_open_resolver=lambda **_kwargs: 1_000,
        )
        == []
    )


def test_fetch_symbol_trades_tail_fetches_windows_and_streams_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-empty tail window should be fetched, streamed, and deduplicated."""

    import application.services.fetch_trade_symbol as module

    tick = _trade_tick(trade_id="tail", trade_time=datetime(2022, 4, 29, 0, 2, tzinfo=UTC))
    monkeypatch.setattr(module, "trade_windows_in_random_order", lambda *_args, **_kwargs: [(100, 200)])
    chunks: list[list[TradeTick]] = []
    day_start_ms = int(datetime(2022, 4, 29, tzinfo=UTC).timestamp() * 1000)
    rows = fetch_symbol_trades(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        lake_root="lake/bronze",
        tail_delta_only=True,
        latest_open_time_reader=lambda **_kwargs: datetime.fromtimestamp(0, tz=UTC),
        now_open_resolver=lambda **_kwargs: day_start_ms + 200,
        range_fetcher=lambda **_kwargs: [tick, tick],
        on_history_chunk=chunks.append,
    )
    assert rows == [tick]
    assert chunks == []


def test_fetch_symbol_trades_gap_marks_empty_trade_minutes(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful empty gap window should be persisted as confirmed empty coverage."""

    import application.services.fetch_trade_symbol as module

    monkeypatch.setattr(module, "missing_trade_minute_ranges", lambda **_kwargs: [(100, 200)])
    monkeypatch.setattr(module, "ranges_in_random_order", lambda ranges: ranges)
    monkeypatch.setattr(module, "trade_windows_in_random_order", lambda *_args, **_kwargs: [(100, 200)])
    writes: list[dict[str, object]] = []
    day_start_ms = int(datetime(2022, 4, 29, tzinfo=UTC).timestamp() * 1000)
    rows = fetch_symbol_trades(
        exchange="deribit",
        market="option",
        symbol="BTC",
        lake_root="lake/bronze",
        partition_dates_reader=lambda **_kwargs: [date(2022, 4, 29)],
        partition_open_time_bounds_reader=lambda **_kwargs: {},
        open_time_minutes_reader=lambda **_kwargs: [],
        empty_minutes_reader=lambda **_kwargs: [],
        now_open_resolver=lambda **_kwargs: day_start_ms + 200,
        range_fetcher=lambda **_kwargs: [],
        empty_minutes_writer=lambda **kwargs: writes.append(kwargs) or [],
    )
    assert rows == []
    assert writes[0]["dataset_type"] == "options_trades"


def _trade_tick(*, trade_id: str, trade_time: datetime) -> TradeTick:
    return TradeTick(
        exchange="deribit",
        symbol="BTC",
        instrument_type="perp",
        trade_id=trade_id,
        trade_time=trade_time,
        price=100.0,
        quantity=1.0,
        side="buy",
        is_maker=False,
        source_endpoint="public_trades",
    )
