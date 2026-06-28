"""Tests for Bronze trade-window fetch helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from application.services.fetch_trade_windows import (
    dedupe_sort_trade_rows,
    fetch_trade_window,
    raise_if_all_trade_windows_failed,
    split_range_into_trade_windows,
)
from ingestion.http_client import HttpClientError
from ingestion.trades import TradeTick


def _trade_tick(*, trade_id: str, trade_time: datetime, price: float = 100.0) -> TradeTick:
    return TradeTick(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        instrument_type="perp",
        trade_id=trade_id,
        trade_time=trade_time,
        price=price,
        quantity=1.0,
        side="buy",
        is_maker=False,
        source_endpoint="public_trades",
    )


def test_split_range_into_trade_windows_uses_runtime_policy_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEPTH_PERP_TRADES_WINDOW_MINUTES", "60")
    start_ms = int(datetime(2026, 4, 27, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 27, 0, 59, 59, 999000, tzinfo=UTC).timestamp() * 1000)

    assert split_range_into_trade_windows(start_ms, end_ms, market="perp") == [(start_ms, end_ms)]


def test_dedupe_sort_trade_rows_preserves_distinct_trade_ids() -> None:
    ts = datetime(2026, 4, 27, 0, 0, tzinfo=UTC)
    duplicate_old = _trade_tick(trade_id="b", trade_time=ts, price=100.0)
    duplicate_new = _trade_tick(trade_id="b", trade_time=ts, price=101.0)
    same_timestamp_distinct_id = _trade_tick(trade_id="a", trade_time=ts, price=102.0)

    rows = dedupe_sort_trade_rows([duplicate_old, duplicate_new, same_timestamp_distinct_id])

    assert [row.trade_id for row in rows] == ["a", "b"]
    assert rows[1].price == 101.0


def test_fetch_trade_window_returns_recoverable_http_timeout() -> None:
    def _range_fetcher(**kwargs: Any) -> list[TradeTick]:
        del kwargs
        raise HttpClientError("connection timed out")

    rows, error = fetch_trade_window(
        range_fetcher=_range_fetcher,
        exchange="deribit",
        market="perp",
        symbol="BTC-PERPETUAL",
        start_open_ms=1,
        end_open_ms=2,
    )

    assert rows == []
    assert error == "[NET_TIMEOUT] connection timed out"


def test_fetch_trade_window_reraises_non_recoverable_error() -> None:
    def _range_fetcher(**kwargs: Any) -> list[TradeTick]:
        del kwargs
        raise ValueError("bad symbol")

    with pytest.raises(ValueError, match="bad symbol"):
        fetch_trade_window(
            range_fetcher=_range_fetcher,
            exchange="deribit",
            market="perp",
            symbol="BTC-PERPETUAL",
            start_open_ms=1,
            end_open_ms=2,
        )


def test_raise_if_all_trade_windows_failed_only_raises_when_every_window_failed() -> None:
    raise_if_all_trade_windows_failed(
        failed_windows=["1-2: [NET_TIMEOUT] timeout"],
        attempted_windows=2,
        exchange="deribit",
        market="perp",
        symbol="BTC-PERPETUAL",
    )

    with pytest.raises(RuntimeError, match="all trade windows failed"):
        raise_if_all_trade_windows_failed(
            failed_windows=["1-2: [NET_TIMEOUT] timeout"],
            attempted_windows=1,
            exchange="deribit",
            market="perp",
            symbol="BTC-PERPETUAL",
        )
