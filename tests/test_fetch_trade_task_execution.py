"""Tests for bounded Bronze trade task execution."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime

from application.dto import TradeFetchTaskDTO
from application.services.fetch_trade_task_execution import fetch_trade_tasks_bounded
from ingestion.http_client import HttpClientError
from ingestion.trades import TradeTick


def test_fetch_trade_tasks_bounded_continues_after_network_unreachable() -> None:
    """One network failure should not prevent later trade tasks from running."""

    tasks = [
        TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="BTC"),
        TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="ETH"),
    ]

    def _fetcher(**kwargs: object) -> list[TradeTick]:
        if kwargs["symbol"] == "BTC":
            raise HttpClientError("No route to host")
        return [_tick(str(kwargs["symbol"]))]

    result = fetch_trade_tasks_bounded(
        tasks=tasks,
        lake_root="/tmp/lake",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=_fetcher,
        timeout_s=None,
        heartbeat_s=30.0,
        runner=_inline_runner,
    )

    assert result.rows[("deribit", "perp", "ETH")] == [_tick("ETH")]
    assert "[NET_UNREACHABLE]" in result.errors[("deribit", "perp", "BTC")]


def test_fetch_trade_tasks_bounded_uses_concurrency() -> None:
    tasks = [
        TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="BTC"),
        TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="ETH"),
    ]

    def _fetcher(**kwargs: object) -> list[TradeTick]:
        time.sleep(0.2)
        return [_tick(str(kwargs["symbol"]))]

    started_at = time.monotonic()
    result = fetch_trade_tasks_bounded(
        tasks=tasks,
        lake_root="/tmp/lake",
        concurrency=2,
        logger=logging.getLogger("test"),
        symbol_fetcher=_fetcher,
        timeout_s=None,
        heartbeat_s=30.0,
        runner=_inline_runner,
    )

    assert time.monotonic() - started_at < 0.35
    assert len(result.rows) == 2
    assert result.errors == {}


def _inline_runner(fn: Callable[..., list[TradeTick]], **kwargs: object) -> list[TradeTick]:
    del kwargs["timeout_s"], kwargs["heartbeat_s"], kwargs["heartbeat"]
    return fn(**kwargs)


def _tick(symbol: str) -> TradeTick:
    return TradeTick(
        exchange="deribit",
        symbol=symbol,
        instrument_type="perp",
        trade_id=symbol,
        trade_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        price=100.0,
        quantity=1.0,
        side="buy",
        is_maker=False,
        source_endpoint="public_trades",
    )
