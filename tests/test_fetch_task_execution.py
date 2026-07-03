"""Tests for standard Bronze fetch task execution helpers."""

from __future__ import annotations

import logging
from typing import Any

from application.dto import CandleFetchTaskDTO
from application.services.fetch_task_execution import fetch_candle_tasks_sequential
from ingestion.spot_ohlcv import SpotCandle


def test_fetch_candle_tasks_sequential_retries_without_history_chunk_when_unsupported() -> None:
    """Task execution should preserve legacy fetchers without chunk callback support."""

    task = CandleFetchTaskDTO(exchange="deribit", market="spot_ohlcv", symbol="BTC", timeframe="1m")
    calls: list[dict[str, object]] = []

    def _fetcher(**kwargs: object) -> list[SpotCandle]:
        calls.append(kwargs)
        if "on_history_chunk" in kwargs:
            raise TypeError("unexpected keyword argument 'on_history_chunk'")
        return []

    result = fetch_candle_tasks_sequential(
        tasks=[task],
        lake_root="/tmp/lake",
        logger=logging.getLogger("test"),
        symbol_fetcher=_fetcher,
        timeout_s=None,
        heartbeat_s=30.0,
        runner=_inline_runner,
    )

    assert result.errors == {}
    assert result.rows == {("deribit", "spot_ohlcv", "BTC", "1m"): []}
    assert len(calls) == 2
    assert "on_history_chunk" in calls[0]
    assert "on_history_chunk" not in calls[1]


def _inline_runner(
    fn: Any,
    *,
    timeout_s: float | None,
    heartbeat_s: float,
    heartbeat: Any,
    use_process_timeout: bool,
    **kwargs: object,
) -> object:
    del timeout_s, heartbeat_s, heartbeat, use_process_timeout
    return fn(**kwargs)
