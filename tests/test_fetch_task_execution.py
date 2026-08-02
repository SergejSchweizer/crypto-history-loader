"""Tests for standard Bronze fetch task execution helpers."""

from __future__ import annotations

import logging
from typing import Any

from application.dto import CandleFetchTaskDTO, FundingFetchTaskDTO, OpenInterestFetchTaskDTO, VolatilityFetchTaskDTO
from application.services.fetch_task_execution import (
    fetch_candle_tasks_sequential,
    fetch_funding_tasks_sequential,
    fetch_open_interest_tasks_sequential,
    fetch_volatility_tasks_sequential,
)
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


def test_non_ohlcv_task_executors_store_successes_and_isolate_errors() -> None:
    """All non-OHLCV sequential executors share the completion/error contract."""

    logger = logging.getLogger("test")
    open_interest_task = OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="1m")
    funding_task = FundingFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="8h")
    volatility_task = VolatilityFetchTaskDTO(
        exchange="deribit", symbol="BTC", timeframe="1m", dataset_type="volatility_index_data"
    )
    completed: list[object] = []

    open_interest = fetch_open_interest_tasks_sequential(
        tasks=[open_interest_task],
        lake_root="/tmp/lake",
        logger=logger,
        symbol_fetcher=lambda **_kwargs: [],
        timeout_s=None,
        heartbeat_s=1.0,
        runner=_inline_runner,
        on_task_complete=lambda task, rows: completed.append((task, rows)),
    )
    funding = fetch_funding_tasks_sequential(
        tasks=[funding_task],
        lake_root="/tmp/lake",
        logger=logger,
        symbol_fetcher=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("offline")),
        timeout_s=None,
        heartbeat_s=1.0,
        runner=_inline_runner,
    )
    volatility = fetch_volatility_tasks_sequential(
        tasks=[volatility_task],
        lake_root="/tmp/lake",
        logger=logger,
        symbol_fetcher=lambda **_kwargs: [],
        timeout_s=None,
        heartbeat_s=1.0,
        runner=_inline_runner,
    )

    assert open_interest.rows == {("deribit", "BTC", "1m"): []}
    assert completed == [(open_interest_task, [])]
    assert funding.rows == {}
    assert funding.errors == {("deribit", "BTC", "8h"): "offline"}
    assert volatility.rows == {("deribit", "BTC", "1m"): []}
