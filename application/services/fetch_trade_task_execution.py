"""Bounded task execution for Bronze trade fetches."""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any, cast

from application.dto import TradeFetchResultDTO, TradeFetchTaskDTO
from application.services.fetch_executors import elapsed_seconds
from application.services.fetch_task_callbacks import bind_task_chunk_callback
from application.services.fetch_trade_windows import classify_trade_fetch_error
from ingestion.spot import Exchange
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick


def fetch_trade_tasks_bounded(
    *,
    tasks: list[TradeFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[TradeTick | OptionTradeTick]],
    timeout_s: float | None,
    heartbeat_s: float,
    runner: Callable[..., Any],
    on_task_complete: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
    on_task_chunk: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
) -> TradeFetchResultDTO:
    """Fetch trade tasks with bounded symbol-level concurrency."""

    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]] = {}
    task_errors: dict[tuple[Exchange, TradeMarket, str], str] = {}
    bounded_concurrency = max(1, min(concurrency, total_tasks or 1))

    def _fetch_one(
        idx: int, task: TradeFetchTaskDTO
    ) -> tuple[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick] | None, str | None]:
        logger.info(
            "Fetch start [%s/%s] type=trades exchange=%s market=%s symbol=%s mode=%s",
            idx,
            total_tasks,
            task.exchange,
            task.market,
            task.symbol,
            "auto-bootstrap-or-tail",
        )
        started_at = datetime.now(UTC)

        def _heartbeat(elapsed_s: int) -> None:
            logger.debug(
                "Fetch heartbeat type=trades exchange=%s market=%s symbol=%s elapsed_s=%s",
                task.exchange,
                task.market,
                task.symbol,
                elapsed_s,
            )

        try:
            rows = cast(
                list[TradeTick | OptionTradeTick],
                runner(
                    symbol_fetcher,
                    timeout_s=timeout_s,
                    heartbeat_s=heartbeat_s,
                    heartbeat=_heartbeat,
                    exchange=task.exchange,
                    market=task.market,
                    symbol=task.symbol,
                    lake_root=lake_root,
                    on_history_chunk=bind_task_chunk_callback(task, on_task_chunk),
                ),
            )
            logger.info(
                "Fetch done [%s/%s] type=trades exchange=%s market=%s symbol=%s rows=%s elapsed_s=%s",
                idx,
                total_tasks,
                task.exchange,
                task.market,
                task.symbol,
                len(rows),
                elapsed_seconds(started_at),
            )
        except Exception as exc:  # noqa: BLE001
            error_class = classify_trade_fetch_error(exc)
            elapsed_s = elapsed_seconds(started_at)
            if error_class == "NET_UNREACHABLE":
                logger.error(
                    "Fetch error type=trades class=%s exchange=%s market=%s symbol=%s elapsed_s=%s",
                    error_class,
                    task.exchange,
                    task.market,
                    task.symbol,
                    elapsed_s,
                )
            else:
                logger.exception(
                    "Fetch error type=trades class=%s exchange=%s market=%s symbol=%s elapsed_s=%s",
                    error_class,
                    task.exchange,
                    task.market,
                    task.symbol,
                    elapsed_s,
                )
            return task, None, f"[{error_class}] {exc}"
        return task, rows, None

    def _store_success(task: TradeFetchTaskDTO, rows: list[TradeTick | OptionTradeTick]) -> None:
        key = (task.exchange, task.market, task.symbol)
        if on_task_complete is not None:
            on_task_complete(task, rows)
        task_results[key] = rows

    if bounded_concurrency == 1:
        for idx, task in enumerate(tasks, start=1):
            task, rows, error = _fetch_one(idx, task)
            key = (task.exchange, task.market, task.symbol)
            if error is not None:
                task_errors[key] = error
            elif rows is not None:
                try:
                    _store_success(task, rows)
                except Exception as exc:  # noqa: BLE001
                    task_errors[key] = f"[{classify_trade_fetch_error(exc)}] {exc}"
        return TradeFetchResultDTO(rows=task_results, errors=task_errors)

    with ThreadPoolExecutor(max_workers=bounded_concurrency) as executor:
        futures = {executor.submit(_fetch_one, idx, task): task for idx, task in enumerate(tasks, start=1)}
        for future in as_completed(futures):
            task, rows, error = future.result()
            key = (task.exchange, task.market, task.symbol)
            if error is not None:
                task_errors[key] = error
                continue
            if rows is not None:
                try:
                    _store_success(task, rows)
                except Exception as exc:  # noqa: BLE001
                    task_errors[key] = f"[{classify_trade_fetch_error(exc)}] {exc}"
    return TradeFetchResultDTO(rows=task_results, errors=task_errors)
