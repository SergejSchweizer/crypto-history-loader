"""Execution orchestration helpers for bronze loader."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TypeVar, cast

T = TypeVar("T")


def fetch_all_task_groups(
    *,
    candle_tasks: list[tuple[str, str, str, str]],
    oi_tasks: list[tuple[str, str, str]],
    funding_tasks: list[tuple[str, str, str]],
    trade_tasks: list[tuple[str, str, str]] | None,
    lake_root: str,
    candle_concurrency: int,
    oi_concurrency: int,
    funding_concurrency: int,
    trade_concurrency: int,
    logger: logging.Logger,
    fetch_candles_fn: Callable[..., object],
    fetch_oi_fn: Callable[..., object],
    fetch_funding_fn: Callable[..., object],
    fetch_trades_fn: Callable[..., object],
    on_candle_task_complete: Callable[[object, list[T]], None] | None = None,
    on_oi_task_complete: Callable[[object, list[T]], None] | None = None,
    on_funding_task_complete: Callable[[object, list[T]], None] | None = None,
    on_trade_task_complete: Callable[[object, list[T]], None] | None = None,
    on_candle_task_chunk: Callable[[object, list[T]], None] | None = None,
    on_oi_task_chunk: Callable[[object, list[T]], None] | None = None,
    on_funding_task_chunk: Callable[[object, list[T]], None] | None = None,
    on_trade_task_chunk: Callable[[object, list[T]], None] | None = None,
) -> tuple[
    dict[tuple[str, str, str, str], list[T]],
    dict[tuple[str, str, str, str], str],
    dict[tuple[str, str, str], list[T]],
    dict[tuple[str, str, str], str],
    dict[tuple[str, str, str], list[T]],
    dict[tuple[str, str, str], str],
    dict[tuple[str, str, str], list[T]],
    dict[tuple[str, str, str], str],
]:
    """Fetch all configured task groups sequentially."""

    task_results: dict[tuple[str, str, str, str], list[T]] = {}
    task_errors: dict[tuple[str, str, str, str], str] = {}
    oi_results: dict[tuple[str, str, str], list[T]] = {}
    oi_errors: dict[tuple[str, str, str], str] = {}
    funding_results: dict[tuple[str, str, str], list[T]] = {}
    funding_errors: dict[tuple[str, str, str], str] = {}
    trade_results: dict[tuple[str, str, str], list[T]] = {}
    trade_errors: dict[tuple[str, str, str], str] = {}

    if candle_tasks:
        candle_rows, candle_errors = cast(
            tuple[dict[tuple[str, str, str, str], list[T]], dict[tuple[str, str, str, str], str]],
            fetch_candles_fn(
                tasks=candle_tasks,
                lake_root=lake_root,
                concurrency=candle_concurrency,
                logger=logger,
                on_task_complete=on_candle_task_complete,
                on_task_chunk=on_candle_task_chunk,
            ),
        )
        task_results.update(candle_rows)
        task_errors.update(candle_errors)

    if oi_tasks:
        oi_rows, oi_task_errors = cast(
            tuple[dict[tuple[str, str, str], list[T]], dict[tuple[str, str, str], str]],
            fetch_oi_fn(
                oi_tasks=oi_tasks,
                lake_root=lake_root,
                concurrency=oi_concurrency,
                logger=logger,
                on_task_complete=on_oi_task_complete,
                on_task_chunk=on_oi_task_chunk,
            ),
        )
        oi_results.update(oi_rows)
        oi_errors.update(oi_task_errors)

    if funding_tasks:
        funding_rows, funding_task_errors = cast(
            tuple[dict[tuple[str, str, str], list[T]], dict[tuple[str, str, str], str]],
            fetch_funding_fn(
                funding_tasks=funding_tasks,
                lake_root=lake_root,
                concurrency=funding_concurrency,
                logger=logger,
                on_task_complete=on_funding_task_complete,
                on_task_chunk=on_funding_task_chunk,
            ),
        )
        funding_results.update(funding_rows)
        funding_errors.update(funding_task_errors)

    if trade_tasks:
        trade_rows, trade_task_errors = cast(
            tuple[dict[tuple[str, str, str], list[T]], dict[tuple[str, str, str], str]],
            fetch_trades_fn(
                trade_tasks=trade_tasks,
                lake_root=lake_root,
                concurrency=trade_concurrency,
                logger=logger,
                on_task_complete=on_trade_task_complete,
                on_task_chunk=on_trade_task_chunk,
            ),
        )
        trade_results.update(trade_rows)
        trade_errors.update(trade_task_errors)

    return (
        task_results,
        task_errors,
        oi_results,
        oi_errors,
        funding_results,
        funding_errors,
        trade_results,
        trade_errors,
    )
