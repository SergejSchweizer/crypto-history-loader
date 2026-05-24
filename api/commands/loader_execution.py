"""Execution orchestration helpers for bronze loader."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TypeVar, cast

T = TypeVar("T")


@dataclass(frozen=True)
class _TaskGroupConfig:
    """Runtime dispatch contract for one Bronze task group."""

    name: str
    tasks: Sequence[tuple[str, ...]] | None
    fetch_fn: Callable[..., object]
    task_param_name: str
    concurrency: int
    on_task_complete: object | None
    on_task_chunk: object | None


def _fetch_group(
    *,
    config: _TaskGroupConfig,
    lake_root: str,
    logger: logging.Logger,
) -> tuple[dict[tuple[str, ...], list[T]], dict[tuple[str, ...], str]]:
    """Fetch one configured task group with a shared call shape."""

    group_tasks = list(config.tasks or [])
    if not group_tasks:
        return {}, {}
    kwargs: dict[str, object] = {
        config.task_param_name: group_tasks,
        "lake_root": lake_root,
        "concurrency": config.concurrency,
        "logger": logger,
        "on_task_complete": config.on_task_complete,
        "on_task_chunk": config.on_task_chunk,
    }
    return cast(
        tuple[dict[tuple[str, ...], list[T]], dict[tuple[str, ...], str]],
        config.fetch_fn(**kwargs),
    )


def fetch_all_task_groups(
    *,
    candle_tasks: list[tuple[str, str, str, str]],
    oi_tasks: list[tuple[str, str, str]],
    funding_tasks: list[tuple[str, str, str]],
    historical_volatility_tasks: list[tuple[str, str, str]],
    volatility_index_data_tasks: list[tuple[str, str, str]],
    trade_tasks: list[tuple[str, str, str]] | None,
    lake_root: str,
    candle_concurrency: int,
    oi_concurrency: int,
    funding_concurrency: int,
    volatility_concurrency: int,
    trade_concurrency: int,
    logger: logging.Logger,
    fetch_candles_fn: Callable[..., object],
    fetch_oi_fn: Callable[..., object],
    fetch_funding_fn: Callable[..., object],
    fetch_historical_volatility_fn: Callable[..., object],
    fetch_volatility_index_data_fn: Callable[..., object],
    fetch_trades_fn: Callable[..., object],
    on_candle_task_complete: Callable[[object, list[T]], None] | None = None,
    on_oi_task_complete: Callable[[object, list[T]], None] | None = None,
    on_funding_task_complete: Callable[[object, list[T]], None] | None = None,
    on_historical_volatility_task_complete: Callable[[object, list[T]], None] | None = None,
    on_volatility_index_data_task_complete: Callable[[object, list[T]], None] | None = None,
    on_trade_task_complete: Callable[[object, list[T]], None] | None = None,
    on_candle_task_chunk: Callable[[object, list[T]], None] | None = None,
    on_oi_task_chunk: Callable[[object, list[T]], None] | None = None,
    on_funding_task_chunk: Callable[[object, list[T]], None] | None = None,
    on_historical_volatility_task_chunk: Callable[[object, list[T]], None] | None = None,
    on_volatility_index_data_task_chunk: Callable[[object, list[T]], None] | None = None,
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
    historical_volatility_results: dict[tuple[str, str, str], list[T]] = {}
    historical_volatility_errors: dict[tuple[str, str, str], str] = {}
    volatility_index_data_results: dict[tuple[str, str, str], list[T]] = {}
    volatility_index_data_errors: dict[tuple[str, str, str], str] = {}
    trade_results: dict[tuple[str, str, str], list[T]] = {}
    trade_errors: dict[tuple[str, str, str], str] = {}

    group_configs = (
        _TaskGroupConfig(
            name="candle",
            tasks=candle_tasks,
            fetch_fn=fetch_candles_fn,
            task_param_name="tasks",
            concurrency=candle_concurrency,
            on_task_complete=on_candle_task_complete,
            on_task_chunk=on_candle_task_chunk,
        ),
        _TaskGroupConfig(
            name="oi",
            tasks=oi_tasks,
            fetch_fn=fetch_oi_fn,
            task_param_name="oi_tasks",
            concurrency=oi_concurrency,
            on_task_complete=on_oi_task_complete,
            on_task_chunk=on_oi_task_chunk,
        ),
        _TaskGroupConfig(
            name="funding",
            tasks=funding_tasks,
            fetch_fn=fetch_funding_fn,
            task_param_name="funding_tasks",
            concurrency=funding_concurrency,
            on_task_complete=on_funding_task_complete,
            on_task_chunk=on_funding_task_chunk,
        ),
        _TaskGroupConfig(
            name="historical_volatility",
            tasks=historical_volatility_tasks,
            fetch_fn=fetch_historical_volatility_fn,
            task_param_name="volatility_tasks",
            concurrency=volatility_concurrency,
            on_task_complete=on_historical_volatility_task_complete,
            on_task_chunk=on_historical_volatility_task_chunk,
        ),
        _TaskGroupConfig(
            name="volatility_index_data",
            tasks=volatility_index_data_tasks,
            fetch_fn=fetch_volatility_index_data_fn,
            task_param_name="volatility_tasks",
            concurrency=volatility_concurrency,
            on_task_complete=on_volatility_index_data_task_complete,
            on_task_chunk=on_volatility_index_data_task_chunk,
        ),
        _TaskGroupConfig(
            name="trade",
            tasks=trade_tasks,
            fetch_fn=fetch_trades_fn,
            task_param_name="trade_tasks",
            concurrency=trade_concurrency,
            on_task_complete=on_trade_task_complete,
            on_task_chunk=on_trade_task_chunk,
        ),
    )
    for config in group_configs:
        rows: dict[tuple[str, ...], list[T]]
        errors: dict[tuple[str, ...], str]
        rows, errors = _fetch_group(config=config, lake_root=lake_root, logger=logger)
        if config.name == "candle":
            task_results.update(cast(dict[tuple[str, str, str, str], list[T]], rows))
            task_errors.update(cast(dict[tuple[str, str, str, str], str], errors))
        elif config.name == "oi":
            oi_results.update(cast(dict[tuple[str, str, str], list[T]], rows))
            oi_errors.update(cast(dict[tuple[str, str, str], str], errors))
        elif config.name == "funding":
            funding_results.update(cast(dict[tuple[str, str, str], list[T]], rows))
            funding_errors.update(cast(dict[tuple[str, str, str], str], errors))
        elif config.name == "historical_volatility":
            historical_volatility_results.update(cast(dict[tuple[str, str, str], list[T]], rows))
            historical_volatility_errors.update(cast(dict[tuple[str, str, str], str], errors))
        elif config.name == "volatility_index_data":
            volatility_index_data_results.update(cast(dict[tuple[str, str, str], list[T]], rows))
            volatility_index_data_errors.update(cast(dict[tuple[str, str, str], str], errors))
        else:
            trade_results.update(cast(dict[tuple[str, str, str], list[T]], rows))
            trade_errors.update(cast(dict[tuple[str, str, str], str], errors))

    return (
        task_results,
        task_errors,
        oi_results,
        oi_errors,
        funding_results,
        funding_errors,
        historical_volatility_results,
        historical_volatility_errors,
        volatility_index_data_results,
        volatility_index_data_errors,
        trade_results,
        trade_errors,
    )
