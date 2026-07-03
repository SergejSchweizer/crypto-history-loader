"""Sequential fetch task execution for non-trade Bronze datasets."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TypeVar, cast

from application.dto import (
    CandleFetchResultDTO,
    CandleFetchTaskDTO,
    FundingFetchResultDTO,
    FundingFetchTaskDTO,
    OpenInterestFetchResultDTO,
    OpenInterestFetchTaskDTO,
    VolatilityFetchResultDTO,
    VolatilityFetchTaskDTO,
)
from application.services.fetch_executors import elapsed_seconds, run_with_optional_history_chunk
from application.services.fetch_task_callbacks import bind_task_chunk_callback
from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot_ohlcv import Exchange, Market, SpotCandle
from ingestion.volatility import VolatilityPoint

TRow = TypeVar("TRow")
TTask = TypeVar("TTask")


def fetch_candle_tasks_sequential(
    *,
    tasks: list[CandleFetchTaskDTO],
    lake_root: str,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[SpotCandle]],
    timeout_s: float | None,
    heartbeat_s: float,
    runner: Callable[..., Any],
    on_task_complete: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_task_chunk: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
) -> CandleFetchResultDTO:
    """Fetch OHLCV tasks sequentially and split rows from per-task errors."""

    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]] = {}
    task_errors: dict[tuple[Exchange, Market, str, str], str] = {}
    for idx, task in enumerate(tasks, start=1):
        logger.info(
            "Fetch start [%s/%s] type=ohlcv exchange=%s market=%s symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            task.exchange,
            task.market,
            task.symbol,
            task.timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        key = (task.exchange, task.market, task.symbol, task.timeframe)
        task_started_at = datetime.now(UTC)

        def _heartbeat(elapsed_s: int) -> None:
            del elapsed_s

        try:
            rows = cast(
                list[SpotCandle],
                run_with_optional_history_chunk(
                    runner=runner,
                    fn=symbol_fetcher,
                    timeout_s=timeout_s,
                    heartbeat_s=heartbeat_s,
                    heartbeat=_heartbeat,
                    use_process_timeout=False,
                    kwargs={
                        "exchange": task.exchange,
                        "market": task.market,
                        "symbol": task.symbol,
                        "timeframe": task.timeframe,
                        "lake_root": lake_root,
                        "on_history_chunk": bind_task_chunk_callback(task, on_task_chunk),
                    },
                ),
            )
            _store_standard_success(
                task=task,
                rows=rows,
                key=key,
                result_rows=task_results,
                on_task_complete=on_task_complete,
            )
            _log_standard_done(
                logger=logger,
                idx=idx,
                total_tasks=total_tasks,
                dataset_label="ohlcv",
                task=task,
                row_count=len(rows),
                elapsed_s=elapsed_seconds(task_started_at),
            )
        except Exception as exc:  # noqa: BLE001
            _record_standard_error(
                logger=logger,
                dataset_label="ohlcv",
                task=task,
                elapsed_s=elapsed_seconds(task_started_at),
                key=key,
                task_errors=task_errors,
                exc=exc,
            )
    return CandleFetchResultDTO(rows=task_results, errors=task_errors)


def fetch_open_interest_tasks_sequential(
    *,
    tasks: list[OpenInterestFetchTaskDTO],
    lake_root: str,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[OpenInterestPoint]],
    timeout_s: float | None,
    heartbeat_s: float,
    runner: Callable[..., Any],
    on_task_complete: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_task_chunk: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
) -> OpenInterestFetchResultDTO:
    """Fetch open-interest tasks sequentially and split rows from per-task errors."""

    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]] = {}
    task_errors: dict[tuple[Exchange, str, str], str] = {}
    for idx, task in enumerate(tasks, start=1):
        logger.info(
            "Fetch start [%s/%s] type=oi exchange=%s market=perp symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            task.exchange,
            task.symbol,
            task.timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        key = (task.exchange, task.symbol, task.timeframe)
        task_started_at = datetime.now(UTC)

        def _heartbeat(elapsed_s: int) -> None:
            del elapsed_s

        try:
            rows = cast(
                list[OpenInterestPoint],
                run_with_optional_history_chunk(
                    runner=runner,
                    fn=symbol_fetcher,
                    timeout_s=timeout_s,
                    heartbeat_s=heartbeat_s,
                    heartbeat=_heartbeat,
                    use_process_timeout=True,
                    kwargs={
                        "exchange": task.exchange,
                        "market": "perp",
                        "symbol": task.symbol,
                        "timeframe": task.timeframe,
                        "lake_root": lake_root,
                        "on_history_chunk": bind_task_chunk_callback(task, on_task_chunk),
                    },
                ),
            )
            _store_standard_success(
                task=task,
                rows=rows,
                key=key,
                result_rows=task_results,
                on_task_complete=on_task_complete,
            )
            _log_standard_done(
                logger=logger,
                idx=idx,
                total_tasks=total_tasks,
                dataset_label="oi",
                task=task,
                row_count=len(rows),
                elapsed_s=elapsed_seconds(task_started_at),
            )
        except Exception as exc:  # noqa: BLE001
            _record_standard_error(
                logger=logger,
                dataset_label="oi",
                task=task,
                elapsed_s=elapsed_seconds(task_started_at),
                key=key,
                task_errors=task_errors,
                exc=exc,
            )
    return OpenInterestFetchResultDTO(rows=task_results, errors=task_errors)


def fetch_funding_tasks_sequential(
    *,
    tasks: list[FundingFetchTaskDTO],
    lake_root: str,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[FundingPoint]],
    timeout_s: float | None,
    heartbeat_s: float,
    runner: Callable[..., Any],
    on_task_complete: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_task_chunk: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
) -> FundingFetchResultDTO:
    """Fetch funding tasks sequentially and split rows from per-task errors."""

    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, str, str], list[FundingPoint]] = {}
    task_errors: dict[tuple[Exchange, str, str], str] = {}
    for idx, task in enumerate(tasks, start=1):
        logger.info(
            "Fetch start [%s/%s] type=funding exchange=%s market=perp symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            task.exchange,
            task.symbol,
            task.timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        key = (task.exchange, task.symbol, task.timeframe)
        task_started_at = datetime.now(UTC)

        def _heartbeat(elapsed_s: int) -> None:
            del elapsed_s

        try:
            rows = cast(
                list[FundingPoint],
                run_with_optional_history_chunk(
                    runner=runner,
                    fn=symbol_fetcher,
                    timeout_s=timeout_s,
                    heartbeat_s=heartbeat_s,
                    heartbeat=_heartbeat,
                    use_process_timeout=False,
                    kwargs={
                        "exchange": task.exchange,
                        "market": "perp",
                        "symbol": task.symbol,
                        "timeframe": task.timeframe,
                        "lake_root": lake_root,
                        "on_history_chunk": bind_task_chunk_callback(task, on_task_chunk),
                    },
                ),
            )
            _store_standard_success(
                task=task,
                rows=rows,
                key=key,
                result_rows=task_results,
                on_task_complete=on_task_complete,
            )
            _log_standard_done(
                logger=logger,
                idx=idx,
                total_tasks=total_tasks,
                dataset_label="funding",
                task=task,
                row_count=len(rows),
                elapsed_s=elapsed_seconds(task_started_at),
            )
        except Exception as exc:  # noqa: BLE001
            _record_standard_error(
                logger=logger,
                dataset_label="funding",
                task=task,
                elapsed_s=elapsed_seconds(task_started_at),
                key=key,
                task_errors=task_errors,
                exc=exc,
            )
    return FundingFetchResultDTO(rows=task_results, errors=task_errors)


def fetch_volatility_tasks_sequential(
    *,
    tasks: list[VolatilityFetchTaskDTO],
    lake_root: str,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[VolatilityPoint]],
    timeout_s: float | None,
    heartbeat_s: float,
    runner: Callable[..., Any],
    on_task_complete: Callable[[VolatilityFetchTaskDTO, list[VolatilityPoint]], None] | None = None,
    on_task_chunk: Callable[[VolatilityFetchTaskDTO, list[VolatilityPoint]], None] | None = None,
) -> VolatilityFetchResultDTO:
    """Fetch volatility tasks sequentially and split rows from per-task errors."""

    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, str, str], list[VolatilityPoint]] = {}
    task_errors: dict[tuple[Exchange, str, str], str] = {}
    for idx, task in enumerate(tasks, start=1):
        logger.info(
            "Fetch start [%s/%s] type=%s exchange=%s market=perp symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            task.dataset_type,
            task.exchange,
            task.symbol,
            task.timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        key = (task.exchange, task.symbol, task.timeframe)
        task_started_at = datetime.now(UTC)

        def _heartbeat(elapsed_s: int) -> None:
            del elapsed_s

        try:
            rows = cast(
                list[VolatilityPoint],
                run_with_optional_history_chunk(
                    runner=runner,
                    fn=symbol_fetcher,
                    timeout_s=timeout_s,
                    heartbeat_s=heartbeat_s,
                    heartbeat=_heartbeat,
                    use_process_timeout=False,
                    kwargs={
                        "exchange": task.exchange,
                        "market": "perp",
                        "symbol": task.symbol,
                        "timeframe": task.timeframe,
                        "lake_root": lake_root,
                        "on_history_chunk": bind_task_chunk_callback(task, on_task_chunk),
                    },
                ),
            )
            _store_standard_success(
                task=task,
                rows=rows,
                key=key,
                result_rows=task_results,
                on_task_complete=on_task_complete,
            )
            _log_standard_done(
                logger=logger,
                idx=idx,
                total_tasks=total_tasks,
                dataset_label=task.dataset_type,
                task=task,
                row_count=len(rows),
                elapsed_s=elapsed_seconds(task_started_at),
            )
        except Exception as exc:  # noqa: BLE001
            _record_standard_error(
                logger=logger,
                dataset_label=task.dataset_type,
                task=task,
                elapsed_s=elapsed_seconds(task_started_at),
                key=key,
                task_errors=task_errors,
                exc=exc,
            )
    return VolatilityFetchResultDTO(rows=task_results, errors=task_errors)


def _store_standard_success(
    *,
    task: TTask,
    rows: list[TRow],
    key: tuple[Any, ...],
    result_rows: dict[Any, list[TRow]],
    on_task_complete: Callable[[TTask, list[TRow]], None] | None,
) -> None:
    if on_task_complete is not None:
        on_task_complete(task, rows)
    result_rows[key] = rows


def _log_standard_done(
    *,
    logger: logging.Logger,
    idx: int,
    total_tasks: int,
    dataset_label: str,
    task: CandleFetchTaskDTO | OpenInterestFetchTaskDTO | FundingFetchTaskDTO | VolatilityFetchTaskDTO,
    row_count: int,
    elapsed_s: int,
) -> None:
    logger.info(
        "Fetch done [%s/%s] type=%s exchange=%s market=%s symbol=%s timeframe=%s rows=%s elapsed_s=%s",
        idx,
        total_tasks,
        dataset_label,
        task.exchange,
        getattr(task, "market", "perp"),
        task.symbol,
        task.timeframe,
        row_count,
        elapsed_s,
    )


def _record_standard_error(
    *,
    logger: logging.Logger,
    dataset_label: str,
    task: CandleFetchTaskDTO | OpenInterestFetchTaskDTO | FundingFetchTaskDTO | VolatilityFetchTaskDTO,
    elapsed_s: int,
    key: tuple[Any, ...],
    task_errors: dict[Any, str],
    exc: Exception,
) -> None:
    logger.exception(
        "Fetch error type=%s exchange=%s market=%s symbol=%s timeframe=%s elapsed_s=%s",
        dataset_label,
        task.exchange,
        getattr(task, "market", "perp"),
        task.symbol,
        task.timeframe,
        elapsed_s,
    )
    task_errors[key] = str(exc)
