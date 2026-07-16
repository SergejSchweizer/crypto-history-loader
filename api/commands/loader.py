"""Bronze build command implementation."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from typing import Any, cast

from api.commands import loader_compat as _loader_compat
from api.commands import loader_fetchers as _loader_fetchers
from api.commands import loader_output_utils as _loader_output_utils
from api.commands import loader_parser as _loader_parser
from api.commands.loader_dataset_handlers import (
    populate_funding_output,
    populate_ohlcv_output,
    populate_open_interest_output,
    populate_trades_output,
    populate_volatility_output,
)
from api.commands.loader_execution import FetchAllTaskGroupsResult
from api.commands.loader_execution import fetch_all_task_groups as fetch_all_task_groups_execution
from api.commands.loader_fetchers import BronzeSymbolFetchDependencies
from api.commands.loader_workflow import BronzeWorkflowDependencies
from api.commands.loader_workflow import run_bronze_build as _run_bronze_build_workflow
from application.datasets import dataset_spec
from application.dto import (
    CandleFetchTaskDTO,
    FundingFetchTaskDTO,
    OpenInterestFetchTaskDTO,
    TradeFetchTaskDTO,
    VolatilityFetchTaskDTO,
)
from application.services.bronze_reporting_service import (
    symbol_progress_rows,
    symbol_progress_rows_from_dataset_tasks,
    trade_error_breakdown,
)
from application.services.bronze_runtime_service import (
    BronzeRuntimeAdapter,
    BronzeRuntimeBoundsContext,
    resolve_symbol_start_open_ms_bound,
)
from application.services.fetch_service import (
    fetch_candle_tasks_parallel,
    fetch_funding_tasks_parallel,
    fetch_open_interest_tasks_parallel,
    fetch_trade_tasks_parallel,
    fetch_volatility_tasks_parallel,
)
from application.services.gapfill_service import (
    _last_closed_open_ms as last_closed_open_ms,  # pyright: ignore[reportPrivateUsage]
)
from application.services.gapfill_service import (
    _missing_ranges_ms as missing_ranges_ms,  # pyright: ignore[reportPrivateUsage]
)
from application.services.lake_maintenance_service import ensure_bronze_sidecars
from application.services.lake_query_service import (
    latest_open_time_in_lake,
    latest_open_time_in_lake_by_dataset,
    open_times_in_lake,
    open_times_in_lake_by_dataset,
)
from application.services.runtime_service import SingleInstanceError, SingleInstanceLock
from application.services.storage_service import persist_loader_outputs_dto
from ingestion.funding import (
    FundingPoint,
    fetch_funding_all_history,
    fetch_funding_range,
    funding_interval_to_milliseconds,
    normalize_funding_timeframe,
)
from ingestion.open_interest import (
    OpenInterestPoint,
    fetch_open_interest_all_history,
    fetch_open_interest_range,
    normalize_open_interest_timeframe,
    open_interest_interval_to_milliseconds,
)
from ingestion.spot_ohlcv import (
    Exchange,
    Market,
    SpotCandle,
    fetch_candles_all_history,
    fetch_candles_range,
    interval_to_milliseconds,
    normalize_storage_symbol,
)
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick, fetch_trades_all_history, fetch_trades_range
from ingestion.volatility import (
    VolatilityPoint,
    fetch_volatility_index_all_history,
    fetch_volatility_index_range,
    normalize_volatility_timeframe,
    volatility_interval_to_milliseconds,
)

OPEN_INTEREST_DATASET_TYPE = dataset_spec("open_interest").dataset_type


_RUNTIME_ADAPTER = BronzeRuntimeAdapter()
_last_closed_open_ms = last_closed_open_ms
_missing_ranges_ms = missing_ranges_ms
_serialize_candle = _loader_output_utils.serialize_candle
_sidecar_path_list = _loader_output_utils.sidecar_path_list
_sanitize_symbols = _loader_compat.sanitize_symbols
_resolved_symbol_groups = _loader_compat.resolved_symbol_groups
_build_bronze_fetch_plan = _loader_compat.build_bronze_fetch_plan
_build_bronze_execution_policy = _loader_compat.build_bronze_execution_policy
_task_key_tuple_to_string = _loader_compat.task_key_tuple_to_string
_volatility_task_key_map = _loader_compat.volatility_task_key_map
_dataset_task_key_maps = _loader_compat.dataset_task_key_maps
_hydrate_checkpoint_aliases = _loader_compat.hydrate_checkpoint_aliases
_bronze_checkpoint_fingerprint = _loader_compat.bronze_checkpoint_fingerprint
_bronze_checkpoint_path = _loader_compat.bronze_checkpoint_path
_load_bronze_checkpoint = _loader_compat.load_bronze_checkpoint
_write_bronze_checkpoint = _loader_compat.write_bronze_checkpoint
_parse_start_date_to_open_ms = _loader_compat.parse_start_date_to_open_ms
_canonical_symbol_key = _loader_compat.canonical_symbol_key
_parse_symbol_start_dates = _loader_compat.parse_symbol_start_dates
_parse_exchange_symbol_start_dates = _loader_compat.parse_exchange_symbol_start_dates


def _current_runtime_bounds_context() -> BronzeRuntimeBoundsContext:
    """Return effective runtime bounds context with global fallback support."""

    return _RUNTIME_ADAPTER.context


def add_bronze_build_parser(subparsers: Any) -> None:
    """Register canonical ``bronze-build`` parser."""

    _loader_parser.add_bronze_build_parser(subparsers)


def _symbol_fetch_dependencies() -> BronzeSymbolFetchDependencies:
    """Build symbol-fetch dependency adapters from current loader module globals."""

    return _loader_fetchers.build_symbol_fetch_dependencies(
        open_times_in_lake=open_times_in_lake,
        open_times_in_lake_by_dataset=open_times_in_lake_by_dataset,
        latest_open_time_in_lake=latest_open_time_in_lake,
        latest_open_time_in_lake_by_dataset=latest_open_time_in_lake_by_dataset,
        normalize_storage_symbol=normalize_storage_symbol,
        interval_to_milliseconds=interval_to_milliseconds,
        open_interest_interval_to_milliseconds=open_interest_interval_to_milliseconds,
        funding_interval_to_milliseconds=funding_interval_to_milliseconds,
        volatility_interval_to_milliseconds=volatility_interval_to_milliseconds,
        normalize_open_interest_timeframe=normalize_open_interest_timeframe,
        normalize_funding_timeframe=normalize_funding_timeframe,
        normalize_volatility_timeframe=normalize_volatility_timeframe,
        last_closed_open_ms=_last_closed_open_ms,
        missing_ranges_ms=_missing_ranges_ms,
        fetch_candles_all_history=fetch_candles_all_history,
        fetch_candles_range=fetch_candles_range,
        fetch_open_interest_all_history=fetch_open_interest_all_history,
        fetch_open_interest_range=fetch_open_interest_range,
        fetch_funding_all_history=fetch_funding_all_history,
        fetch_funding_range=fetch_funding_range,
        fetch_volatility_index_all_history=fetch_volatility_index_all_history,
        fetch_volatility_index_range=fetch_volatility_index_range,
        fetch_trades_all_history=fetch_trades_all_history,
        fetch_trades_range=fetch_trades_range,
    )


def _fetch_symbol_candles(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[SpotCandle]], None] | None = None,
) -> list[SpotCandle]:
    return _loader_fetchers.fetch_symbol_candles(
        dependencies=_symbol_fetch_dependencies(),
        runtime_context=_current_runtime_bounds_context(),
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        on_history_chunk=on_history_chunk,
    )


def _fetch_symbol_open_interest(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[OpenInterestPoint]], None] | None = None,
) -> list[OpenInterestPoint]:
    return _loader_fetchers.fetch_symbol_open_interest(
        dependencies=_symbol_fetch_dependencies(),
        runtime_context=_current_runtime_bounds_context(),
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        on_history_chunk=on_history_chunk,
    )


def _fetch_symbol_funding(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[FundingPoint]], None] | None = None,
) -> list[FundingPoint]:
    return _loader_fetchers.fetch_symbol_funding(
        dependencies=_symbol_fetch_dependencies(),
        runtime_context=_current_runtime_bounds_context(),
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        on_history_chunk=on_history_chunk,
    )


def _fetch_symbol_volatility_index_data(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[VolatilityPoint]], None] | None = None,
) -> list[VolatilityPoint]:
    return _loader_fetchers.fetch_symbol_volatility_index_data(
        dependencies=_symbol_fetch_dependencies(),
        runtime_context=_current_runtime_bounds_context(),
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        on_history_chunk=on_history_chunk,
    )


def _fetch_symbol_trades(
    exchange: Exchange,
    market: TradeMarket,
    symbol: str,
    lake_root: str,
    on_history_chunk: Callable[[list[TradeTick | OptionTradeTick]], None] | None = None,
) -> list[TradeTick | OptionTradeTick]:
    return _loader_fetchers.fetch_symbol_trades(
        dependencies=_symbol_fetch_dependencies(),
        runtime_context=_current_runtime_bounds_context(),
        exchange=exchange,
        market=market,
        symbol=symbol,
        lake_root=lake_root,
        on_history_chunk=on_history_chunk,
    )


def _symbol_start_open_ms_bound(exchange: Exchange, symbol: str) -> int | None:
    """Resolve effective start bound for Bronze fetches.

    In default incremental mode (``tail_delta_only``), cap backfill to the
    last 30 days even when older static bounds are configured.
    """

    return resolve_symbol_start_open_ms_bound(
        exchange=exchange,
        symbol=symbol,
        context=_current_runtime_bounds_context(),
    )


def _configure_bronze_start_bounds(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Initialize Bronze start-bound adapter state from CLI/config args and emit boundary logs."""

    _RUNTIME_ADAPTER.configure(
        tail_delta_only=bool(getattr(args, "tail_delta_only", False)),
        start_date=cast(str | None, getattr(args, "start_date", None)),
        symbol_start_dates=cast(list[str] | None, getattr(args, "symbol_start_dates", None)),
        exchange_symbol_start_dates=cast(list[str] | None, getattr(args, "exchange_symbol_start_dates", None)),
        logger=logger,
    )


def _fetch_candle_tasks_parallel(
    tasks: list[tuple[Exchange, Market, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_task_chunk: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
) -> tuple[dict[tuple[Exchange, Market, str, str], list[SpotCandle]], dict[tuple[Exchange, Market, str, str], str]]:
    service_tasks = [
        CandleFetchTaskDTO(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe)
        for exchange, market, symbol, timeframe in tasks
    ]
    result = fetch_candle_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_candles,
        shared_semaphore=shared_semaphore,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )
    return result.rows, result.errors


def _fetch_open_interest_tasks_parallel(
    open_interest_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_task_chunk: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
) -> tuple[dict[tuple[Exchange, str, str], list[OpenInterestPoint]], dict[tuple[Exchange, str, str], str]]:
    service_tasks = [
        OpenInterestFetchTaskDTO(exchange=exchange, symbol=symbol, timeframe=timeframe)
        for exchange, symbol, timeframe in open_interest_tasks
    ]
    result = fetch_open_interest_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_open_interest,
        shared_semaphore=shared_semaphore,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )
    return result.rows, result.errors


def _fetch_funding_tasks_parallel(
    funding_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_task_chunk: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
) -> tuple[dict[tuple[Exchange, str, str], list[FundingPoint]], dict[tuple[Exchange, str, str], str]]:
    service_tasks = [
        FundingFetchTaskDTO(exchange=exchange, symbol=symbol, timeframe=timeframe)
        for exchange, symbol, timeframe in funding_tasks
    ]
    result = fetch_funding_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_funding,
        shared_semaphore=shared_semaphore,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )
    return result.rows, result.errors


def _fetch_volatility_index_data_tasks_parallel(
    volatility_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[VolatilityFetchTaskDTO, list[VolatilityPoint]], None] | None = None,
    on_task_chunk: Callable[[VolatilityFetchTaskDTO, list[VolatilityPoint]], None] | None = None,
) -> tuple[dict[tuple[Exchange, str, str], list[VolatilityPoint]], dict[tuple[Exchange, str, str], str]]:
    service_tasks = [
        VolatilityFetchTaskDTO(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            dataset_type="volatility_index_data",
        )
        for exchange, symbol, timeframe in volatility_tasks
    ]
    result = fetch_volatility_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_volatility_index_data,
        shared_semaphore=shared_semaphore,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )
    return result.rows, result.errors


def _fetch_trade_tasks_parallel(
    trade_tasks: list[tuple[Exchange, TradeMarket, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
    on_task_chunk: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
) -> tuple[
    dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]],
    dict[tuple[Exchange, TradeMarket, str], str],
]:
    service_tasks = [
        TradeFetchTaskDTO(exchange=exchange, market=market, symbol=symbol) for exchange, market, symbol in trade_tasks
    ]
    result = fetch_trade_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_trades,
        shared_semaphore=shared_semaphore,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )
    return result.rows, result.errors


def _fetch_all_task_groups(
    candle_tasks: list[tuple[Exchange, Market, str, str]],
    open_interest_tasks: list[tuple[Exchange, str, str]],
    funding_tasks: list[tuple[Exchange, str, str]],
    volatility_index_data_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    candle_concurrency: int,
    open_interest_concurrency: int,
    funding_concurrency: int,
    volatility_concurrency: int,
    logger: logging.Logger,
    on_candle_task_complete: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_open_interest_task_complete: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_funding_task_complete: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_volatility_index_data_task_complete: Callable[[VolatilityFetchTaskDTO, list[VolatilityPoint]], None]
    | None = None,
    trade_tasks: list[tuple[Exchange, TradeMarket, str]] | None = None,
    trade_concurrency: int = 1,
    on_trade_task_complete: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
    on_candle_task_chunk: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_open_interest_task_chunk: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_funding_task_chunk: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_volatility_index_data_task_chunk: Callable[[VolatilityFetchTaskDTO, list[VolatilityPoint]], None] | None = None,
    on_trade_task_chunk: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
) -> FetchAllTaskGroupsResult:
    """Fetch task groups sequentially across dataset types."""

    return fetch_all_task_groups_execution(
        candle_tasks=cast(list[tuple[str, str, str, str]], candle_tasks),
        open_interest_tasks=cast(list[tuple[str, str, str]], open_interest_tasks),
        funding_tasks=cast(list[tuple[str, str, str]], funding_tasks),
        volatility_tasks=cast(list[tuple[str, str, str]], volatility_index_data_tasks),
        trade_tasks=cast(list[tuple[str, str, str]] | None, trade_tasks),
        lake_root=lake_root,
        candle_concurrency=candle_concurrency,
        open_interest_concurrency=open_interest_concurrency,
        funding_concurrency=funding_concurrency,
        volatility_concurrency=volatility_concurrency,
        trade_concurrency=trade_concurrency,
        logger=logger,
        fetch_candles_fn=cast(Callable[..., object], _fetch_candle_tasks_parallel),
        fetch_open_interest_fn=cast(Callable[..., object], _fetch_open_interest_tasks_parallel),
        fetch_funding_fn=cast(Callable[..., object], _fetch_funding_tasks_parallel),
        fetch_volatility_fn=cast(Callable[..., object], _fetch_volatility_index_data_tasks_parallel),
        fetch_trades_fn=cast(Callable[..., object], _fetch_trade_tasks_parallel),
        on_candle_task_complete=cast(Callable[[object, list[object]], None] | None, on_candle_task_complete),
        on_open_interest_task_complete=cast(
            Callable[[object, list[object]], None] | None, on_open_interest_task_complete
        ),
        on_funding_task_complete=cast(Callable[[object, list[object]], None] | None, on_funding_task_complete),
        on_volatility_task_complete=cast(
            Callable[[object, list[object]], None] | None, on_volatility_index_data_task_complete
        ),
        on_trade_task_complete=cast(Callable[[object, list[object]], None] | None, on_trade_task_complete),
        on_candle_task_chunk=cast(Callable[[object, list[object]], None] | None, on_candle_task_chunk),
        on_open_interest_task_chunk=cast(Callable[[object, list[object]], None] | None, on_open_interest_task_chunk),
        on_funding_task_chunk=cast(Callable[[object, list[object]], None] | None, on_funding_task_chunk),
        on_volatility_task_chunk=cast(
            Callable[[object, list[object]], None] | None, on_volatility_index_data_task_chunk
        ),
        on_trade_task_chunk=cast(Callable[[object, list[object]], None] | None, on_trade_task_chunk),
    )


def run_bronze_build(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run bronze-build command."""

    _run_bronze_build_workflow(args=args, logger=logger, dependencies=_bronze_workflow_dependencies())


def _bronze_workflow_dependencies() -> BronzeWorkflowDependencies:
    """Build workflow dependencies from current loader module globals."""

    return BronzeWorkflowDependencies(
        configure_bronze_start_bounds=_configure_bronze_start_bounds,
        current_runtime_bounds_context=_current_runtime_bounds_context,
        single_instance_lock=SingleInstanceLock,
        single_instance_error=SingleInstanceError,
        build_bronze_fetch_plan=_build_bronze_fetch_plan,
        build_bronze_execution_policy=_build_bronze_execution_policy,
        bronze_checkpoint_path=_bronze_checkpoint_path,
        bronze_checkpoint_fingerprint=_bronze_checkpoint_fingerprint,
        load_bronze_checkpoint=_load_bronze_checkpoint,
        hydrate_checkpoint_aliases=_hydrate_checkpoint_aliases,
        write_bronze_checkpoint=_write_bronze_checkpoint,
        fetch_all_task_groups=_fetch_all_task_groups,
        persist_loader_outputs=persist_loader_outputs_dto,
        sidecar_path_list=_sidecar_path_list,
        ensure_bronze_sidecars=ensure_bronze_sidecars,
        populate_ohlcv_output=populate_ohlcv_output,
        populate_open_interest_output=populate_open_interest_output,
        populate_funding_output=populate_funding_output,
        populate_volatility_output=populate_volatility_output,
        populate_trades_output=populate_trades_output,
        symbol_progress_rows=symbol_progress_rows,
        symbol_progress_rows_from_dataset_tasks=symbol_progress_rows_from_dataset_tasks,
        trade_error_breakdown=trade_error_breakdown,
        candle_serializer=_serialize_candle,
        open_interest_dataset_type=OPEN_INTEREST_DATASET_TYPE,
    )
