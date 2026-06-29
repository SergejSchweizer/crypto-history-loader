"""Bronze build command implementation."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from api.commands import loader_output_utils as _loader_output_utils
from api.commands.loader_dataset_handlers import (
    populate_funding_output,
    populate_ohlcv_output,
    populate_oi_output,
    populate_trades_output,
    populate_volatility_output,
)
from api.commands.loader_execution import fetch_all_task_groups as fetch_all_task_groups_execution
from api.commands.loader_output import BronzeRunState, IncrementalPersistor, finalize_bronze_output
from api.commands.loader_planning import (
    build_bronze_fetch_plan,
    canonical_symbol_key,
    parse_exchange_symbol_start_dates,
    parse_start_date_to_open_ms,
    parse_symbol_start_dates,
    resolved_symbol_groups,
    sanitize_symbols,
)
from application.datasets import DATASET_REGISTRY, dataset_spec
from application.dto import (
    BronzeExecutionPolicyDTO,
    BronzeFetchPlanDTO,
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
    BronzeRuntimeBoundsContext,
    CheckpointDataset,
    add_completed_checkpoint_key,
    apply_checkpoint_filter_with_key_maps,
    bronze_checkpoint_fingerprint,
    bronze_checkpoint_key_maps,
    bronze_checkpoint_path,
    build_bronze_execution_policy,
    build_bronze_runtime_bounds_context,
    checkpoint_task_keys,
    dataset_task_key_maps,
    has_checkpoint_state,
    hydrate_checkpoint_aliases,
    load_bronze_checkpoint,
    resolve_symbol_start_open_ms_bound,
    task_key_tuple_to_string,
    volatility_task_key_map,
    write_bronze_checkpoint,
)
from application.services.fetch_service import (
    fetch_candle_tasks_parallel,
    fetch_funding_tasks_parallel,
    fetch_open_interest_tasks_parallel,
    fetch_symbol_candles,
    fetch_symbol_funding,
    fetch_symbol_open_interest,
    fetch_symbol_trades,
    fetch_symbol_volatility,
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
from application.services.runtime_service import SingleInstanceError, SingleInstanceLock, fetch_concurrency
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
from ingestion.spot import (
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

MARKET_CHOICES = tuple(DATASET_REGISTRY.keys())
OI_DATASET_TYPE = dataset_spec("oi").dataset_type


_RUNTIME_BOUNDS_CONTEXT = BronzeRuntimeBoundsContext(
    tail_delta_only=False,
    global_start_open_ms=None,
    symbol_start_open_ms={},
    exchange_symbol_start_open_ms={},
)
_last_closed_open_ms = last_closed_open_ms
_missing_ranges_ms = missing_ranges_ms
_serialize_candle = _loader_output_utils.serialize_candle
_sidecar_path_list = _loader_output_utils.sidecar_path_list


def _current_runtime_bounds_context() -> BronzeRuntimeBoundsContext:
    """Return effective runtime bounds context with global fallback support."""

    return _RUNTIME_BOUNDS_CONTEXT


def _sanitize_symbols(raw_symbols: object, logger: logging.Logger) -> list[str]:  # pyright: ignore[reportUnusedFunction]
    """Return validated symbol list, dropping null/blank/non-string entries."""

    return sanitize_symbols(raw_symbols=raw_symbols, logger=logger)


def _resolved_symbol_groups(  # pyright: ignore[reportUnusedFunction]
    args: argparse.Namespace, logger: logging.Logger
) -> tuple[list[str], list[str], list[str]]:
    """Return deterministically ordered symbol groups for Bronze task planning."""

    return resolved_symbol_groups(args=args, logger=logger)


def _build_bronze_fetch_plan(args: argparse.Namespace, logger: logging.Logger) -> BronzeFetchPlanDTO:
    """Build deterministic Bronze task plan shared across all dataset fetchers."""

    return build_bronze_fetch_plan(args=args, logger=logger)


def _build_bronze_execution_policy() -> BronzeExecutionPolicyDTO:
    """Build standardized Bronze execution policy."""

    return build_bronze_execution_policy(configured_concurrency=fetch_concurrency())


def _task_key_tuple_to_string(parts: tuple[object, ...]) -> str:
    """Serialize tuple task key to stable checkpoint string."""

    return task_key_tuple_to_string(parts)


def _volatility_task_key_map(plan: BronzeFetchPlanDTO) -> dict[tuple[Exchange, str, str], str]:
    """Return tuple->checkpoint-key mapping for volatility dataset tasks."""

    return volatility_task_key_map(plan)


def _dataset_task_key_maps(
    plan: BronzeFetchPlanDTO,
) -> tuple[
    dict[tuple[Exchange, Market, str, str], str],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, TradeMarket, str], str],
]:
    """Return tuple->checkpoint-key mappings derived from registry dataset tasks."""

    return dataset_task_key_maps(plan)


def _hydrate_checkpoint_aliases(
    *,
    completed: dict[str, set[str]],
    candle_tasks: list[tuple[Exchange, Market, str, str]],
    oi_tasks: list[tuple[Exchange, str, str]],
    funding_tasks: list[tuple[Exchange, str, str]],
    volatility_index_data_tasks: list[tuple[Exchange, str, str]],
    trade_tasks: list[tuple[Exchange, TradeMarket, str]],
    candle_key_map: dict[tuple[Exchange, Market, str, str], str],
    oi_key_map: dict[tuple[Exchange, str, str], str],
    funding_key_map: dict[tuple[Exchange, str, str], str],
    volatility_key_map: dict[tuple[Exchange, str, str], str],
    trade_key_map: dict[tuple[Exchange, TradeMarket, str], str],
) -> None:
    """Augment completed checkpoint keys with registry aliases for backward compatibility."""

    hydrate_checkpoint_aliases(
        completed=completed,
        candle_tasks=candle_tasks,
        oi_tasks=oi_tasks,
        funding_tasks=funding_tasks,
        volatility_index_data_tasks=volatility_index_data_tasks,
        trade_tasks=trade_tasks,
        candle_key_map=candle_key_map,
        oi_key_map=oi_key_map,
        funding_key_map=funding_key_map,
        volatility_key_map=volatility_key_map,
        trade_key_map=trade_key_map,
    )


def _bronze_checkpoint_fingerprint(args: argparse.Namespace, plan: BronzeFetchPlanDTO) -> str:
    """Build stable fingerprint for one Bronze invocation plan."""

    return bronze_checkpoint_fingerprint(args=args, plan=plan)


def _bronze_checkpoint_path() -> Path:
    """Return Bronze restart-checkpoint path."""

    return bronze_checkpoint_path()


def _load_bronze_checkpoint(path: Path, fingerprint: str, logger: logging.Logger) -> dict[str, set[str]]:
    """Load matching Bronze checkpoint completed-task sets."""

    return load_bronze_checkpoint(path=path, fingerprint=fingerprint, logger=logger)


def _write_bronze_checkpoint(
    path: Path,
    *,
    fingerprint: str,
    completed: dict[str, set[str]],
) -> None:
    """Persist Bronze checkpoint atomically."""

    write_bronze_checkpoint(path, fingerprint=fingerprint, completed=completed)


def _add_ingest_parser(
    subparsers: Any,
    *,
    command_name: str,
    help_text: str,
) -> None:
    """Register ingest parser."""

    parser = subparsers.add_parser(command_name, help=help_text)
    parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["deribit"],
        help="Optional list of exchanges to fetch in one run",
    )
    parser.add_argument(
        "--dataset",
        nargs="+",
        choices=MARKET_CHOICES,
        default=["spot"],
        help="One or more data types to fetch, e.g. --dataset spot perp oi funding",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTC", "ETH", "SOL"],
        help="Symbols used for all selected markets/datasets.",
    )
    parser.set_defaults(tail_delta_only=False)
    parser.add_argument(
        "--save-parquet-lake",
        action="store_true",
        help="Save fetched candles to parquet lake partitions",
    )
    parser.add_argument(
        "--lake-root",
        default="lake/bronze",
        help="Root directory for parquet lake files",
    )
    parser.add_argument(
        "--no-json-output",
        action="store_true",
        help="Suppress JSON output from bronze-build command",
    )
    parser.add_argument(
        "--tail-delta-only",
        dest="tail_delta_only",
        action="store_true",
        help="Fetch only new tail data after latest stored point (overrides default full-gap-fill mode).",
    )
    parser.add_argument(
        "--full-gap-fill",
        dest="tail_delta_only",
        action="store_false",
        help="Run full historical internal gap checks (default behavior).",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Inclusive UTC date boundary (YYYY-MM-DD) for Bronze ingestion history.",
    )
    parser.add_argument(
        "--symbol-start-dates",
        nargs="+",
        default=None,
        help="Per-symbol inclusive UTC start dates (SYMBOL=YYYY-MM-DD), e.g. BTC=2023-04-24",
    )
    parser.add_argument(
        "--exchange-symbol-start-dates",
        nargs="+",
        default=None,
        help=(
            "Per exchange-symbol inclusive UTC start dates (EXCHANGE:SYMBOL=YYYY-MM-DD), e.g. deribit:BTC=2023-04-24"
        ),
    )


def add_bronze_build_parser(subparsers: Any) -> None:
    """Register canonical ``bronze-build`` parser."""

    _add_ingest_parser(
        subparsers,
        command_name="bronze-build",
        help_text="Bronze medallion ingest from supported exchanges",
    )


def _fetch_symbol_candles(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[SpotCandle]], None] | None = None,
) -> list[SpotCandle]:
    return fetch_symbol_candles(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_in_lake,
        symbol_normalizer=normalize_storage_symbol,
        interval_ms_resolver=interval_to_milliseconds,
        now_open_resolver=_last_closed_open_ms,
        ranges_builder=_missing_ranges_ms,
        history_fetcher=fetch_candles_all_history,
        range_fetcher=fetch_candles_range,
        latest_open_time_reader=latest_open_time_in_lake,
        tail_delta_only=_current_runtime_bounds_context().tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=_symbol_start_open_ms_bound(exchange=exchange, symbol=symbol),
    )


def _fetch_symbol_open_interest(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[OpenInterestPoint]], None] | None = None,
) -> list[OpenInterestPoint]:
    return fetch_symbol_open_interest(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_in_lake_by_dataset,
        timeframe_normalizer=normalize_open_interest_timeframe,
        symbol_normalizer=normalize_storage_symbol,
        interval_ms_resolver=open_interest_interval_to_milliseconds,
        now_open_resolver=_last_closed_open_ms,
        ranges_builder=_missing_ranges_ms,
        history_fetcher=fetch_open_interest_all_history,
        range_fetcher=fetch_open_interest_range,
        latest_open_time_reader=latest_open_time_in_lake_by_dataset,
        tail_delta_only=_current_runtime_bounds_context().tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=_symbol_start_open_ms_bound(exchange=exchange, symbol=symbol),
    )


def _fetch_symbol_funding(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[FundingPoint]], None] | None = None,
) -> list[FundingPoint]:
    return fetch_symbol_funding(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_in_lake_by_dataset,
        timeframe_normalizer=normalize_funding_timeframe,
        symbol_normalizer=normalize_storage_symbol,
        interval_ms_resolver=funding_interval_to_milliseconds,
        now_open_resolver=_last_closed_open_ms,
        ranges_builder=_missing_ranges_ms,
        history_fetcher=fetch_funding_all_history,
        range_fetcher=fetch_funding_range,
        latest_open_time_reader=latest_open_time_in_lake_by_dataset,
        tail_delta_only=_current_runtime_bounds_context().tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=_symbol_start_open_ms_bound(exchange=exchange, symbol=symbol),
    )


def _fetch_symbol_volatility_index_data(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[VolatilityPoint]], None] | None = None,
) -> list[VolatilityPoint]:
    return fetch_symbol_volatility(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        dataset_type="volatility_index_data",
        open_times_reader=open_times_in_lake_by_dataset,
        timeframe_normalizer=normalize_volatility_timeframe,
        interval_ms_resolver=volatility_interval_to_milliseconds,
        now_open_resolver=_last_closed_open_ms,
        ranges_builder=_missing_ranges_ms,
        history_fetcher=fetch_volatility_index_all_history,
        range_fetcher=fetch_volatility_index_range,
        latest_open_time_reader=latest_open_time_in_lake_by_dataset,
        tail_delta_only=_current_runtime_bounds_context().tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=_symbol_start_open_ms_bound(exchange=exchange, symbol=symbol),
    )


def _fetch_symbol_trades(
    exchange: Exchange,
    market: TradeMarket,
    symbol: str,
    lake_root: str,
    on_history_chunk: Callable[[list[TradeTick | OptionTradeTick]], None] | None = None,
) -> list[TradeTick | OptionTradeTick]:
    return fetch_symbol_trades(
        exchange=exchange,
        market=market,
        symbol=symbol,
        lake_root=lake_root,
        symbol_normalizer=normalize_storage_symbol,
        history_fetcher=fetch_trades_all_history,
        range_fetcher=fetch_trades_range,
        latest_open_time_reader=latest_open_time_in_lake_by_dataset,
        tail_delta_only=_current_runtime_bounds_context().tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=_symbol_start_open_ms_bound(exchange=exchange, symbol=symbol),
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


def _parse_start_date_to_open_ms(start_date: str | None) -> int | None:  # pyright: ignore[reportUnusedFunction]
    """Parse inclusive UTC start date ``YYYY-MM-DD`` to epoch milliseconds."""

    return parse_start_date_to_open_ms(start_date=start_date)


def _canonical_symbol_key(symbol: str) -> str:  # pyright: ignore[reportUnusedFunction]
    """Return canonical base symbol key for per-symbol start-date matching."""

    return canonical_symbol_key(symbol=symbol)


def _parse_symbol_start_dates(entries: list[str] | None) -> dict[str, int]:  # pyright: ignore[reportUnusedFunction]
    """Parse ``SYMBOL=YYYY-MM-DD`` entries into canonical symbol->epoch-ms map."""

    return parse_symbol_start_dates(entries=entries)


def _parse_exchange_symbol_start_dates(  # pyright: ignore[reportUnusedFunction]
    entries: list[str] | None,
) -> dict[str, int]:
    """Parse ``EXCHANGE:SYMBOL=YYYY-MM-DD`` entries into canonical exchange:symbol->epoch-ms map."""

    return parse_exchange_symbol_start_dates(entries=entries)


def _configure_bronze_start_bounds(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Initialize Bronze start-bound globals from CLI/config args and emit boundary logs."""

    global _RUNTIME_BOUNDS_CONTEXT
    _RUNTIME_BOUNDS_CONTEXT = build_bronze_runtime_bounds_context(
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
    oi_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_task_chunk: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
) -> tuple[dict[tuple[Exchange, str, str], list[OpenInterestPoint]], dict[tuple[Exchange, str, str], str]]:
    service_tasks = [
        OpenInterestFetchTaskDTO(exchange=exchange, symbol=symbol, timeframe=timeframe)
        for exchange, symbol, timeframe in oi_tasks
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
    oi_tasks: list[tuple[Exchange, str, str]],
    funding_tasks: list[tuple[Exchange, str, str]],
    volatility_index_data_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    candle_concurrency: int,
    oi_concurrency: int,
    funding_concurrency: int,
    volatility_concurrency: int,
    logger: logging.Logger,
    on_candle_task_complete: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_oi_task_complete: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_funding_task_complete: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_volatility_index_data_task_complete: Callable[[VolatilityFetchTaskDTO, list[VolatilityPoint]], None]
    | None = None,
    trade_tasks: list[tuple[Exchange, TradeMarket, str]] | None = None,
    trade_concurrency: int = 1,
    on_trade_task_complete: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
    on_candle_task_chunk: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_oi_task_chunk: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_funding_task_chunk: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_volatility_index_data_task_chunk: Callable[[VolatilityFetchTaskDTO, list[VolatilityPoint]], None] | None = None,
    on_trade_task_chunk: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
) -> tuple[
    dict[tuple[Exchange, Market, str, str], list[SpotCandle]],
    dict[tuple[Exchange, Market, str, str], str],
    dict[tuple[Exchange, str, str], list[OpenInterestPoint]],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, str, str], list[FundingPoint]],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, str, str], list[VolatilityPoint]],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]],
    dict[tuple[Exchange, TradeMarket, str], str],
]:
    """Fetch task groups sequentially across dataset types."""

    return cast(
        tuple[
            dict[tuple[Exchange, Market, str, str], list[SpotCandle]],
            dict[tuple[Exchange, Market, str, str], str],
            dict[tuple[Exchange, str, str], list[OpenInterestPoint]],
            dict[tuple[Exchange, str, str], str],
            dict[tuple[Exchange, str, str], list[FundingPoint]],
            dict[tuple[Exchange, str, str], str],
            dict[tuple[Exchange, str, str], list[VolatilityPoint]],
            dict[tuple[Exchange, str, str], str],
            dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]],
            dict[tuple[Exchange, TradeMarket, str], str],
        ],
        fetch_all_task_groups_execution(
            candle_tasks=cast(list[tuple[str, str, str, str]], candle_tasks),
            oi_tasks=cast(list[tuple[str, str, str]], oi_tasks),
            funding_tasks=cast(list[tuple[str, str, str]], funding_tasks),
            volatility_tasks=cast(list[tuple[str, str, str]], volatility_index_data_tasks),
            trade_tasks=cast(list[tuple[str, str, str]] | None, trade_tasks),
            lake_root=lake_root,
            candle_concurrency=candle_concurrency,
            oi_concurrency=oi_concurrency,
            funding_concurrency=funding_concurrency,
            volatility_concurrency=volatility_concurrency,
            trade_concurrency=trade_concurrency,
            logger=logger,
            fetch_candles_fn=cast(Callable[..., object], _fetch_candle_tasks_parallel),
            fetch_oi_fn=cast(Callable[..., object], _fetch_open_interest_tasks_parallel),
            fetch_funding_fn=cast(Callable[..., object], _fetch_funding_tasks_parallel),
            fetch_volatility_fn=cast(Callable[..., object], _fetch_volatility_index_data_tasks_parallel),
            fetch_trades_fn=cast(Callable[..., object], _fetch_trade_tasks_parallel),
            on_candle_task_complete=cast(Callable[[object, list[object]], None] | None, on_candle_task_complete),
            on_oi_task_complete=cast(Callable[[object, list[object]], None] | None, on_oi_task_complete),
            on_funding_task_complete=cast(Callable[[object, list[object]], None] | None, on_funding_task_complete),
            on_volatility_task_complete=cast(
                Callable[[object, list[object]], None] | None, on_volatility_index_data_task_complete
            ),
            on_trade_task_complete=cast(Callable[[object, list[object]], None] | None, on_trade_task_complete),
            on_candle_task_chunk=cast(Callable[[object, list[object]], None] | None, on_candle_task_chunk),
            on_oi_task_chunk=cast(Callable[[object, list[object]], None] | None, on_oi_task_chunk),
            on_funding_task_chunk=cast(Callable[[object, list[object]], None] | None, on_funding_task_chunk),
            on_volatility_task_chunk=cast(
                Callable[[object, list[object]], None] | None, on_volatility_index_data_task_chunk
            ),
            on_trade_task_chunk=cast(Callable[[object, list[object]], None] | None, on_trade_task_chunk),
        ),
    )


def run_bronze_build(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run bronze-build command."""

    _configure_bronze_start_bounds(args=args, logger=logger)
    if _current_runtime_bounds_context().tail_delta_only:
        rolling_bound = datetime.now(UTC) - timedelta(days=30)
        logger.info(
            "Bronze default tail-mode cap enabled max_missing_window_days=30 rolling_start_utc=%s",
            rolling_bound.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

    try:
        with SingleInstanceLock(".run/crypto-history-loader.lock"):
            plan = _build_bronze_fetch_plan(args=args, logger=logger)
            ohlcv_markets = plan.ohlcv_markets
            data_types = plan.data_types
            oi_requested = "oi" in data_types
            funding_requested = "funding" in data_types
            volatility_index_data_requested = "volatility_index_data" in data_types
            perp_trades_requested = "perp_trades" in data_types
            option_trades_requested = "option_trades" in data_types
            multi_market = len(data_types) > 1
            state = BronzeRunState.from_plan(plan)
            logger.info(
                "Deterministic schedule markets=%s symbols=%s perp_trade_symbols=%s option_trade_symbols=%s",
                data_types,
                plan.symbols,
                plan.perp_trade_symbols,
                plan.option_trade_symbols,
            )
            key_maps = bronze_checkpoint_key_maps(plan)
            candle_key_map = key_maps.candle
            oi_key_map = key_maps.oi
            funding_key_map = key_maps.funding
            volatility_key_map = key_maps.volatility_index_data
            trade_key_map = key_maps.trade
            checkpoint_path = _bronze_checkpoint_path()
            checkpoint_enabled = bool(args.save_parquet_lake) or checkpoint_path.exists()
            checkpoint_fingerprint = _bronze_checkpoint_fingerprint(args=args, plan=plan)
            checkpoint_completed = (
                _load_bronze_checkpoint(
                    path=checkpoint_path,
                    fingerprint=checkpoint_fingerprint,
                    logger=logger,
                )
                if checkpoint_enabled
                else {"candle": set(), "oi": set(), "funding": set(), "volatility_index_data": set(), "trade": set()}
            )
            _hydrate_checkpoint_aliases(
                completed=checkpoint_completed,
                candle_tasks=state.candle_tasks,
                oi_tasks=state.oi_tasks,
                funding_tasks=state.funding_tasks,
                volatility_index_data_tasks=state.volatility_index_data_tasks,
                trade_tasks=state.trade_tasks,
                candle_key_map=candle_key_map,
                oi_key_map=oi_key_map,
                funding_key_map=funding_key_map,
                volatility_key_map=volatility_key_map,
                trade_key_map=trade_key_map,
            )

            pending_tasks = apply_checkpoint_filter_with_key_maps(
                candle_tasks=state.candle_tasks,
                oi_tasks=state.oi_tasks,
                funding_tasks=state.funding_tasks,
                volatility_index_data_tasks=state.volatility_index_data_tasks,
                trade_tasks=state.trade_tasks,
                completed=checkpoint_completed,
                key_maps=key_maps,
            )
            state.candle_tasks = pending_tasks.candle_tasks
            state.oi_tasks = pending_tasks.oi_tasks
            state.funding_tasks = pending_tasks.funding_tasks
            state.volatility_index_data_tasks = pending_tasks.volatility_index_data_tasks
            state.trade_tasks = pending_tasks.trade_tasks
            if has_checkpoint_state(checkpoint_completed):
                logger.info(
                    (
                        "Resuming from Bronze checkpoint '%s' pending_tasks "
                        "candle=%s oi=%s funding=%s volatility_index_data=%s trade=%s"
                    ),
                    checkpoint_path,
                    len(state.candle_tasks),
                    len(state.oi_tasks),
                    len(state.funding_tasks),
                    len(state.volatility_index_data_tasks),
                    len(state.trade_tasks),
                )

            policy = _build_bronze_execution_policy()
            candle_concurrency = policy.candle_concurrency
            oi_concurrency = policy.oi_concurrency
            funding_concurrency = policy.funding_concurrency
            volatility_concurrency = policy.funding_concurrency
            trade_concurrency = policy.trade_concurrency
            incremental_parquet_on_fetch = bool(args.save_parquet_lake)
            logger.info(
                (
                    "Fetch mode enabled for spot/perp, oi, funding, volatility_index_data, and trades "
                    "with concurrency=%s (configured=%s)"
                ),
                policy.effective_concurrency,
                policy.configured_concurrency,
            )
            if incremental_parquet_on_fetch:
                logger.info("Incremental parquet flush enabled during fetch execution")

            def _mark_checkpoint_complete(dataset: str, key: tuple[object, ...]) -> None:
                add_completed_checkpoint_key(
                    completed=checkpoint_completed,
                    dataset=cast(CheckpointDataset, dataset),
                    key=key,
                    key_maps=key_maps,
                )
                if checkpoint_enabled:
                    _write_bronze_checkpoint(
                        checkpoint_path,
                        fingerprint=checkpoint_fingerprint,
                        completed=checkpoint_completed,
                    )

            incremental_persistor = IncrementalPersistor(
                lake_root=cast(str, args.lake_root),
                mark_checkpoint_complete=_mark_checkpoint_complete,
                persist_fn=persist_loader_outputs_dto,
            )

            fetch_results = cast(
                Any,
                _fetch_all_task_groups(
                    candle_tasks=state.candle_tasks,
                    oi_tasks=state.oi_tasks,
                    funding_tasks=state.funding_tasks,
                    volatility_index_data_tasks=state.volatility_index_data_tasks,
                    trade_tasks=state.trade_tasks,
                    lake_root=cast(str, args.lake_root),
                    candle_concurrency=candle_concurrency,
                    oi_concurrency=oi_concurrency,
                    funding_concurrency=funding_concurrency,
                    volatility_concurrency=volatility_concurrency,
                    trade_concurrency=trade_concurrency,
                    logger=logger,
                    on_candle_task_complete=(
                        lambda task, rows: incremental_persistor.on_candle_task_complete(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_oi_task_complete=(
                        lambda task, rows: incremental_persistor.on_oi_task_complete(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_funding_task_complete=(
                        lambda task, rows: incremental_persistor.on_funding_task_complete(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_candle_task_chunk=(
                        lambda task, rows: incremental_persistor.on_candle_task_chunk(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_oi_task_chunk=(lambda task, rows: incremental_persistor.on_oi_task_chunk(task, rows, logger))
                    if incremental_parquet_on_fetch
                    else None,
                    on_funding_task_chunk=(
                        lambda task, rows: incremental_persistor.on_funding_task_chunk(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_volatility_index_data_task_chunk=(
                        lambda task, rows: incremental_persistor.on_volatility_index_data_task_chunk(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_trade_task_complete=(
                        lambda task, rows: incremental_persistor.on_trade_task_complete(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_trade_task_chunk=(
                        lambda task, rows: incremental_persistor.on_trade_task_chunk(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                ),
            )
            if len(fetch_results) == 8:
                (
                    task_results,
                    task_errors,
                    oi_results,
                    oi_errors,
                    funding_results,
                    funding_errors,
                    trade_results,
                    trade_errors,
                ) = fetch_results
                volatility_index_data_results = {}
                volatility_index_data_errors = {}
            else:
                (
                    task_results,
                    task_errors,
                    oi_results,
                    oi_errors,
                    funding_results,
                    funding_errors,
                    volatility_index_data_results,
                    volatility_index_data_errors,
                    trade_results,
                    trade_errors,
                ) = fetch_results
            for key in task_results:
                _mark_checkpoint_complete("candle", key)
            for oi_key in oi_results:
                _mark_checkpoint_complete("oi", oi_key)
            for funding_key in funding_results:
                _mark_checkpoint_complete("funding", funding_key)
            for volatility_key in volatility_index_data_results:
                _mark_checkpoint_complete("volatility_index_data", volatility_key)
            for trade_key in trade_results:
                _mark_checkpoint_complete("trade", trade_key)
            pending_task_keys = checkpoint_task_keys(
                candle_tasks=state.candle_tasks,
                oi_tasks=state.oi_tasks,
                funding_tasks=state.funding_tasks,
                volatility_index_data_tasks=state.volatility_index_data_tasks,
                trade_tasks=state.trade_tasks,
                key_maps=key_maps,
            )
            success_task_keys = checkpoint_task_keys(
                candle_tasks=task_results,
                oi_tasks=oi_results,
                funding_tasks=funding_results,
                volatility_index_data_tasks=volatility_index_data_results,
                trade_tasks=trade_results,
                key_maps=key_maps,
            )
            fairness_rows = symbol_progress_rows_from_dataset_tasks(
                dataset_tasks=[task for task in plan.dataset_tasks if task.checkpoint_key() in pending_task_keys],
                success_keys=success_task_keys,
            )
            finalize_bronze_output(
                logger=logger,
                output=state.output,
                tasks=state.candle_tasks,
                oi_tasks=state.oi_tasks,
                funding_tasks=state.funding_tasks,
                volatility_index_data_tasks=state.volatility_index_data_tasks,
                trade_tasks=state.trade_tasks,
                task_results=task_results,
                task_errors=task_errors,
                oi_results=oi_results,
                oi_errors=oi_errors,
                funding_results=funding_results,
                funding_errors=funding_errors,
                volatility_index_data_results=volatility_index_data_results,
                volatility_index_data_errors=volatility_index_data_errors,
                trade_results=trade_results,
                trade_errors=trade_errors,
                multi_market=multi_market,
                oi_requested=oi_requested,
                funding_requested=funding_requested,
                volatility_index_data_requested=volatility_index_data_requested,
                perp_trades_requested=perp_trades_requested,
                option_trades_requested=option_trades_requested,
                candles_for_storage=state.candles_for_storage,
                open_interest_for_storage=state.open_interest_for_storage,
                funding_for_storage=state.funding_for_storage,
                volatility_index_data_for_storage=state.volatility_index_data_for_storage,
                trades_for_storage=state.trades_for_storage,
                ohlcv_markets=ohlcv_markets,
                args=cast(Any, args),
                incremental_parquet_on_fetch=incremental_parquet_on_fetch,
                incremental_parquet_files=incremental_persistor.incremental_parquet_files,
                oi_dataset_type=OI_DATASET_TYPE,
                sidecar_path_list_fn=_sidecar_path_list,
                ensure_bronze_sidecars_fn=ensure_bronze_sidecars,
                populate_ohlcv_output_fn=populate_ohlcv_output,
                populate_oi_output_fn=populate_oi_output,
                populate_funding_output_fn=populate_funding_output,
                populate_volatility_output_fn=populate_volatility_output,
                populate_trades_output_fn=populate_trades_output,
                symbol_progress_rows_fn=symbol_progress_rows,
                fairness_rows=fairness_rows,
                trade_error_breakdown_fn=trade_error_breakdown,
                candle_serializer=_serialize_candle,
                persist_fn=persist_loader_outputs_dto,
            )

            if not args.no_json_output:
                print(json.dumps(state.output, indent=2))
            if checkpoint_enabled and not (
                task_errors or oi_errors or funding_errors or volatility_index_data_errors or trade_errors
            ):
                checkpoint_path.unlink(missing_ok=True)
                logger.info("Cleared Bronze checkpoint '%s' after successful run", checkpoint_path)
            elif checkpoint_enabled:
                logger.warning(
                    "Retaining Bronze checkpoint '%s' for resume; failures remain",
                    checkpoint_path,
                )
            logger.info("Command complete: bronze-build")
    except SingleInstanceError as exc:
        logger.warning("Single-instance lock active")
        raise SystemExit(str(exc)) from exc
