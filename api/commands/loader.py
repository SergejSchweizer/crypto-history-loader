"""Bronze build command implementation."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from api.commands.loader_bounds import configure_bronze_start_bounds
from api.commands.loader_checkpoint import apply_checkpoint_filter, has_checkpoint_state
from api.commands.loader_dataset_handlers import (
    populate_funding_output,
    populate_ohlcv_output,
    populate_oi_output,
    populate_trades_output,
    populate_volatility_output,
)
from api.commands.loader_execution import fetch_all_task_groups as fetch_all_task_groups_execution
from api.commands.loader_output import IncrementalPersistor, finalize_bronze_output
from api.commands.loader_planning import (
    build_bronze_fetch_plan,
    canonical_symbol_key,
    parse_exchange_symbol_start_dates,
    parse_start_date_to_open_ms,
    parse_symbol_start_dates,
    resolved_symbol_groups,
    sanitize_symbols,
)
from api.commands.loader_runtime import BronzeRuntimeBoundsContext, resolve_symbol_start_open_ms_bound
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
    bronze_checkpoint_fingerprint,
    bronze_checkpoint_path,
    build_bronze_execution_policy,
    load_bronze_checkpoint,
    task_key_tuple_to_string,
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
from application.services.runtime_service import SingleInstanceError, SingleInstanceLock, fetch_concurrency
from application.services.storage_service import persist_loader_outputs_dto
from ingestion.funding import (
    FundingPoint,
    fetch_funding_all_history,
    fetch_funding_range,
    funding_interval_to_milliseconds,
    normalize_funding_timeframe,
)
from ingestion.lake import (
    ensure_bronze_sidecars,
    latest_open_time_in_lake,
    latest_open_time_in_lake_by_dataset,
    open_times_in_lake,
    open_times_in_lake_by_dataset,
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

tail_delta_only = True
bronze_start_open_ms: int | None = None
bronze_symbol_start_open_ms: dict[str, int] = {}
bronze_exchange_symbol_start_open_ms: dict[str, int] = {}
_TAIL_DELTA_ONLY = True
_BRONZE_START_OPEN_MS: int | None = None
_BRONZE_SYMBOL_START_OPEN_MS: dict[str, int] = {}
_BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS: dict[str, int] = {}
MARKET_CHOICES = tuple(DATASET_REGISTRY.keys())
OI_DATASET_TYPE = dataset_spec("oi").dataset_type


runtime_bounds_context = BronzeRuntimeBoundsContext(
    tail_delta_only=True,
    global_start_open_ms=None,
    symbol_start_open_ms={},
    exchange_symbol_start_open_ms={},
)
_last_closed_open_ms = last_closed_open_ms
_missing_ranges_ms = missing_ranges_ms


def _current_runtime_bounds_context() -> BronzeRuntimeBoundsContext:
    """Return effective runtime bounds context with legacy global fallback."""

    return BronzeRuntimeBoundsContext(
        tail_delta_only=_TAIL_DELTA_ONLY,
        global_start_open_ms=_BRONZE_START_OPEN_MS,
        symbol_start_open_ms=_BRONZE_SYMBOL_START_OPEN_MS,
        exchange_symbol_start_open_ms=_BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS,
    )


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


def _dataset_task_key_maps(
    plan: BronzeFetchPlanDTO,
) -> tuple[
    dict[tuple[Exchange, Market, str, str], str],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, TradeMarket, str], str],
]:
    """Return tuple->checkpoint-key mappings derived from registry dataset tasks."""

    candle_map: dict[tuple[Exchange, Market, str, str], str] = {}
    oi_map: dict[tuple[Exchange, str, str], str] = {}
    funding_map: dict[tuple[Exchange, str, str], str] = {}
    volatility_index_data_map: dict[tuple[Exchange, str, str], str] = {}
    trade_map: dict[tuple[Exchange, TradeMarket, str], str] = {}
    for task in plan.dataset_tasks:
        key = task.checkpoint_key()
        if task.dataset_type in {"spot", "perp"}:
            candle_map[task.candle_tuple()] = key
        elif task.dataset_type == "oi":
            oi_map[task.interval_tuple()] = key
        elif task.dataset_type == "funding":
            funding_map[task.interval_tuple()] = key
        elif task.dataset_type == "volatility_index_data":
            volatility_index_data_map[task.interval_tuple()] = key
        elif task.dataset_type in {"perp_trades", "option_trades"}:
            trade_map[task.trade_tuple()] = key
    return candle_map, oi_map, funding_map, volatility_index_data_map, trade_map


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
    volatility_index_data_key_map: dict[tuple[Exchange, str, str], str],
    trade_key_map: dict[tuple[Exchange, TradeMarket, str], str],
) -> None:
    """Augment completed checkpoint keys with registry aliases for backward compatibility."""

    for candle_task in candle_tasks:
        legacy = _task_key_tuple_to_string((candle_task[0], candle_task[1], candle_task[2], candle_task[3]))
        if legacy in completed["candle"]:
            completed["candle"].add(candle_key_map.get(candle_task, legacy))
    for oi_task in oi_tasks:
        legacy = _task_key_tuple_to_string((oi_task[0], oi_task[1], oi_task[2]))
        if legacy in completed["oi"]:
            completed["oi"].add(oi_key_map.get(oi_task, legacy))
    for funding_task in funding_tasks:
        legacy = _task_key_tuple_to_string((funding_task[0], funding_task[1], funding_task[2]))
        if legacy in completed["funding"]:
            completed["funding"].add(funding_key_map.get(funding_task, legacy))
    for volatility_task in volatility_index_data_tasks:
        legacy = _task_key_tuple_to_string((volatility_task[0], volatility_task[1], volatility_task[2]))
        if legacy in completed["volatility_index_data"]:
            completed["volatility_index_data"].add(volatility_index_data_key_map.get(volatility_task, legacy))
    for trade_task in trade_tasks:
        legacy = _task_key_tuple_to_string((trade_task[0], trade_task[1], trade_task[2]))
        if legacy in completed["trade"]:
            completed["trade"].add(trade_key_map.get(trade_task, legacy))


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


def _sidecar_path_list(parquet_files: list[str], suffix: str) -> list[str]:
    """Build sorted unique sidecar paths for provided parquet files."""

    return sorted({str(Path(path).with_suffix(suffix).resolve()) for path in parquet_files})


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
        "--market",
        nargs="+",
        choices=MARKET_CHOICES,
        default=["spot"],
        help="One or more data types to fetch, e.g. --market spot perp oi funding",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT", "ETHUSDT"],
        help="Symbols or instrument aliases (exchange specific)",
    )
    parser.add_argument(
        "--perp-trade-symbols",
        nargs="+",
        default=None,
        help="Symbols for perp_trades ingestion (defaults to --symbols when omitted).",
    )
    parser.add_argument(
        "--option-trade-symbols",
        nargs="+",
        default=None,
        help="Symbols for option_trades ingestion (defaults to --symbols when omitted).",
    )
    parser.set_defaults(tail_delta_only=True)
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
        help="Fetch only new tail data after latest stored point (overrides config).",
    )
    parser.add_argument(
        "--full-gap-fill",
        dest="tail_delta_only",
        action="store_false",
        help="Run full historical internal gap checks instead of default tail-only delta mode.",
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


def _serialize_candle(candle: SpotCandle) -> dict[str, object]:
    data = asdict(candle)
    for key in ("open_time", "close_time"):
        value = data[key]
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


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
        tail_delta_only=tail_delta_only,
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
        tail_delta_only=tail_delta_only,
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
        tail_delta_only=tail_delta_only,
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
        tail_delta_only=tail_delta_only,
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
        tail_delta_only=tail_delta_only,
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


def _configure_bronze_start_bounds(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Initialize Bronze start-bound globals from CLI/config args and emit boundary logs."""

    global bronze_start_open_ms, bronze_symbol_start_open_ms, bronze_exchange_symbol_start_open_ms
    global _BRONZE_START_OPEN_MS, _BRONZE_SYMBOL_START_OPEN_MS, _BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS
    global runtime_bounds_context
    (
        bronze_start_open_ms,
        bronze_symbol_start_open_ms,
        bronze_exchange_symbol_start_open_ms,
    ) = configure_bronze_start_bounds(
        args=args,
        logger=logger,
    )
    runtime_bounds_context = BronzeRuntimeBoundsContext(
        tail_delta_only=bool(getattr(args, "tail_delta_only", _TAIL_DELTA_ONLY)),
        global_start_open_ms=bronze_start_open_ms,
        symbol_start_open_ms=bronze_symbol_start_open_ms,
        exchange_symbol_start_open_ms=bronze_exchange_symbol_start_open_ms,
    )
    _BRONZE_START_OPEN_MS = bronze_start_open_ms  # pyright: ignore[reportConstantRedefinition]
    _BRONZE_SYMBOL_START_OPEN_MS = bronze_symbol_start_open_ms  # pyright: ignore[reportConstantRedefinition]
    _BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS = bronze_exchange_symbol_start_open_ms  # pyright: ignore[reportConstantRedefinition]


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
            volatility_index_data_tasks=cast(list[tuple[str, str, str]], volatility_index_data_tasks),
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
            fetch_volatility_index_data_fn=cast(Callable[..., object], _fetch_volatility_index_data_tasks_parallel),
            fetch_trades_fn=cast(Callable[..., object], _fetch_trade_tasks_parallel),
            on_candle_task_complete=cast(Callable[[object, list[object]], None] | None, on_candle_task_complete),
            on_oi_task_complete=cast(Callable[[object, list[object]], None] | None, on_oi_task_complete),
            on_funding_task_complete=cast(Callable[[object, list[object]], None] | None, on_funding_task_complete),
            on_volatility_index_data_task_complete=cast(
                Callable[[object, list[object]], None] | None,
                on_volatility_index_data_task_complete,
            ),
            on_trade_task_complete=cast(Callable[[object, list[object]], None] | None, on_trade_task_complete),
            on_candle_task_chunk=cast(Callable[[object, list[object]], None] | None, on_candle_task_chunk),
            on_oi_task_chunk=cast(Callable[[object, list[object]], None] | None, on_oi_task_chunk),
            on_funding_task_chunk=cast(Callable[[object, list[object]], None] | None, on_funding_task_chunk),
            on_volatility_index_data_task_chunk=cast(
                Callable[[object, list[object]], None] | None,
                on_volatility_index_data_task_chunk,
            ),
            on_trade_task_chunk=cast(Callable[[object, list[object]], None] | None, on_trade_task_chunk),
        ),
    )


def run_bronze_build(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run bronze-build command."""

    global tail_delta_only, _TAIL_DELTA_ONLY
    tail_delta_only = bool(args.tail_delta_only)
    _TAIL_DELTA_ONLY = tail_delta_only  # pyright: ignore[reportConstantRedefinition]
    _configure_bronze_start_bounds(args=args, logger=logger)
    if tail_delta_only:
        rolling_bound = datetime.now(UTC) - timedelta(days=30)
        logger.info(
            "Bronze default tail-mode cap enabled max_missing_window_days=30 rolling_start_utc=%s",
            rolling_bound.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

    try:
        with SingleInstanceLock(".run/crypto-history-loader.lock"):
            plan = _build_bronze_fetch_plan(args=args, logger=logger)
            exchanges = plan.exchanges
            ohlcv_markets = plan.ohlcv_markets
            data_types = plan.data_types
            oi_requested = "oi" in data_types
            funding_requested = "funding" in data_types
            volatility_index_data_requested = "volatility_index_data" in data_types
            perp_trades_requested = "perp_trades" in data_types
            option_trades_requested = "option_trades" in data_types
            multi_market = len(data_types) > 1
            output: dict[str, object] = {}
            candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = {}
            open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = {}
            funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = {}
            volatility_index_data_for_storage: dict[Market, dict[str, dict[str, list[VolatilityPoint]]]] = {}
            trades_for_storage: dict[TradeMarket, dict[str, dict[str, list[TradeTick | OptionTradeTick]]]] = {}
            tasks: list[tuple[Exchange, Market, str, str]] = []
            oi_tasks: list[tuple[Exchange, str, str]] = []
            funding_tasks: list[tuple[Exchange, str, str]] = []
            volatility_index_data_tasks: list[tuple[Exchange, str, str]] = []
            trade_tasks: list[tuple[Exchange, TradeMarket, str]] = []
            logger.info(
                "Deterministic schedule markets=%s symbols=%s perp_trade_symbols=%s option_trade_symbols=%s",
                data_types,
                plan.symbols,
                plan.perp_trade_symbols,
                plan.option_trade_symbols,
            )
            for exchange in exchanges:
                exchange_output: dict[str, object] = {}
                output[exchange] = exchange_output
            tasks.extend(plan.candle_tasks)
            oi_tasks.extend(plan.oi_tasks)
            funding_tasks.extend(plan.funding_tasks)
            volatility_index_data_tasks.extend(plan.volatility_index_data_tasks)
            trade_tasks.extend(plan.trade_tasks)
            (
                candle_key_map,
                oi_key_map,
                funding_key_map,
                volatility_index_data_key_map,
                trade_key_map,
            ) = _dataset_task_key_maps(plan)
            checkpoint_enabled = (
                bool(args.save_parquet_lake) if bool(getattr(args, "_invoked_via_cli", False)) else True
            )
            checkpoint_path = _bronze_checkpoint_path()
            checkpoint_fingerprint = _bronze_checkpoint_fingerprint(args=args, plan=plan)
            checkpoint_completed: dict[str, set[str]] = {
                "candle": set(),
                "oi": set(),
                "funding": set(),
                "volatility_index_data": set(),
                "trade": set(),
            }
            if checkpoint_enabled:
                checkpoint_completed = _load_bronze_checkpoint(
                    path=checkpoint_path,
                    fingerprint=checkpoint_fingerprint,
                    logger=logger,
                )
                _hydrate_checkpoint_aliases(
                    completed=checkpoint_completed,
                    candle_tasks=tasks,
                    oi_tasks=oi_tasks,
                    funding_tasks=funding_tasks,
                    volatility_index_data_tasks=volatility_index_data_tasks,
                    trade_tasks=trade_tasks,
                    candle_key_map=candle_key_map,
                    oi_key_map=oi_key_map,
                    funding_key_map=funding_key_map,
                    volatility_index_data_key_map=volatility_index_data_key_map,
                    trade_key_map=trade_key_map,
                )

                pending_tasks = apply_checkpoint_filter(
                    candle_tasks=tasks,
                    oi_tasks=oi_tasks,
                    funding_tasks=funding_tasks,
                    volatility_index_data_tasks=volatility_index_data_tasks,
                    trade_tasks=trade_tasks,
                    completed=checkpoint_completed,
                    candle_key_serializer=lambda task: candle_key_map.get(
                        task,
                        _task_key_tuple_to_string((task[0], task[1], task[2], task[3])),
                    ),
                    oi_key_serializer=lambda task: oi_key_map.get(
                        task,
                        _task_key_tuple_to_string((task[0], task[1], task[2])),
                    ),
                    funding_key_serializer=lambda task: funding_key_map.get(
                        task,
                        _task_key_tuple_to_string((task[0], task[1], task[2])),
                    ),
                    volatility_index_data_key_serializer=lambda task: volatility_index_data_key_map.get(
                        task,
                        _task_key_tuple_to_string((task[0], task[1], task[2])),
                    ),
                    trade_key_serializer=lambda task: trade_key_map.get(
                        task,
                        _task_key_tuple_to_string((task[0], task[1], task[2])),
                    ),
                )
                tasks = pending_tasks.candle_tasks
                oi_tasks = pending_tasks.oi_tasks
                funding_tasks = pending_tasks.funding_tasks
                volatility_index_data_tasks = pending_tasks.volatility_index_data_tasks
                trade_tasks = pending_tasks.trade_tasks
                if has_checkpoint_state(checkpoint_completed):
                    logger.info(
                        (
                            "Resuming from Bronze checkpoint '%s' pending_tasks candle=%s oi=%s funding=%s "
                            "volatility_index_data=%s trade=%s"
                        ),
                        checkpoint_path,
                        len(tasks),
                        len(oi_tasks),
                        len(funding_tasks),
                        len(volatility_index_data_tasks),
                        len(trade_tasks),
                    )
                _write_bronze_checkpoint(
                    checkpoint_path,
                    fingerprint=checkpoint_fingerprint,
                    completed=checkpoint_completed,
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
                    "Fetch mode enabled for spot/perp, oi, funding, volatility, and perp_trades with "
                    "concurrency=%s (configured=%s; parallelization disabled)"
                ),
                policy.effective_concurrency,
                policy.configured_concurrency,
            )
            if incremental_parquet_on_fetch:
                logger.info("Incremental parquet flush enabled during fetch execution")

            def _mark_checkpoint_complete(dataset: str, key: tuple[object, ...]) -> None:
                if dataset == "candle":
                    serialized_key = candle_key_map.get(
                        cast(tuple[Exchange, Market, str, str], key),
                        _task_key_tuple_to_string(key),
                    )
                elif dataset == "oi":
                    serialized_key = oi_key_map.get(
                        cast(tuple[Exchange, str, str], key),
                        _task_key_tuple_to_string(key),
                    )
                elif dataset == "funding":
                    serialized_key = funding_key_map.get(
                        cast(tuple[Exchange, str, str], key),
                        _task_key_tuple_to_string(key),
                    )
                elif dataset == "volatility_index_data":
                    serialized_key = volatility_index_data_key_map.get(
                        cast(tuple[Exchange, str, str], key),
                        _task_key_tuple_to_string(key),
                    )
                elif dataset == "trade":
                    serialized_key = trade_key_map.get(
                        cast(tuple[Exchange, TradeMarket, str], key),
                        _task_key_tuple_to_string(key),
                    )
                else:
                    serialized_key = _task_key_tuple_to_string(key)
                checkpoint_completed[dataset].add(serialized_key)
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
            ) = _fetch_all_task_groups(
                candle_tasks=tasks,
                oi_tasks=oi_tasks,
                funding_tasks=funding_tasks,
                volatility_index_data_tasks=volatility_index_data_tasks,
                trade_tasks=trade_tasks,
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
                on_oi_task_complete=(lambda task, rows: incremental_persistor.on_oi_task_complete(task, rows, logger))
                if incremental_parquet_on_fetch
                else None,
                on_funding_task_complete=(
                    lambda task, rows: incremental_persistor.on_funding_task_complete(task, rows, logger)
                )
                if incremental_parquet_on_fetch
                else None,
                on_volatility_index_data_task_complete=(
                    lambda task, rows: incremental_persistor.on_volatility_index_data_task_complete(task, rows, logger)
                )
                if incremental_parquet_on_fetch
                else None,
                on_candle_task_chunk=(lambda task, rows: incremental_persistor.on_candle_task_chunk(task, rows, logger))
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
                on_trade_task_chunk=(lambda task, rows: incremental_persistor.on_trade_task_chunk(task, rows, logger))
                if incremental_parquet_on_fetch
                else None,
            )
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
            pending_task_keys: set[str] = set()
            for candle_task in tasks:
                pending_task_keys.add(candle_key_map.get(candle_task, _task_key_tuple_to_string(candle_task)))
            for oi_task in oi_tasks:
                pending_task_keys.add(oi_key_map.get(oi_task, _task_key_tuple_to_string(oi_task)))
            for funding_task in funding_tasks:
                pending_task_keys.add(funding_key_map.get(funding_task, _task_key_tuple_to_string(funding_task)))
            for volatility_task in volatility_index_data_tasks:
                pending_task_keys.add(
                    volatility_index_data_key_map.get(volatility_task, _task_key_tuple_to_string(volatility_task))
                )
            for trade_task in trade_tasks:
                pending_task_keys.add(trade_key_map.get(trade_task, _task_key_tuple_to_string(trade_task)))
            success_task_keys: set[str] = set()
            for candle_key in task_results:
                success_task_keys.add(candle_key_map.get(candle_key, _task_key_tuple_to_string(candle_key)))
            for oi_key in oi_results:
                success_task_keys.add(oi_key_map.get(oi_key, _task_key_tuple_to_string(oi_key)))
            for funding_key in funding_results:
                success_task_keys.add(funding_key_map.get(funding_key, _task_key_tuple_to_string(funding_key)))
            for volatility_key in volatility_index_data_results:
                success_task_keys.add(
                    volatility_index_data_key_map.get(volatility_key, _task_key_tuple_to_string(volatility_key))
                )
            for trade_key in trade_results:
                success_task_keys.add(trade_key_map.get(trade_key, _task_key_tuple_to_string(trade_key)))
            fairness_rows = symbol_progress_rows_from_dataset_tasks(
                dataset_tasks=[task for task in plan.dataset_tasks if task.checkpoint_key() in pending_task_keys],
                success_keys=success_task_keys,
            )
            finalize_bronze_output(
                logger=logger,
                output=output,
                tasks=tasks,
                oi_tasks=oi_tasks,
                funding_tasks=funding_tasks,
                volatility_index_data_tasks=volatility_index_data_tasks,
                trade_tasks=trade_tasks,
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
                candles_for_storage=candles_for_storage,
                open_interest_for_storage=open_interest_for_storage,
                funding_for_storage=funding_for_storage,
                volatility_index_data_for_storage=volatility_index_data_for_storage,
                trades_for_storage=trades_for_storage,
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
                print(json.dumps(output, indent=2))
            if checkpoint_enabled and not (
                task_errors
                or oi_errors
                or funding_errors
                or volatility_index_data_errors
                or trade_errors
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
