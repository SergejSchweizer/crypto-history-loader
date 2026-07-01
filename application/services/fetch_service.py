"""Fetch orchestration services for OHLCV and open-interest datasets."""

from __future__ import annotations

import logging
import multiprocessing as _mp
from collections.abc import Callable
from datetime import UTC, date, datetime

from application.dto import (
    CandleFetchResultDTO,
    CandleFetchTaskDTO,
    FundingFetchResultDTO,
    FundingFetchTaskDTO,
    OpenInterestFetchResultDTO,
    OpenInterestFetchTaskDTO,
    TradeFetchResultDTO,
    TradeFetchTaskDTO,
    VolatilityFetchResultDTO,
    VolatilityFetchTaskDTO,
)
from application.services import fetch_executors as _fetch_executors
from application.services import fetch_funding_symbol as _funding_symbol
from application.services import fetch_history_rows as _history_rows
from application.services import fetch_ohlcv_symbol as _ohlcv_symbol
from application.services import fetch_open_interest_symbol as _oi_symbol
from application.services import fetch_range_planning as _range_planning
from application.services import fetch_task_execution as _task_execution
from application.services import fetch_trade_task_execution as _trade_task_execution
from application.services import fetch_trade_windows as _trade_windows
from application.services import fetch_volatility_symbol as _volatility_symbol
from application.services.fetch_executors import elapsed_seconds
from application.services.fetch_runtime_policy import heartbeat_seconds, task_timeout_seconds
from application.services.fetch_task_callbacks import bind_task_chunk_callback
from application.services.gapfill_service import _last_closed_open_ms, _missing_ranges_ms
from ingestion.funding import (
    FundingPoint,
    fetch_funding_all_history,
    fetch_funding_range,
    funding_interval_to_milliseconds,
    normalize_funding_timeframe,
)
from ingestion.lake_queries import (
    open_time_bounds_in_lake_by_dataset,
    open_times_in_lake,
    open_times_in_lake_by_dataset,
    partition_dates_in_lake_by_dataset,
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
    normalize_volatility_timeframe,
    volatility_interval_to_milliseconds,
)

logger = logging.getLogger(__name__)

mp = _mp
TRADE_BOUNDARY_TOLERANCE_MS = _range_planning.TRADE_BOUNDARY_TOLERANCE_MS
_day_end_ms = _range_planning.day_end_ms
_day_start_ms = _range_planning.day_start_ms
_missing_trade_day_ranges = _range_planning.missing_trade_day_ranges
_ranges_in_random_order = _range_planning.ranges_in_random_order
_split_range_into_utc_days = _range_planning.split_range_into_utc_days
_run_with_optional_timeout = _fetch_executors.run_with_optional_timeout
_timeout_worker = _fetch_executors.timeout_worker
_classify_trade_fetch_error = _trade_windows.classify_trade_fetch_error
_dedupe_sort_trade_rows = _trade_windows.dedupe_sort_trade_rows
_fetch_trade_window = _trade_windows.fetch_trade_window
_log_trade_window_progress = _trade_windows.log_trade_window_progress
_raise_if_all_trade_windows_failed = _trade_windows.raise_if_all_trade_windows_failed
_split_range_into_trade_windows = _trade_windows.split_range_into_trade_windows
_trade_window_ms = _trade_windows.trade_window_size_ms
_trade_windows_in_random_order = _trade_windows.trade_windows_in_random_order
_row_open_time_ms = _history_rows.row_open_time_ms
_filter_rows_by_start_bound = _history_rows.filter_rows_by_start_bound
_filter_chunk_callback = _history_rows.filter_chunk_callback
_bind_task_chunk_callback = bind_task_chunk_callback


def _task_timeout_seconds() -> float | None:
    """Return optional per-task timeout in seconds from environment."""

    return task_timeout_seconds()


def _heartbeat_seconds() -> float:
    """Return heartbeat interval in seconds for long-running fetch tasks."""

    return heartbeat_seconds()


def fetch_symbol_candles(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake,
    symbol_normalizer: Callable[..., str] = normalize_storage_symbol,
    interval_ms_resolver: Callable[..., int] = interval_to_milliseconds,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    history_fetcher: Callable[..., list[SpotCandle]] = fetch_candles_all_history,
    range_fetcher: Callable[..., list[SpotCandle]] = fetch_candles_range,
    latest_open_time_reader: Callable[..., datetime | None] | None = None,
    tail_delta_only: bool = False,
    on_history_chunk: Callable[[list[SpotCandle]], None] | None = None,
    start_open_ms_bound: int | None = None,
) -> list[SpotCandle]:
    """Fetch candles for one symbol with auto bootstrap/gap-fill behavior."""

    return _ohlcv_symbol.fetch_symbol_candles(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_reader,
        symbol_normalizer=symbol_normalizer,
        interval_ms_resolver=interval_ms_resolver,
        now_open_resolver=now_open_resolver,
        ranges_builder=ranges_builder,
        history_fetcher=history_fetcher,
        range_fetcher=range_fetcher,
        latest_open_time_reader=latest_open_time_reader,
        tail_delta_only=tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=start_open_ms_bound,
    )


def fetch_symbol_open_interest(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake_by_dataset,
    timeframe_normalizer: Callable[..., str] = normalize_open_interest_timeframe,
    symbol_normalizer: Callable[..., str] = normalize_storage_symbol,
    interval_ms_resolver: Callable[..., int] = open_interest_interval_to_milliseconds,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    history_fetcher: Callable[..., list[OpenInterestPoint]] = fetch_open_interest_all_history,
    range_fetcher: Callable[..., list[OpenInterestPoint]] = fetch_open_interest_range,
    latest_open_time_reader: Callable[..., datetime | None] | None = None,
    tail_delta_only: bool = False,
    on_history_chunk: Callable[[list[OpenInterestPoint]], None] | None = None,
    start_open_ms_bound: int | None = None,
) -> list[OpenInterestPoint]:
    """Fetch open-interest for one symbol with auto bootstrap/gap-fill behavior."""

    return _oi_symbol.fetch_symbol_open_interest(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_reader,
        timeframe_normalizer=timeframe_normalizer,
        symbol_normalizer=symbol_normalizer,
        interval_ms_resolver=interval_ms_resolver,
        now_open_resolver=now_open_resolver,
        ranges_builder=ranges_builder,
        history_fetcher=history_fetcher,
        range_fetcher=range_fetcher,
        latest_open_time_reader=latest_open_time_reader,
        tail_delta_only=tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=start_open_ms_bound,
    )


def fetch_symbol_funding(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake_by_dataset,
    timeframe_normalizer: Callable[..., str] = normalize_funding_timeframe,
    symbol_normalizer: Callable[..., str] = normalize_storage_symbol,
    interval_ms_resolver: Callable[..., int] = funding_interval_to_milliseconds,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    history_fetcher: Callable[..., list[FundingPoint]] = fetch_funding_all_history,
    range_fetcher: Callable[..., list[FundingPoint]] = fetch_funding_range,
    latest_open_time_reader: Callable[..., datetime | None] | None = None,
    tail_delta_only: bool = False,
    on_history_chunk: Callable[[list[FundingPoint]], None] | None = None,
    start_open_ms_bound: int | None = None,
) -> list[FundingPoint]:
    """Fetch funding for one symbol with auto bootstrap/gap-fill behavior."""

    return _funding_symbol.fetch_symbol_funding(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_reader,
        timeframe_normalizer=timeframe_normalizer,
        symbol_normalizer=symbol_normalizer,
        interval_ms_resolver=interval_ms_resolver,
        now_open_resolver=now_open_resolver,
        ranges_builder=ranges_builder,
        history_fetcher=history_fetcher,
        range_fetcher=range_fetcher,
        latest_open_time_reader=latest_open_time_reader,
        tail_delta_only=tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=start_open_ms_bound,
    )


def fetch_symbol_trades(
    exchange: Exchange,
    market: TradeMarket,
    symbol: str,
    lake_root: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake_by_dataset,
    partition_dates_reader: Callable[..., list[date]] = partition_dates_in_lake_by_dataset,
    partition_open_time_bounds_reader: Callable[..., dict[date, tuple[datetime, datetime]]] = (
        open_time_bounds_in_lake_by_dataset
    ),
    symbol_normalizer: Callable[..., str] = normalize_storage_symbol,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    history_fetcher: Callable[..., list[TradeTick | OptionTradeTick]] = fetch_trades_all_history,
    range_fetcher: Callable[..., list[TradeTick] | list[OptionTradeTick] | list[TradeTick | OptionTradeTick]] = (
        fetch_trades_range
    ),
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    latest_open_time_reader: Callable[..., datetime | None] | None = None,
    tail_delta_only: bool = False,
    on_history_chunk: Callable[[list[TradeTick | OptionTradeTick]], None] | None = None,
    start_open_ms_bound: int | None = None,
) -> list[TradeTick | OptionTradeTick]:
    """Fetch trades for one symbol with auto bootstrap/tail behavior."""

    storage_symbol = (
        symbol.upper().strip()
        if market == "option"
        else symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    )
    trades_dataset_type = "option_trades" if market == "option" else "perp_trades"
    end_open_ms = now_open_resolver(interval_ms=60_000)
    if start_open_ms_bound is not None and end_open_ms < start_open_ms_bound:
        return []

    if tail_delta_only:
        latest_reader = latest_open_time_reader
        if latest_reader is None:
            raise ValueError("latest_open_time_reader is required when tail_delta_only is enabled")
        latest_open_time = latest_reader(
            lake_root=lake_root,
            dataset_type=trades_dataset_type,
            market=market,
            exchange=exchange,
            symbol=storage_symbol,
            timeframe="tick",
        )
        if latest_open_time is None:
            rows = history_fetcher(
                exchange=exchange,
                symbol=symbol,
                market=market,
                on_history_chunk=_filter_chunk_callback(on_history_chunk, start_open_ms_bound),
            )
            filtered_rows = _filter_rows_by_start_bound(rows, start_open_ms_bound)
            return _dedupe_sort_trade_rows(filtered_rows)
        start_open_ms = int(latest_open_time.timestamp() * 1000) + 1
        if start_open_ms_bound is not None:
            start_open_ms = max(start_open_ms, start_open_ms_bound)
        if start_open_ms > end_open_ms:
            return []
        tail_rows: list[TradeTick | OptionTradeTick] = []
        failed_windows: list[str] = []
        windows = _trade_windows_in_random_order(
            start_open_ms,
            end_open_ms,
            market=market,
        )
        logger.debug(
            "Trade tail plan exchange=%s market=%s symbol=%s start_ms=%s end_ms=%s windows=%s",
            exchange,
            market,
            symbol,
            start_open_ms,
            end_open_ms,
            len(windows),
        )
        attempted_windows = 0
        phase_started_at = datetime.now(UTC)
        for window_start_ms, window_end_ms in windows:
            attempted_windows += 1
            window_rows, error = _fetch_trade_window(
                range_fetcher=range_fetcher,
                exchange=exchange,
                market=market,
                symbol=symbol,
                start_open_ms=window_start_ms,
                end_open_ms=window_end_ms,
            )
            if error is not None:
                failed_windows.append(f"{window_start_ms}-{window_end_ms}: {error}")
                _log_trade_window_progress(
                    phase="tail",
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    completed_windows=attempted_windows,
                    total_windows=len(windows),
                    rows=len(tail_rows),
                    failed_windows=len(failed_windows),
                    started_at=phase_started_at,
                )
                continue
            tail_rows.extend(window_rows)
            _log_trade_window_progress(
                phase="tail",
                exchange=exchange,
                market=market,
                symbol=symbol,
                completed_windows=attempted_windows,
                total_windows=len(windows),
                rows=len(tail_rows),
                failed_windows=len(failed_windows),
                started_at=phase_started_at,
            )
        _raise_if_all_trade_windows_failed(
            failed_windows=failed_windows,
            attempted_windows=attempted_windows,
            exchange=exchange,
            market=market,
            symbol=symbol,
        )
        if failed_windows:
            logger.warning(
                "Trade tail fetch completed with failed trade windows exchange=%s market=%s symbol=%s "
                "failed=%s attempted=%s",
                exchange,
                market,
                symbol,
                len(failed_windows),
                attempted_windows,
            )
        return _dedupe_sort_trade_rows(tail_rows)

    del open_times_reader
    scan_started_at = datetime.now(UTC)
    logger.debug(
        "Trade partition scan start dataset_type=%s exchange=%s market=%s symbol=%s timeframe=tick lake_root=%s",
        trades_dataset_type,
        exchange,
        market,
        storage_symbol,
        lake_root,
    )
    stored_partition_dates = partition_dates_reader(
        lake_root=lake_root,
        dataset_type=trades_dataset_type,
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe="tick",
    )
    stored_open_time_bounds = partition_open_time_bounds_reader(
        lake_root=lake_root,
        dataset_type=trades_dataset_type,
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe="tick",
    )
    logger.debug(
        (
            "Trade partition scan done dataset_type=%s exchange=%s market=%s symbol=%s "
            "partitions=%s bounded_partitions=%s elapsed_s=%s"
        ),
        trades_dataset_type,
        exchange,
        market,
        storage_symbol,
        len(stored_partition_dates),
        len(stored_open_time_bounds),
        elapsed_seconds(scan_started_at),
    )
    if not stored_partition_dates:
        if start_open_ms_bound is not None:
            bootstrap_rows: list[TradeTick | OptionTradeTick] = []
            bootstrap_failed_windows: list[str] = []
            windows = _trade_windows_in_random_order(
                start_open_ms_bound,
                end_open_ms,
                market=market,
            )
            logger.debug(
                "Trade bootstrap plan exchange=%s market=%s symbol=%s start_ms=%s end_ms=%s windows=%s",
                exchange,
                market,
                symbol,
                start_open_ms_bound,
                end_open_ms,
                len(windows),
            )
            attempted_windows = 0
            phase_started_at = datetime.now(UTC)
            for window_start_ms, window_end_ms in windows:
                attempted_windows += 1
                window_rows, error = _fetch_trade_window(
                    range_fetcher=range_fetcher,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    start_open_ms=window_start_ms,
                    end_open_ms=window_end_ms,
                )
                if error is not None:
                    bootstrap_failed_windows.append(f"{window_start_ms}-{window_end_ms}: {error}")
                    _log_trade_window_progress(
                        phase="bootstrap",
                        exchange=exchange,
                        market=market,
                        symbol=symbol,
                        completed_windows=attempted_windows,
                        total_windows=len(windows),
                        rows=len(bootstrap_rows),
                        failed_windows=len(bootstrap_failed_windows),
                        started_at=phase_started_at,
                    )
                    continue
                if window_rows and on_history_chunk is not None:
                    on_history_chunk(window_rows)
                bootstrap_rows.extend(window_rows)
                _log_trade_window_progress(
                    phase="bootstrap",
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    completed_windows=attempted_windows,
                    total_windows=len(windows),
                    rows=len(bootstrap_rows),
                    failed_windows=len(bootstrap_failed_windows),
                    started_at=phase_started_at,
                )
            _raise_if_all_trade_windows_failed(
                failed_windows=bootstrap_failed_windows,
                attempted_windows=attempted_windows,
                exchange=exchange,
                market=market,
                symbol=symbol,
            )
            if bootstrap_failed_windows:
                logger.warning(
                    "Trade bootstrap completed with failed trade windows exchange=%s market=%s symbol=%s "
                    "failed=%s attempted=%s",
                    exchange,
                    market,
                    symbol,
                    len(bootstrap_failed_windows),
                    attempted_windows,
                )
            return _dedupe_sort_trade_rows(bootstrap_rows)
        rows = history_fetcher(
            exchange=exchange,
            symbol=symbol,
            market=market,
            on_history_chunk=_filter_chunk_callback(on_history_chunk, start_open_ms_bound),
        )
        filtered_rows = _filter_rows_by_start_bound(rows, start_open_ms_bound)
        return _dedupe_sort_trade_rows(filtered_rows)

    earliest_existing_ms = _day_start_ms(min(stored_partition_dates))
    if end_open_ms < earliest_existing_ms:
        return []

    del ranges_builder
    missing_range_start_ms = start_open_ms_bound if start_open_ms_bound is not None else earliest_existing_ms
    missing_ranges = _missing_trade_day_ranges(
        existing_dates=stored_partition_dates,
        coverage_bounds=stored_open_time_bounds or None,
        start_open_ms=missing_range_start_ms,
        end_open_ms=end_open_ms,
    )
    logger.debug(
        (
            "Trade gap plan ranges exchange=%s market=%s symbol=%s stored_partitions=%s "
            "ranges=%s start_bound_ms=%s end_ms=%s"
        ),
        exchange,
        market,
        symbol,
        len(stored_partition_dates),
        len(missing_ranges),
        start_open_ms_bound,
        end_open_ms,
    )
    if not missing_ranges:
        return []

    gap_rows: list[TradeTick | OptionTradeTick] = []
    gap_failed_windows: list[str] = []
    window_plan: list[tuple[int, int]] = []
    for start_open_ms, gap_end_ms in _ranges_in_random_order(missing_ranges):
        window_plan.extend(
            _trade_windows_in_random_order(
                start_open_ms,
                gap_end_ms,
                market=market,
            )
        )
    logger.debug(
        "Trade gap window plan exchange=%s market=%s symbol=%s ranges=%s windows=%s",
        exchange,
        market,
        symbol,
        len(missing_ranges),
        len(window_plan),
    )
    attempted_windows = 0
    phase_started_at = datetime.now(UTC)
    for window_start_ms, window_end_ms in window_plan:
        attempted_windows += 1
        window_rows, error = _fetch_trade_window(
            range_fetcher=range_fetcher,
            exchange=exchange,
            market=market,
            symbol=symbol,
            start_open_ms=window_start_ms,
            end_open_ms=window_end_ms,
        )
        if error is not None:
            gap_failed_windows.append(f"{window_start_ms}-{window_end_ms}: {error}")
            _log_trade_window_progress(
                phase="gap",
                exchange=exchange,
                market=market,
                symbol=symbol,
                completed_windows=attempted_windows,
                total_windows=len(window_plan),
                rows=len(gap_rows),
                failed_windows=len(gap_failed_windows),
                started_at=phase_started_at,
            )
            continue
        if window_rows and on_history_chunk is not None:
            on_history_chunk(window_rows)
        gap_rows.extend(window_rows)
        _log_trade_window_progress(
            phase="gap",
            exchange=exchange,
            market=market,
            symbol=symbol,
            completed_windows=attempted_windows,
            total_windows=len(window_plan),
            rows=len(gap_rows),
            failed_windows=len(gap_failed_windows),
            started_at=phase_started_at,
        )
    _raise_if_all_trade_windows_failed(
        failed_windows=gap_failed_windows,
        attempted_windows=attempted_windows,
        exchange=exchange,
        market=market,
        symbol=symbol,
    )
    if gap_failed_windows:
        logger.warning(
            "Trade gap fill completed with failed trade windows exchange=%s market=%s symbol=%s failed=%s attempted=%s",
            exchange,
            market,
            symbol,
            len(gap_failed_windows),
            attempted_windows,
        )
    filtered = _filter_rows_by_start_bound(gap_rows, start_open_ms_bound)
    return _dedupe_sort_trade_rows(filtered)


def fetch_symbol_volatility(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    *,
    dataset_type: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake_by_dataset,
    timeframe_normalizer: Callable[..., str] = normalize_volatility_timeframe,
    interval_ms_resolver: Callable[..., int] = volatility_interval_to_milliseconds,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    history_fetcher: Callable[..., list[VolatilityPoint]],
    range_fetcher: Callable[..., list[VolatilityPoint]],
    latest_open_time_reader: Callable[..., datetime | None] | None = None,
    tail_delta_only: bool = False,
    on_history_chunk: Callable[[list[VolatilityPoint]], None] | None = None,
    start_open_ms_bound: int | None = None,
) -> list[VolatilityPoint]:
    """Fetch one volatility dataset for one symbol with bootstrap/gap-fill behavior."""

    return _volatility_symbol.fetch_symbol_volatility(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        dataset_type=dataset_type,
        open_times_reader=open_times_reader,
        timeframe_normalizer=timeframe_normalizer,
        interval_ms_resolver=interval_ms_resolver,
        now_open_resolver=now_open_resolver,
        ranges_builder=ranges_builder,
        history_fetcher=history_fetcher,
        range_fetcher=range_fetcher,
        latest_open_time_reader=latest_open_time_reader,
        tail_delta_only=tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=start_open_ms_bound,
    )


def fetch_candle_tasks_parallel(
    tasks: list[CandleFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[SpotCandle]] = fetch_symbol_candles,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_task_chunk: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
) -> CandleFetchResultDTO:
    """Fetch OHLCV tasks sequentially."""

    del concurrency, shared_semaphore
    return _task_execution.fetch_candle_tasks_sequential(
        tasks=tasks,
        lake_root=lake_root,
        logger=logger,
        symbol_fetcher=symbol_fetcher,
        timeout_s=_task_timeout_seconds(),
        heartbeat_s=_heartbeat_seconds(),
        runner=_run_with_optional_timeout,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )


def fetch_open_interest_tasks_parallel(
    tasks: list[OpenInterestFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[OpenInterestPoint]] = fetch_symbol_open_interest,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_task_chunk: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
) -> OpenInterestFetchResultDTO:
    """Fetch open-interest tasks sequentially."""

    del concurrency, shared_semaphore
    return _task_execution.fetch_open_interest_tasks_sequential(
        tasks=tasks,
        lake_root=lake_root,
        logger=logger,
        symbol_fetcher=symbol_fetcher,
        timeout_s=_task_timeout_seconds(),
        heartbeat_s=_heartbeat_seconds(),
        runner=_run_with_optional_timeout,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )


def fetch_funding_tasks_parallel(
    tasks: list[FundingFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[FundingPoint]] = fetch_symbol_funding,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_task_chunk: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
) -> FundingFetchResultDTO:
    """Fetch funding tasks sequentially."""

    del concurrency, shared_semaphore
    return _task_execution.fetch_funding_tasks_sequential(
        tasks=tasks,
        lake_root=lake_root,
        logger=logger,
        symbol_fetcher=symbol_fetcher,
        timeout_s=_task_timeout_seconds(),
        heartbeat_s=_heartbeat_seconds(),
        runner=_run_with_optional_timeout,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )


def fetch_volatility_tasks_parallel(
    tasks: list[VolatilityFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[VolatilityPoint]],
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[VolatilityFetchTaskDTO, list[VolatilityPoint]], None] | None = None,
    on_task_chunk: Callable[[VolatilityFetchTaskDTO, list[VolatilityPoint]], None] | None = None,
) -> VolatilityFetchResultDTO:
    """Fetch volatility tasks sequentially."""

    del concurrency, shared_semaphore
    return _task_execution.fetch_volatility_tasks_sequential(
        tasks=tasks,
        lake_root=lake_root,
        logger=logger,
        symbol_fetcher=symbol_fetcher,
        timeout_s=_task_timeout_seconds(),
        heartbeat_s=_heartbeat_seconds(),
        runner=_run_with_optional_timeout,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )


def fetch_trade_tasks_parallel(
    tasks: list[TradeFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[TradeTick | OptionTradeTick]] = fetch_symbol_trades,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
    on_task_chunk: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
) -> TradeFetchResultDTO:
    """Fetch trade tasks with bounded symbol-level concurrency."""

    del shared_semaphore
    return _trade_task_execution.fetch_trade_tasks_bounded(
        tasks=tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=symbol_fetcher,
        timeout_s=_task_timeout_seconds(),
        heartbeat_s=_heartbeat_seconds(),
        runner=_run_with_optional_timeout,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )
