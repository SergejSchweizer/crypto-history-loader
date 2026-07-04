"""Trade symbol-level Bronze fetch planning."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, date, datetime

from application.services.fetch_executors import elapsed_seconds
from application.services.fetch_history_rows import filter_chunk_callback, filter_rows_by_start_bound
from application.services.fetch_range_planning import day_start_ms, missing_trade_day_ranges, ranges_in_random_order
from application.services.fetch_trade_windows import (
    dedupe_sort_trade_rows,
    fetch_trade_window,
    log_trade_window_progress,
    raise_if_all_trade_windows_failed,
    trade_windows_in_random_order,
)
from application.services.gapfill_service import _last_closed_open_ms, _missing_ranges_ms
from ingestion.lake_queries import (
    open_time_bounds_in_lake_by_dataset,
    open_times_in_lake_by_dataset,
    partition_dates_in_lake_by_dataset,
)
from ingestion.spot_ohlcv import Exchange, normalize_storage_symbol
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick, fetch_trades_all_history, fetch_trades_range

logger = logging.getLogger(__name__)


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
    trades_dataset_type = "options_trades" if market == "option" else "perps_trades"
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
                on_history_chunk=filter_chunk_callback(on_history_chunk, start_open_ms_bound),
            )
            filtered_rows = filter_rows_by_start_bound(rows, start_open_ms_bound)
            return dedupe_sort_trade_rows(filtered_rows)
        start_open_ms = int(latest_open_time.timestamp() * 1000) + 1
        if start_open_ms_bound is not None:
            start_open_ms = max(start_open_ms, start_open_ms_bound)
        if start_open_ms > end_open_ms:
            return []
        tail_rows: list[TradeTick | OptionTradeTick] = []
        failed_windows: list[str] = []
        windows = trade_windows_in_random_order(
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
            window_rows, error = fetch_trade_window(
                range_fetcher=range_fetcher,
                exchange=exchange,
                market=market,
                symbol=symbol,
                start_open_ms=window_start_ms,
                end_open_ms=window_end_ms,
            )
            if error is not None:
                failed_windows.append(f"{window_start_ms}-{window_end_ms}: {error}")
                log_trade_window_progress(
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
            log_trade_window_progress(
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
        raise_if_all_trade_windows_failed(
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
        return dedupe_sort_trade_rows(tail_rows)

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
            windows = trade_windows_in_random_order(
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
                window_rows, error = fetch_trade_window(
                    range_fetcher=range_fetcher,
                    exchange=exchange,
                    market=market,
                    symbol=symbol,
                    start_open_ms=window_start_ms,
                    end_open_ms=window_end_ms,
                )
                if error is not None:
                    bootstrap_failed_windows.append(f"{window_start_ms}-{window_end_ms}: {error}")
                    log_trade_window_progress(
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
                log_trade_window_progress(
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
            raise_if_all_trade_windows_failed(
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
            return dedupe_sort_trade_rows(bootstrap_rows)
        rows = history_fetcher(
            exchange=exchange,
            symbol=symbol,
            market=market,
            on_history_chunk=filter_chunk_callback(on_history_chunk, start_open_ms_bound),
        )
        filtered_rows = filter_rows_by_start_bound(rows, start_open_ms_bound)
        return dedupe_sort_trade_rows(filtered_rows)

    earliest_existing_ms = day_start_ms(min(stored_partition_dates))
    if end_open_ms < earliest_existing_ms:
        return []

    del ranges_builder
    missing_range_start_ms = start_open_ms_bound if start_open_ms_bound is not None else earliest_existing_ms
    missing_ranges = missing_trade_day_ranges(
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
    for start_open_ms, gap_end_ms in ranges_in_random_order(missing_ranges):
        window_plan.extend(
            trade_windows_in_random_order(
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
        window_rows, error = fetch_trade_window(
            range_fetcher=range_fetcher,
            exchange=exchange,
            market=market,
            symbol=symbol,
            start_open_ms=window_start_ms,
            end_open_ms=window_end_ms,
        )
        if error is not None:
            gap_failed_windows.append(f"{window_start_ms}-{window_end_ms}: {error}")
            log_trade_window_progress(
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
        log_trade_window_progress(
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
    raise_if_all_trade_windows_failed(
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
    filtered = filter_rows_by_start_bound(gap_rows, start_open_ms_bound)
    return dedupe_sort_trade_rows(filtered)
