"""Fetch orchestration services for OHLCV and open-interest datasets."""

from __future__ import annotations

import logging
import multiprocessing as _mp
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime
from typing import Any, TypeVar, cast

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
from application.schema import dataset_contract
from application.services import fetch_executors as _fetch_executors
from application.services import fetch_range_planning as _range_planning
from application.services import fetch_trade_windows as _trade_windows
from application.services.fetch_bootstrap import fetch_bootstrap_history_rows, fetch_bounded_daily_rows
from application.services.fetch_executors import elapsed_seconds, run_with_optional_history_chunk
from application.services.fetch_runtime_policy import heartbeat_seconds, task_timeout_seconds
from application.services.gapfill_service import _last_closed_open_ms, _missing_ranges_ms
from ingestion.funding import (
    FundingPoint,
    fetch_funding_all_history,
    fetch_funding_range,
    funding_interval_to_milliseconds,
    normalize_funding_timeframe,
)
from ingestion.lake import (
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

OI_DATASET_TYPE = dataset_contract("oi").dataset_type
TRow = TypeVar("TRow")
logger = logging.getLogger(__name__)

mp = _mp
TRADE_BOUNDARY_TOLERANCE_MS = _range_planning.TRADE_BOUNDARY_TOLERANCE_MS
_day_end_ms = _range_planning.day_end_ms
_day_start_ms = _range_planning.day_start_ms
_day_windows_in_random_order = _range_planning.day_windows_in_random_order
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


def _row_open_time_ms(row: object) -> int:
    """Return row open timestamp in epoch milliseconds."""

    row_any = cast(Any, row)
    timestamp = getattr(row_any, "open_time", None)
    if timestamp is None:
        timestamp = getattr(row_any, "trade_time", None)
    if not isinstance(timestamp, datetime):
        raise ValueError("row is missing open_time/trade_time datetime attribute")
    return int(timestamp.timestamp() * 1000)


def _filter_rows_by_start_bound(rows: list[TRow], start_open_ms_bound: int | None) -> list[TRow]:
    """Filter rows by inclusive start bound when provided."""

    if start_open_ms_bound is None:
        return rows
    return [item for item in rows if _row_open_time_ms(item) >= start_open_ms_bound]


def _filter_chunk_callback(
    on_history_chunk: Callable[[list[TRow]], None] | None,
    start_open_ms_bound: int | None,
) -> Callable[[list[TRow]], None] | None:
    """Wrap chunk callback with optional start-bound filtering."""

    if on_history_chunk is None:
        return None
    if start_open_ms_bound is None:
        return on_history_chunk

    def _filtered_chunk(rows: list[TRow]) -> None:
        filtered = _filter_rows_by_start_bound(rows, start_open_ms_bound)
        if filtered:
            on_history_chunk(filtered)

    return _filtered_chunk


def _task_timeout_seconds() -> float | None:
    """Return optional per-task timeout in seconds from environment."""

    return task_timeout_seconds()


def _heartbeat_seconds() -> float:
    """Return heartbeat interval in seconds for long-running fetch tasks."""

    return heartbeat_seconds()


def _fetch_bounded_daily_rows(
    *,
    start_open_ms_bound: int,
    end_open_ms: int,
    range_fetcher: Callable[..., list[TRow]],
    fetch_kwargs: dict[str, object],
    on_history_chunk: Callable[[list[TRow]], None] | None,
) -> list[TRow]:
    """Fetch inclusive bounded history in UTC-day windows with deterministic deduplication."""

    return fetch_bounded_daily_rows(
        day_windows=_day_windows_in_random_order(start_open_ms_bound, end_open_ms),
        range_fetcher=range_fetcher,
        fetch_kwargs=fetch_kwargs,
        dedupe_key=_row_open_time_ms,
        on_history_chunk=on_history_chunk,
    )


def _fetch_bootstrap_history_rows(
    *,
    history_fetcher: Callable[..., list[TRow]],
    fetch_kwargs: dict[str, object],
    on_history_chunk: Callable[[list[TRow]], None] | None,
    start_open_ms_bound: int | None,
) -> list[TRow]:
    """Run history bootstrap fetch, then apply deterministic bound-filtered deduplication."""

    filtered_rows = fetch_bootstrap_history_rows(
        history_fetcher=history_fetcher,
        fetch_kwargs=fetch_kwargs,
        on_history_chunk=on_history_chunk,
        wrap_chunk_callback=lambda callback: _filter_chunk_callback(callback, start_open_ms_bound),
        filter_rows=lambda rows: _filter_rows_by_start_bound(rows, start_open_ms_bound),
    )
    unique_by_open_time = {_row_open_time_ms(item): item for item in filtered_rows}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


def _build_missing_ranges_with_optional_head_gap(
    *,
    existing_open_times: list[datetime],
    interval_ms: int,
    end_open_ms: int,
    start_open_ms_bound: int | None,
    ranges_builder: Callable[..., list[tuple[int, int]]],
) -> list[tuple[int, int]]:
    """Build missing ranges with optional head-gap extension from explicit start bound.

    This applies one shared gap-planning policy across dataset fetchers: internal gaps,
    tail gap to ``end_open_ms``, and optional head-gap when an earlier explicit start
    boundary is configured.
    """

    missing_ranges = ranges_builder(
        existing_open_times=existing_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )
    earliest_existing_ms = int(min(existing_open_times).timestamp() * 1000)
    if start_open_ms_bound is not None and start_open_ms_bound < earliest_existing_ms:
        head_end_ms = min(earliest_existing_ms - interval_ms, end_open_ms)
        if start_open_ms_bound <= head_end_ms:
            missing_ranges.append((start_open_ms_bound, head_end_ms))
    return missing_ranges


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

    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    interval_ms = interval_ms_resolver(exchange=exchange, interval=timeframe)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)
    if start_open_ms_bound is not None and end_open_ms < start_open_ms_bound:
        return []

    if tail_delta_only:
        latest_reader = latest_open_time_reader
        if latest_reader is None:
            raise ValueError("latest_open_time_reader is required when tail_delta_only is enabled")
        latest_open_time = latest_reader(
            lake_root=lake_root,
            market=market,
            exchange=exchange,
            symbol=storage_symbol,
            timeframe=timeframe,
        )
        if latest_open_time is None:
            if start_open_ms_bound is not None:
                return _fetch_bounded_daily_rows(
                    start_open_ms_bound=start_open_ms_bound,
                    end_open_ms=end_open_ms,
                    range_fetcher=range_fetcher,
                    fetch_kwargs={
                        "exchange": exchange,
                        "symbol": symbol,
                        "interval": timeframe,
                        "market": market,
                    },
                    on_history_chunk=on_history_chunk,
                )
            return _fetch_bootstrap_history_rows(
                history_fetcher=history_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "market": market,
                    "interval": timeframe,
                },
                on_history_chunk=on_history_chunk,
                start_open_ms_bound=start_open_ms_bound,
            )
        start_open_ms = int(latest_open_time.timestamp() * 1000) + interval_ms
        if start_open_ms_bound is not None:
            start_open_ms = max(start_open_ms, start_open_ms_bound)
        if start_open_ms > end_open_ms:
            return []
        fetched_rows: list[SpotCandle] = []
        for day_start_ms, day_end_ms in _day_windows_in_random_order(start_open_ms, end_open_ms):
            fetched_rows.extend(
                range_fetcher(
                    exchange=exchange,
                    symbol=symbol,
                    interval=timeframe,
                    start_open_ms=day_start_ms,
                    end_open_ms=day_end_ms,
                    market=market,
                )
            )
        unique_by_open_time = {item.open_time: item for item in fetched_rows}
        return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]

    stored_open_times = open_times_reader(
        lake_root=lake_root,
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=timeframe,
    )

    if not stored_open_times:
        if start_open_ms_bound is not None:
            return _fetch_bounded_daily_rows(
                start_open_ms_bound=start_open_ms_bound,
                end_open_ms=end_open_ms,
                range_fetcher=range_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": timeframe,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
            )
        return _fetch_bootstrap_history_rows(
            history_fetcher=history_fetcher,
            fetch_kwargs={
                "exchange": exchange,
                "symbol": symbol,
                "market": market,
                "interval": timeframe,
            },
            on_history_chunk=on_history_chunk,
            start_open_ms_bound=start_open_ms_bound,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = _build_missing_ranges_with_optional_head_gap(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
        start_open_ms_bound=start_open_ms_bound,
        ranges_builder=ranges_builder,
    )
    if not missing_ranges:
        return []

    fetched: list[SpotCandle] = []
    for start_open_ms, gap_end_ms in _ranges_in_random_order(missing_ranges):
        for day_start_ms, day_end_ms in _day_windows_in_random_order(start_open_ms, gap_end_ms):
            fetched.extend(
                range_fetcher(
                    exchange=exchange,
                    symbol=symbol,
                    interval=timeframe,
                    start_open_ms=day_start_ms,
                    end_open_ms=day_end_ms,
                    market=market,
                )
            )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


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

    if market != "perp":
        return []

    normalized_interval = timeframe_normalizer(exchange=exchange, value=timeframe)
    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    interval_ms = interval_ms_resolver(exchange=exchange, interval=normalized_interval)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)
    if start_open_ms_bound is not None and end_open_ms < start_open_ms_bound:
        return []

    if tail_delta_only:
        latest_reader = latest_open_time_reader
        if latest_reader is None:
            raise ValueError("latest_open_time_reader is required when tail_delta_only is enabled")
        latest_open_time = latest_reader(
            lake_root=lake_root,
            dataset_type=OI_DATASET_TYPE,
            market=market,
            exchange=exchange,
            symbol=storage_symbol,
            timeframe=normalized_interval,
        )
        if latest_open_time is None:
            if start_open_ms_bound is not None:
                return _fetch_bounded_daily_rows(
                    start_open_ms_bound=start_open_ms_bound,
                    end_open_ms=end_open_ms,
                    range_fetcher=range_fetcher,
                    fetch_kwargs={
                        "exchange": exchange,
                        "symbol": symbol,
                        "interval": normalized_interval,
                        "market": market,
                    },
                    on_history_chunk=on_history_chunk,
                )
            return _fetch_bootstrap_history_rows(
                history_fetcher=history_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": normalized_interval,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
                start_open_ms_bound=start_open_ms_bound,
            )
        start_open_ms = int(latest_open_time.timestamp() * 1000) + interval_ms
        if start_open_ms_bound is not None:
            start_open_ms = max(start_open_ms, start_open_ms_bound)
        if start_open_ms > end_open_ms:
            return []
        fetched_rows: list[OpenInterestPoint] = []
        for day_start_ms, day_end_ms in _day_windows_in_random_order(start_open_ms, end_open_ms):
            fetched_rows.extend(
                range_fetcher(
                    exchange=exchange,
                    symbol=symbol,
                    interval=normalized_interval,
                    start_open_ms=day_start_ms,
                    end_open_ms=day_end_ms,
                    market=market,
                )
            )
        unique_by_open_time = {item.open_time: item for item in fetched_rows}
        return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]

    stored_open_times = open_times_reader(
        lake_root=lake_root,
        dataset_type=OI_DATASET_TYPE,
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=normalized_interval,
    )

    if not stored_open_times:
        if start_open_ms_bound is not None:
            return _fetch_bounded_daily_rows(
                start_open_ms_bound=start_open_ms_bound,
                end_open_ms=end_open_ms,
                range_fetcher=range_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": normalized_interval,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
            )
        return _fetch_bootstrap_history_rows(
            history_fetcher=history_fetcher,
            fetch_kwargs={
                "exchange": exchange,
                "symbol": symbol,
                "interval": normalized_interval,
                "market": market,
            },
            on_history_chunk=on_history_chunk,
            start_open_ms_bound=start_open_ms_bound,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = _build_missing_ranges_with_optional_head_gap(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
        start_open_ms_bound=start_open_ms_bound,
        ranges_builder=ranges_builder,
    )
    if not missing_ranges:
        return []

    fetched: list[OpenInterestPoint] = []
    for start_open_ms, gap_end_ms in _ranges_in_random_order(missing_ranges):
        for day_start_ms, day_end_ms in _day_windows_in_random_order(start_open_ms, gap_end_ms):
            fetched.extend(
                range_fetcher(
                    exchange=exchange,
                    symbol=symbol,
                    interval=normalized_interval,
                    start_open_ms=day_start_ms,
                    end_open_ms=day_end_ms,
                    market=market,
                )
            )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


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

    if market != "perp":
        return []

    normalized_interval = timeframe_normalizer(exchange=exchange, value=timeframe)
    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    interval_ms = interval_ms_resolver(exchange=exchange, interval=normalized_interval)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)
    if start_open_ms_bound is not None and end_open_ms < start_open_ms_bound:
        return []

    if tail_delta_only:
        latest_reader = latest_open_time_reader
        if latest_reader is None:
            raise ValueError("latest_open_time_reader is required when tail_delta_only is enabled")
        latest_open_time = latest_reader(
            lake_root=lake_root,
            dataset_type="funding",
            market=market,
            exchange=exchange,
            symbol=storage_symbol,
            timeframe=normalized_interval,
        )
        if latest_open_time is None:
            if start_open_ms_bound is not None:
                return _fetch_bounded_daily_rows(
                    start_open_ms_bound=start_open_ms_bound,
                    end_open_ms=end_open_ms,
                    range_fetcher=range_fetcher,
                    fetch_kwargs={
                        "exchange": exchange,
                        "symbol": symbol,
                        "interval": normalized_interval,
                        "market": market,
                    },
                    on_history_chunk=on_history_chunk,
                )
            return _fetch_bootstrap_history_rows(
                history_fetcher=history_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": normalized_interval,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
                start_open_ms_bound=start_open_ms_bound,
            )
        start_open_ms = int(latest_open_time.timestamp() * 1000) + interval_ms
        if start_open_ms_bound is not None:
            start_open_ms = max(start_open_ms, start_open_ms_bound)
        if start_open_ms > end_open_ms:
            return []
        # Funding is naturally sparse (e.g. 8h on Deribit), so one ranged call is
        # substantially faster than many day-sized calls that each re-page backend data.
        fetched_rows = range_fetcher(
            exchange=exchange,
            symbol=symbol,
            interval=normalized_interval,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
            market=market,
        )
        unique_by_open_time = {item.open_time: item for item in fetched_rows}
        return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]

    stored_open_times = open_times_reader(
        lake_root=lake_root,
        dataset_type="funding",
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=normalized_interval,
    )

    if not stored_open_times:
        if start_open_ms_bound is not None:
            return _fetch_bounded_daily_rows(
                start_open_ms_bound=start_open_ms_bound,
                end_open_ms=end_open_ms,
                range_fetcher=range_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": normalized_interval,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
            )
        return _fetch_bootstrap_history_rows(
            history_fetcher=history_fetcher,
            fetch_kwargs={
                "exchange": exchange,
                "symbol": symbol,
                "interval": normalized_interval,
                "market": market,
            },
            on_history_chunk=on_history_chunk,
            start_open_ms_bound=start_open_ms_bound,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = _build_missing_ranges_with_optional_head_gap(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
        start_open_ms_bound=start_open_ms_bound,
        ranges_builder=ranges_builder,
    )
    if not missing_ranges:
        return []

    fetched: list[FundingPoint] = []
    for start_open_ms, gap_end_ms in _ranges_in_random_order(missing_ranges):
        fetched.extend(
            range_fetcher(
                exchange=exchange,
                symbol=symbol,
                interval=normalized_interval,
                start_open_ms=start_open_ms,
                end_open_ms=gap_end_ms,
                market=market,
            )
        )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


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

    if market != "perp":
        return []

    normalized_interval = timeframe_normalizer(exchange=exchange, value=timeframe)
    interval_ms = interval_ms_resolver(exchange=exchange, interval=normalized_interval)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)
    if start_open_ms_bound is not None and end_open_ms < start_open_ms_bound:
        return []

    if tail_delta_only:
        latest_reader = latest_open_time_reader
        if latest_reader is None:
            raise ValueError("latest_open_time_reader is required when tail_delta_only is enabled")
        latest_open_time = latest_reader(
            lake_root=lake_root,
            dataset_type=dataset_type,
            market=market,
            exchange=exchange,
            symbol=symbol.upper(),
            timeframe=normalized_interval,
        )
        if latest_open_time is None:
            if start_open_ms_bound is not None:
                return _fetch_bounded_daily_rows(
                    start_open_ms_bound=start_open_ms_bound,
                    end_open_ms=end_open_ms,
                    range_fetcher=range_fetcher,
                    fetch_kwargs={
                        "exchange": exchange,
                        "symbol": symbol,
                        "interval": normalized_interval,
                        "market": market,
                    },
                    on_history_chunk=on_history_chunk,
                )
            return _fetch_bootstrap_history_rows(
                history_fetcher=history_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": normalized_interval,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
                start_open_ms_bound=start_open_ms_bound,
            )
        start_open_ms = int(latest_open_time.timestamp() * 1000) + interval_ms
        if start_open_ms_bound is not None:
            start_open_ms = max(start_open_ms, start_open_ms_bound)
        if start_open_ms > end_open_ms:
            return []
        fetched_rows = range_fetcher(
            exchange=exchange,
            symbol=symbol,
            interval=normalized_interval,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
            market=market,
        )
        unique_by_open_time = {item.open_time: item for item in fetched_rows}
        return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]

    stored_open_times = open_times_reader(
        lake_root=lake_root,
        dataset_type=dataset_type,
        market=market,
        exchange=exchange,
        symbol=symbol.upper(),
        timeframe=normalized_interval,
    )
    if not stored_open_times:
        if start_open_ms_bound is not None:
            return _fetch_bounded_daily_rows(
                start_open_ms_bound=start_open_ms_bound,
                end_open_ms=end_open_ms,
                range_fetcher=range_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": normalized_interval,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
            )
        return _fetch_bootstrap_history_rows(
            history_fetcher=history_fetcher,
            fetch_kwargs={
                "exchange": exchange,
                "symbol": symbol,
                "interval": normalized_interval,
                "market": market,
            },
            on_history_chunk=on_history_chunk,
            start_open_ms_bound=start_open_ms_bound,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = _build_missing_ranges_with_optional_head_gap(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
        start_open_ms_bound=start_open_ms_bound,
        ranges_builder=ranges_builder,
    )
    if not missing_ranges:
        return []

    fetched: list[VolatilityPoint] = []
    for start_open_ms, gap_end_ms in _ranges_in_random_order(missing_ranges):
        fetched.extend(
            range_fetcher(
                exchange=exchange,
                symbol=symbol,
                interval=normalized_interval,
                start_open_ms=start_open_ms,
                end_open_ms=gap_end_ms,
                market=market,
            )
        )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


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
    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]] = {}
    task_errors: dict[tuple[Exchange, Market, str, str], str] = {}
    task_timeout_s = _task_timeout_seconds()
    heartbeat_s = _heartbeat_seconds()
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
        hb_exchange = task.exchange
        hb_market = task.market
        hb_symbol = task.symbol
        hb_timeframe = task.timeframe

        def _hb_ohlcv(
            elapsed_s: int,
            ex: str = hb_exchange,
            mk: str = hb_market,
            sy: str = hb_symbol,
            tf: str = hb_timeframe,
        ) -> None:
            del elapsed_s, ex, mk, sy, tf

        try:
            candles = cast(
                list[SpotCandle],
                run_with_optional_history_chunk(
                    runner=_run_with_optional_timeout,
                    fn=symbol_fetcher,
                    timeout_s=task_timeout_s,
                    heartbeat_s=heartbeat_s,
                    heartbeat=_hb_ohlcv,
                    use_process_timeout=False,
                    kwargs={
                        "exchange": task.exchange,
                        "market": task.market,
                        "symbol": task.symbol,
                        "timeframe": task.timeframe,
                        "lake_root": lake_root,
                        "on_history_chunk": (lambda rows, _task=task: on_task_chunk(_task, rows))
                        if on_task_chunk is not None
                        else None,
                    },
                ),
            )
            elapsed_s = elapsed_seconds(task_started_at)
            logger.info(
                "Fetch done [%s/%s] type=ohlcv exchange=%s market=%s symbol=%s timeframe=%s rows=%s elapsed_s=%s",
                idx,
                total_tasks,
                task.exchange,
                task.market,
                task.symbol,
                task.timeframe,
                len(candles),
                elapsed_s,
            )
            task_results[key] = candles
            if on_task_complete is not None:
                on_task_complete(task, candles)
        except Exception as exc:  # noqa: BLE001
            elapsed_s = elapsed_seconds(task_started_at)
            logger.exception(
                "Fetch error type=ohlcv exchange=%s market=%s symbol=%s timeframe=%s elapsed_s=%s",
                task.exchange,
                task.market,
                task.symbol,
                task.timeframe,
                elapsed_s,
            )
            task_errors[key] = str(exc)

    return CandleFetchResultDTO(rows=task_results, errors=task_errors)


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
    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]] = {}
    task_errors: dict[tuple[Exchange, str, str], str] = {}
    task_timeout_s = _task_timeout_seconds()
    heartbeat_s = _heartbeat_seconds()
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
        hb_exchange = task.exchange
        hb_symbol = task.symbol
        hb_timeframe = task.timeframe
        history_chunk_cb: Callable[[list[OpenInterestPoint]], None] | None = None
        if on_task_chunk is not None:
            task_for_chunk = task

            def _history_chunk_oi(
                values: list[OpenInterestPoint],
                _task: OpenInterestFetchTaskDTO = task_for_chunk,
            ) -> None:
                on_task_chunk(_task, values)

            history_chunk_cb = _history_chunk_oi

        def _heartbeat_oi(
            elapsed_s: int,
            ex: Exchange = hb_exchange,
            sy: str = hb_symbol,
            tf: str = hb_timeframe,
        ) -> None:
            del elapsed_s, ex, sy, tf

        try:
            rows = cast(
                list[OpenInterestPoint],
                run_with_optional_history_chunk(
                    runner=_run_with_optional_timeout,
                    fn=symbol_fetcher,
                    timeout_s=task_timeout_s,
                    heartbeat_s=heartbeat_s,
                    heartbeat=_heartbeat_oi,
                    use_process_timeout=True,
                    kwargs={
                        "exchange": task.exchange,
                        "market": "perp",
                        "symbol": task.symbol,
                        "timeframe": task.timeframe,
                        "lake_root": lake_root,
                        "on_history_chunk": history_chunk_cb,
                    },
                ),
            )
            elapsed_s = elapsed_seconds(task_started_at)
            logger.info(
                "Fetch done [%s/%s] type=oi exchange=%s market=perp symbol=%s timeframe=%s rows=%s elapsed_s=%s",
                idx,
                total_tasks,
                task.exchange,
                task.symbol,
                task.timeframe,
                len(rows),
                elapsed_s,
            )
            task_results[key] = rows
            if on_task_complete is not None:
                on_task_complete(task, rows)
        except Exception as exc:  # noqa: BLE001
            elapsed_s = elapsed_seconds(task_started_at)
            logger.exception(
                "Fetch error type=oi exchange=%s market=perp symbol=%s timeframe=%s elapsed_s=%s",
                task.exchange,
                task.symbol,
                task.timeframe,
                elapsed_s,
            )
            task_errors[key] = str(exc)
    return OpenInterestFetchResultDTO(rows=task_results, errors=task_errors)


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
    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, str, str], list[FundingPoint]] = {}
    task_errors: dict[tuple[Exchange, str, str], str] = {}
    task_timeout_s = _task_timeout_seconds()
    heartbeat_s = _heartbeat_seconds()
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
        hb_exchange = task.exchange
        hb_symbol = task.symbol
        hb_timeframe = task.timeframe
        history_chunk_cb: Callable[[list[FundingPoint]], None] | None = None
        if on_task_chunk is not None:
            task_for_chunk = task

            def _history_chunk_funding(
                values: list[FundingPoint],
                _task: FundingFetchTaskDTO = task_for_chunk,
            ) -> None:
                on_task_chunk(_task, values)

            history_chunk_cb = _history_chunk_funding

        def _heartbeat_funding(
            elapsed_s: int,
            ex: Exchange = hb_exchange,
            sy: str = hb_symbol,
            tf: str = hb_timeframe,
        ) -> None:
            del elapsed_s, ex, sy, tf

        try:
            rows = cast(
                list[FundingPoint],
                run_with_optional_history_chunk(
                    runner=_run_with_optional_timeout,
                    fn=symbol_fetcher,
                    timeout_s=task_timeout_s,
                    heartbeat_s=heartbeat_s,
                    heartbeat=_heartbeat_funding,
                    use_process_timeout=False,
                    kwargs={
                        "exchange": task.exchange,
                        "market": "perp",
                        "symbol": task.symbol,
                        "timeframe": task.timeframe,
                        "lake_root": lake_root,
                        "on_history_chunk": history_chunk_cb,
                    },
                ),
            )
            elapsed_s = elapsed_seconds(task_started_at)
            logger.info(
                "Fetch done [%s/%s] type=funding exchange=%s market=perp symbol=%s timeframe=%s rows=%s elapsed_s=%s",
                idx,
                total_tasks,
                task.exchange,
                task.symbol,
                task.timeframe,
                len(rows),
                elapsed_s,
            )
            task_results[key] = rows
            if on_task_complete is not None:
                on_task_complete(task, rows)
        except Exception as exc:  # noqa: BLE001
            elapsed_s = elapsed_seconds(task_started_at)
            logger.exception(
                "Fetch error type=funding exchange=%s market=perp symbol=%s timeframe=%s elapsed_s=%s",
                task.exchange,
                task.symbol,
                task.timeframe,
                elapsed_s,
            )
            task_errors[key] = str(exc)
    return FundingFetchResultDTO(rows=task_results, errors=task_errors)


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
    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, str, str], list[VolatilityPoint]] = {}
    task_errors: dict[tuple[Exchange, str, str], str] = {}
    task_timeout_s = _task_timeout_seconds()
    heartbeat_s = _heartbeat_seconds()
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
        history_chunk_cb: Callable[[list[VolatilityPoint]], None] | None = None
        if on_task_chunk is not None:
            task_for_chunk = task

            def _history_chunk_volatility(
                values: list[VolatilityPoint],
                _task: VolatilityFetchTaskDTO = task_for_chunk,
            ) -> None:
                on_task_chunk(_task, values)

            history_chunk_cb = _history_chunk_volatility

        def _heartbeat_volatility(elapsed_s: int) -> None:
            del elapsed_s

        try:
            rows = cast(
                list[VolatilityPoint],
                run_with_optional_history_chunk(
                    runner=_run_with_optional_timeout,
                    fn=symbol_fetcher,
                    timeout_s=task_timeout_s,
                    heartbeat_s=heartbeat_s,
                    heartbeat=_heartbeat_volatility,
                    use_process_timeout=False,
                    kwargs={
                        "exchange": task.exchange,
                        "market": "perp",
                        "symbol": task.symbol,
                        "timeframe": task.timeframe,
                        "lake_root": lake_root,
                        "on_history_chunk": history_chunk_cb,
                    },
                ),
            )
            elapsed_s = elapsed_seconds(task_started_at)
            logger.info(
                "Fetch done [%s/%s] type=%s exchange=%s market=perp symbol=%s timeframe=%s rows=%s elapsed_s=%s",
                idx,
                total_tasks,
                task.dataset_type,
                task.exchange,
                task.symbol,
                task.timeframe,
                len(rows),
                elapsed_s,
            )
            task_results[key] = rows
            if on_task_complete is not None:
                on_task_complete(task, rows)
        except Exception as exc:  # noqa: BLE001
            elapsed_s = elapsed_seconds(task_started_at)
            logger.exception(
                "Fetch error type=%s exchange=%s market=perp symbol=%s timeframe=%s elapsed_s=%s",
                task.dataset_type,
                task.exchange,
                task.symbol,
                task.timeframe,
                elapsed_s,
            )
            task_errors[key] = str(exc)
    return VolatilityFetchResultDTO(rows=task_results, errors=task_errors)


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
    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]] = {}
    task_errors: dict[tuple[Exchange, TradeMarket, str], str] = {}
    task_timeout_s = _task_timeout_seconds()
    heartbeat_s = _heartbeat_seconds()
    bounded_concurrency = max(1, min(concurrency, total_tasks or 1))

    def _fetch_one(
        idx: int, task: TradeFetchTaskDTO
    ) -> tuple[
        TradeFetchTaskDTO,
        list[TradeTick | OptionTradeTick] | None,
        str | None,
    ]:
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

        hb_exchange = task.exchange
        hb_market = task.market
        hb_symbol = task.symbol

        def _hb_trades(
            elapsed_s: int,
            ex: str = hb_exchange,
            mk: TradeMarket = hb_market,
            sy: str = hb_symbol,
        ) -> None:
            logger.debug(
                "Fetch heartbeat type=trades exchange=%s market=%s symbol=%s elapsed_s=%s",
                ex,
                mk,
                sy,
                elapsed_s,
            )

        history_chunk_callback: Callable[[list[TradeTick | OptionTradeTick]], None] | None = None
        if on_task_chunk is not None:
            task_chunk_callback = on_task_chunk

            def _forward_trade_chunk(chunk: list[TradeTick | OptionTradeTick], _task: TradeFetchTaskDTO = task) -> None:
                task_chunk_callback(_task, chunk)

            history_chunk_callback = _forward_trade_chunk

        try:
            rows = _run_with_optional_timeout(
                symbol_fetcher,
                timeout_s=task_timeout_s,
                heartbeat_s=heartbeat_s,
                heartbeat=_hb_trades,
                exchange=task.exchange,
                market=task.market,
                symbol=task.symbol,
                lake_root=lake_root,
                on_history_chunk=history_chunk_callback,
            )
            elapsed_s = elapsed_seconds(started_at)
            logger.info(
                "Fetch done [%s/%s] type=trades exchange=%s market=%s symbol=%s rows=%s elapsed_s=%s",
                idx,
                total_tasks,
                task.exchange,
                task.market,
                task.symbol,
                len(rows),
                elapsed_s,
            )
        except Exception as exc:  # noqa: BLE001
            elapsed_s = elapsed_seconds(started_at)
            error_class = _classify_trade_fetch_error(exc)
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
            # Keep processing remaining tasks even when one route is unreachable.
            # This avoids a single transient network issue cascading into a full
            # trade-run failure classification.
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
                    task_errors[key] = f"[{_classify_trade_fetch_error(exc)}] {exc}"
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
                    task_errors[key] = f"[{_classify_trade_fetch_error(exc)}] {exc}"
    return TradeFetchResultDTO(rows=task_results, errors=task_errors)
