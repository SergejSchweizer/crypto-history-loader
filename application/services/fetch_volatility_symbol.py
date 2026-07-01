"""Volatility symbol-level Bronze fetch planning."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from application.services.fetch_range_planning import (
    build_missing_ranges_with_optional_head_gap,
    ranges_in_random_order,
)
from application.services.fetch_symbol_history import fetch_bootstrap_history_rows, fetch_bounded_daily_rows
from application.services.gapfill_service import _last_closed_open_ms, _missing_ranges_ms
from ingestion.lake_queries import open_times_in_lake_by_dataset
from ingestion.spot import Exchange, Market
from ingestion.volatility import (
    VolatilityPoint,
    normalize_volatility_timeframe,
    volatility_interval_to_milliseconds,
)


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
                return fetch_bounded_daily_rows(
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
            return fetch_bootstrap_history_rows(
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
            return fetch_bounded_daily_rows(
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
        return fetch_bootstrap_history_rows(
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

    missing_ranges = build_missing_ranges_with_optional_head_gap(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
        start_open_ms_bound=start_open_ms_bound,
        ranges_builder=ranges_builder,
    )
    if not missing_ranges:
        return []

    fetched: list[VolatilityPoint] = []
    for start_open_ms, gap_end_ms in ranges_in_random_order(missing_ranges):
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
