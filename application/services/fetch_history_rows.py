"""History row filtering helpers for fetch-service bootstraps."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any, TypeVar, cast

from application.services.fetch_bootstrap import fetch_bootstrap_history_rows, fetch_bounded_daily_rows

TRow = TypeVar("TRow")


def row_open_time_ms(row: object) -> int:
    """Return row open timestamp in epoch milliseconds."""

    row_any = cast(Any, row)
    timestamp = getattr(row_any, "open_time", None)
    if timestamp is None:
        timestamp = getattr(row_any, "trade_time", None)
    if not isinstance(timestamp, datetime):
        raise ValueError("row is missing open_time/trade_time datetime attribute")
    return int(timestamp.timestamp() * 1000)


def filter_rows_by_start_bound(rows: list[TRow], start_open_ms_bound: int | None) -> list[TRow]:
    """Filter rows by inclusive start bound when provided."""

    if start_open_ms_bound is None:
        return rows
    return [item for item in rows if row_open_time_ms(item) >= start_open_ms_bound]


def filter_chunk_callback(
    on_history_chunk: Callable[[list[TRow]], None] | None,
    start_open_ms_bound: int | None,
) -> Callable[[list[TRow]], None] | None:
    """Wrap chunk callback with optional start-bound filtering."""

    if on_history_chunk is None:
        return None
    if start_open_ms_bound is None:
        return on_history_chunk

    def _filtered_chunk(rows: list[TRow]) -> None:
        filtered = filter_rows_by_start_bound(rows, start_open_ms_bound)
        if filtered:
            on_history_chunk(filtered)

    return _filtered_chunk


def fetch_bounded_daily_rows_with_start_bound(
    *,
    day_windows: list[tuple[int, int]],
    range_fetcher: Callable[..., list[TRow]],
    fetch_kwargs: dict[str, object],
    on_history_chunk: Callable[[list[TRow]], None] | None,
) -> list[TRow]:
    """Fetch bounded daily history with deterministic open-time deduplication."""

    return fetch_bounded_daily_rows(
        day_windows=day_windows,
        range_fetcher=range_fetcher,
        fetch_kwargs=fetch_kwargs,
        dedupe_key=row_open_time_ms,
        on_history_chunk=on_history_chunk,
    )


def fetch_bootstrap_history_rows_with_start_bound(
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
        wrap_chunk_callback=lambda callback: filter_chunk_callback(callback, start_open_ms_bound),
        filter_rows=lambda rows: filter_rows_by_start_bound(rows, start_open_ms_bound),
    )
    unique_by_open_time = {row_open_time_ms(item): item for item in filtered_rows}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]
