"""Shared history bootstrap helpers for symbol-level Bronze fetch planning."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from application.services.fetch_history_rows import (
    fetch_bootstrap_history_rows_with_start_bound,
    fetch_bounded_daily_rows_with_start_bound,
)
from application.services.fetch_range_planning import day_windows_in_random_order

TRow = TypeVar("TRow")


def fetch_bounded_daily_rows(
    *,
    start_open_ms_bound: int,
    end_open_ms: int,
    range_fetcher: Callable[..., list[TRow]],
    fetch_kwargs: dict[str, object],
    on_history_chunk: Callable[[list[TRow]], None] | None,
) -> list[TRow]:
    """Fetch inclusive bounded history in UTC-day windows with deterministic deduplication."""

    return fetch_bounded_daily_rows_with_start_bound(
        day_windows=day_windows_in_random_order(start_open_ms_bound, end_open_ms),
        range_fetcher=range_fetcher,
        fetch_kwargs=fetch_kwargs,
        on_history_chunk=on_history_chunk,
    )


def fetch_bootstrap_history_rows(
    *,
    history_fetcher: Callable[..., list[TRow]],
    fetch_kwargs: dict[str, object],
    on_history_chunk: Callable[[list[TRow]], None] | None,
    start_open_ms_bound: int | None,
) -> list[TRow]:
    """Run history bootstrap fetch, then apply deterministic bound-filtered deduplication."""

    return fetch_bootstrap_history_rows_with_start_bound(
        history_fetcher=history_fetcher,
        fetch_kwargs=fetch_kwargs,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=start_open_ms_bound,
    )
