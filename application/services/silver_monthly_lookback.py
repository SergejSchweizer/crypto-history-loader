"""Shared calendar-month arithmetic for cross-month rolling-state buffering (QC-02).

Silver builders that compute rolling windows (realized volatility, implied-volatility
z-scores/percentiles, IV/RV spread z-scores) previously processed each monthly Bronze/
Silver partition independently, which discarded the trailing observations and previous
close needed at the start of every month. These helpers compute which additional prior
calendar months must be read so a builder can buffer enough lookback context, compute
rolling features on the buffered frame, and then trim the output back to the requested
target month.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


def month_start(month: str) -> datetime:
    """Return the UTC start-of-month timestamp for a ``YYYY-MM`` month key."""

    year_part, month_part = month.split("-", 1)
    return datetime(int(year_part), int(month_part), 1, tzinfo=UTC)


def month_end_exclusive(month: str) -> datetime:
    """Return the UTC start timestamp of the calendar month following ``month``."""

    start = month_start(month)
    if start.month == 12:
        return start.replace(year=start.year + 1, month=1)
    return start.replace(month=start.month + 1)


def lookback_month_keys(month: str, *, lookback_days: int) -> list[str]:
    """Return calendar month keys strictly before ``month`` needed to cover ``lookback_days``.

    The result is ordered oldest-first and never includes ``month`` itself. Callers
    typically read these months in addition to ``month`` to buffer enough trailing
    context for rolling-window calculations, then trim the computed frame back to
    ``[month_start(month), month_end_exclusive(month))`` before writing output.
    """

    target_start = month_start(month)
    calculation_start = target_start - timedelta(days=lookback_days)
    keys: list[str] = []
    year, mon = calculation_start.year, calculation_start.month
    while (year, mon) < (target_start.year, target_start.month):
        keys.append(f"{year:04d}-{mon:02d}")
        if mon == 12:
            year += 1
            mon = 1
        else:
            mon += 1
    return keys
