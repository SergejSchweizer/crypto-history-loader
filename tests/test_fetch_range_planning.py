"""Tests for deterministic fetch range planning helpers."""

from __future__ import annotations

from datetime import UTC, date, datetime

from application.services.fetch_range_planning import (
    day_end_ms,
    day_start_ms,
    missing_trade_day_ranges,
    ranges_in_random_order,
    split_range_into_utc_days,
)


def test_split_range_into_utc_days_splits_cross_day_windows() -> None:
    """Split inclusive ranges exactly at UTC day boundaries."""

    start_ms = int(datetime(2026, 4, 27, 23, 59, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 28, 0, 1, tzinfo=UTC).timestamp() * 1000)

    windows = split_range_into_utc_days(start_ms, end_ms)

    assert windows == [
        (
            start_ms,
            int(datetime(2026, 4, 27, 23, 59, 59, 999000, tzinfo=UTC).timestamp() * 1000),
        ),
        (int(datetime(2026, 4, 28, 0, 0, tzinfo=UTC).timestamp() * 1000), end_ms),
    ]


def test_ranges_in_random_order_is_chronological() -> None:
    """Keep missing ranges deterministic for restart-safe fetches."""

    assert ranges_in_random_order([(3, 4), (1, 2), (5, 6)]) == [(1, 2), (3, 4), (5, 6)]


def test_day_start_and_end_ms_are_utc_bounds() -> None:
    """Convert dates to inclusive UTC day bounds."""

    value = date(2026, 4, 27)

    assert day_start_ms(value) == int(datetime(2026, 4, 27, 0, 0, tzinfo=UTC).timestamp() * 1000)
    assert day_end_ms(value) == int(datetime(2026, 4, 27, 23, 59, 59, 999000, tzinfo=UTC).timestamp() * 1000)


def test_missing_trade_day_ranges_resumes_clear_partial_days() -> None:
    """Plan only missing trade-day coverage beyond tolerated edge gaps."""

    target_day = date(2026, 4, 27)
    start_ms = day_start_ms(target_day)
    end_ms = day_end_ms(target_day)
    bounds = {
        target_day: (
            datetime(2026, 4, 27, 0, 0, 30, tzinfo=UTC),
            datetime(2026, 4, 27, 23, 58, 0, tzinfo=UTC),
        )
    }

    assert missing_trade_day_ranges(
        existing_dates=[target_day],
        coverage_bounds=bounds,
        start_open_ms=start_ms,
        end_open_ms=end_ms,
    ) == [(int(datetime(2026, 4, 27, 23, 58, 0, tzinfo=UTC).timestamp() * 1000) + 1, end_ms)]
