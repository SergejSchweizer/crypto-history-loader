"""Tests for deterministic fetch range planning helpers."""

from __future__ import annotations

from datetime import UTC, date, datetime

from application.services.fetch_range_planning import (
    build_missing_ranges_with_optional_head_gap,
    day_end_ms,
    day_start_ms,
    missing_trade_day_ranges,
    missing_trade_minute_ranges,
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


def test_missing_trade_day_ranges_refetches_partition_missing_coverage_metadata() -> None:
    """Refetch a persisted partition when its timestamp bounds are unavailable."""

    target_day = date(2026, 4, 27)
    start_ms = day_start_ms(target_day)
    end_ms = day_end_ms(target_day)

    assert missing_trade_day_ranges(
        existing_dates=[target_day],
        coverage_bounds={date(2026, 4, 26): (datetime(2026, 4, 26, tzinfo=UTC), datetime(2026, 4, 26, tzinfo=UTC))},
        start_open_ms=start_ms,
        end_open_ms=end_ms,
    ) == [(start_ms, end_ms)]


def test_trade_range_planners_reject_inverted_bounds() -> None:
    """Return no fetch work when a caller supplies an inverted interval."""

    start_ms = int(datetime(2026, 4, 27, 10, 1, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 27, 10, 0, tzinfo=UTC).timestamp() * 1000)

    assert split_range_into_utc_days(start_ms, end_ms) == []
    assert (
        missing_trade_day_ranges(
            existing_dates=[],
            start_open_ms=start_ms,
            end_open_ms=end_ms,
        )
        == []
    )
    assert (
        missing_trade_minute_ranges(
            existing_open_minutes=[],
            start_open_ms=start_ms,
            end_open_ms=end_ms,
        )
        == []
    )


def test_missing_trade_minute_ranges_clips_partial_edge_minutes() -> None:
    """Plan only absent minute buckets within exact caller-provided bounds."""

    start_ms = int(datetime(2026, 4, 27, 10, 0, 30, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 27, 10, 3, 15, tzinfo=UTC).timestamp() * 1000)

    assert missing_trade_minute_ranges(
        existing_open_minutes=[datetime(2026, 4, 27, 10, 1, 45, tzinfo=UTC)],
        start_open_ms=start_ms,
        end_open_ms=end_ms,
    ) == [
        (start_ms, int(datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC).timestamp() * 1000)),
        (int(datetime(2026, 4, 27, 10, 2, tzinfo=UTC).timestamp() * 1000), end_ms),
    ]


def test_build_missing_ranges_with_optional_head_gap_extends_from_start_bound() -> None:
    """Include leading explicit start-bound coverage before first persisted point."""

    existing = [datetime(2026, 4, 27, 10, 2, tzinfo=UTC)]
    start_bound_ms = int(datetime(2026, 4, 27, 10, 0, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 27, 10, 5, tzinfo=UTC).timestamp() * 1000)

    ranges = build_missing_ranges_with_optional_head_gap(
        existing_open_times=existing,
        interval_ms=60_000,
        end_open_ms=end_ms,
        start_open_ms_bound=start_bound_ms,
        ranges_builder=lambda **_: [(end_ms - 60_000, end_ms)],
    )

    assert ranges == [(end_ms - 60_000, end_ms), (start_bound_ms, start_bound_ms + 60_000)]


def test_build_missing_ranges_with_optional_head_gap_skips_when_bound_is_covered() -> None:
    """Do not add a leading range when persisted coverage already reaches the bound."""

    existing = [datetime(2026, 4, 27, 10, 0, tzinfo=UTC)]
    start_bound_ms = int(existing[0].timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 27, 10, 5, tzinfo=UTC).timestamp() * 1000)

    ranges = build_missing_ranges_with_optional_head_gap(
        existing_open_times=existing,
        interval_ms=60_000,
        end_open_ms=end_ms,
        start_open_ms_bound=start_bound_ms,
        ranges_builder=lambda **_: [(end_ms - 60_000, end_ms)],
    )

    assert ranges == [(end_ms - 60_000, end_ms)]


def test_build_missing_ranges_with_optional_head_gap_clips_to_early_end_bound() -> None:
    """Clip a leading range when the fetch end predates stored coverage."""

    existing = [datetime(2026, 4, 27, 10, 2, tzinfo=UTC)]
    start_bound_ms = int(datetime(2026, 4, 27, 10, 0, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 27, 10, 1, tzinfo=UTC).timestamp() * 1000)

    assert build_missing_ranges_with_optional_head_gap(
        existing_open_times=existing,
        interval_ms=60_000,
        end_open_ms=end_ms,
        start_open_ms_bound=start_bound_ms,
        ranges_builder=lambda **_: [],
    ) == [(start_bound_ms, end_ms)]
