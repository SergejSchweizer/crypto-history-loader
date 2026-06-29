"""Deterministic time-range planning helpers for Bronze fetches."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

TRADE_BOUNDARY_TOLERANCE_MS = 60_000


def ranges_in_random_order(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Return missing time ranges in deterministic ascending order.

    Args:
        ranges: Inclusive millisecond ranges to schedule.

    Returns:
        Ranges sorted by start and end timestamp. The historical function name
        is preserved because callers still rely on chronological execution for
        restart-safe persistence.
    """

    return sorted(ranges)


def split_range_into_utc_days(start_open_ms: int, end_open_ms: int) -> list[tuple[int, int]]:
    """Split an inclusive millisecond range into UTC day-bounded slices.

    Args:
        start_open_ms: Inclusive range start in epoch milliseconds.
        end_open_ms: Inclusive range end in epoch milliseconds.

    Returns:
        Chronological UTC day windows covering the requested inclusive range.
    """

    if end_open_ms < start_open_ms:
        return []
    start_dt = datetime.fromtimestamp(start_open_ms / 1000, tz=UTC)
    end_dt = datetime.fromtimestamp(end_open_ms / 1000, tz=UTC)
    cursor = start_dt
    windows: list[tuple[int, int]] = []
    while cursor.date() < end_dt.date():
        day_end = (
            datetime.combine(cursor.date(), datetime.min.time(), tzinfo=UTC)
            + timedelta(days=1)
            - timedelta(milliseconds=1)
        )
        windows.append((int(cursor.timestamp() * 1000), int(day_end.timestamp() * 1000)))
        cursor = day_end + timedelta(milliseconds=1)
    windows.append((int(cursor.timestamp() * 1000), end_open_ms))
    return windows


def day_windows_in_random_order(start_open_ms: int, end_open_ms: int) -> list[tuple[int, int]]:
    """Split range into UTC day windows and return deterministic chronological order.

    Args:
        start_open_ms: Inclusive range start in epoch milliseconds.
        end_open_ms: Inclusive range end in epoch milliseconds.

    Returns:
        Sorted UTC day windows. The historical name is preserved for
        compatibility with existing fetch orchestration code.
    """

    return sorted(split_range_into_utc_days(start_open_ms, end_open_ms))


def day_start_ms(value: date) -> int:
    """Return UTC day start timestamp in milliseconds.

    Args:
        value: UTC date to convert.

    Returns:
        Epoch milliseconds for ``00:00:00.000`` UTC on the requested date.
    """

    return int(datetime.combine(value, datetime.min.time(), tzinfo=UTC).timestamp() * 1000)


def day_end_ms(value: date) -> int:
    """Return UTC day end timestamp in milliseconds.

    Args:
        value: UTC date to convert.

    Returns:
        Epoch milliseconds for ``23:59:59.999`` UTC on the requested date.
    """

    return int(
        (
            datetime.combine(value, datetime.min.time(), tzinfo=UTC) + timedelta(days=1) - timedelta(milliseconds=1)
        ).timestamp()
        * 1000
    )


def missing_trade_day_ranges(
    *,
    existing_dates: list[date],
    coverage_bounds: dict[date, tuple[datetime, datetime]] | None = None,
    start_open_ms: int,
    end_open_ms: int,
) -> list[tuple[int, int]]:
    """Build missing trade ranges from daily tick partitions.

    Args:
        existing_dates: Dates with at least one persisted trade partition.
        coverage_bounds: Optional per-date min/max persisted trade timestamp.
        start_open_ms: Inclusive requested start in epoch milliseconds.
        end_open_ms: Inclusive requested end in epoch milliseconds.

    Returns:
        Inclusive millisecond ranges that still need fetching.

    Notes:
        Tick datasets use exact trade timestamps, so candle-grid gap detection
        would misclassify normal intra-minute trade times as missing. Daily
        partitions are the restart-safe coverage unit until a finer manifest
        exists. Near-boundary gaps are treated as covered because trade streams
        rarely contain records exactly at UTC day boundaries.
    """

    if end_open_ms < start_open_ms:
        return []
    existing = set(existing_dates)
    start_day = datetime.fromtimestamp(start_open_ms / 1000, tz=UTC).date()
    end_day = datetime.fromtimestamp(end_open_ms / 1000, tz=UTC).date()
    cursor = start_day
    ranges: list[tuple[int, int]] = []
    while cursor <= end_day:
        range_start_ms = max(start_open_ms, day_start_ms(cursor))
        range_end_ms = min(end_open_ms, day_end_ms(cursor))
        if cursor not in existing:
            ranges.append((range_start_ms, range_end_ms))
            cursor += timedelta(days=1)
            continue
        if coverage_bounds:
            bounds = coverage_bounds.get(cursor)
            if bounds is None:
                ranges.append((range_start_ms, range_end_ms))
                cursor += timedelta(days=1)
                continue
            min_open_ms = int(bounds[0].timestamp() * 1000)
            max_open_ms = int(bounds[1].timestamp() * 1000)
            if min_open_ms - range_start_ms > TRADE_BOUNDARY_TOLERANCE_MS:
                ranges.append((range_start_ms, min(min_open_ms - 1, range_end_ms)))
            if range_end_ms - max_open_ms > TRADE_BOUNDARY_TOLERANCE_MS:
                ranges.append((max(max_open_ms + 1, range_start_ms), range_end_ms))
        cursor += timedelta(days=1)
    return ranges


def build_missing_ranges_with_optional_head_gap(
    *,
    existing_open_times: list[datetime],
    interval_ms: int,
    end_open_ms: int,
    start_open_ms_bound: int | None,
    ranges_builder: Callable[..., list[tuple[int, int]]],
) -> list[tuple[int, int]]:
    """Build missing ranges with optional head-gap extension.

    Args:
        existing_open_times: Persisted timestamps for the requested dataset key.
        interval_ms: Expected interval between adjacent open timestamps.
        end_open_ms: Inclusive fetch end bound in epoch milliseconds.
        start_open_ms_bound: Optional explicit inclusive history start bound.
        ranges_builder: Dataset-specific internal/tail gap planner.

    Returns:
        Missing ranges from the range builder plus a head-gap range when the
        explicit start bound predates persisted coverage.

    Notes:
        The injected range builder owns internal and tail gap detection. This
        helper only adds the missing leading range caused by a later first
        persisted timestamp, keeping start-bound behavior consistent across
        OHLCV, OI, funding, and volatility fetchers.
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
