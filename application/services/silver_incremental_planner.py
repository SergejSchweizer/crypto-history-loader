"""Deterministic planning for incremental Silver monthly partition rebuilds."""

from __future__ import annotations

from dataclasses import dataclass

from application.services.silver_monthly_lookback import lookback_month_keys


@dataclass(frozen=True)
class SilverIncrementalPlan:
    """A deterministic plan of changed inputs and safe output partitions."""

    changed_months: tuple[str, ...]
    rebuild_months: tuple[str, ...]
    lookback_days: int


def plan_incremental_months(
    *,
    available_months: list[str],
    changed_months: list[str],
    lookback_days: int,
) -> SilverIncrementalPlan:
    """Plan the minimum available target months affected by changed source months.

    A rolling feature for a target month depends on input rows from its preceding
    lookback interval. A changed month therefore affects itself and every available
    future target whose lookback includes it. The output order is chronological and
    stable so retries use the identical partition sequence.

    Args:
        available_months: Existing or discoverable target-month keys in ``YYYY-MM`` form.
        changed_months: Source-month keys whose input fingerprint changed.
        lookback_days: Required trailing input window. Must be non-negative.

    Returns:
        The changed source months and the smallest safe ordered set of output months.

    Raises:
        ValueError: If ``lookback_days`` is negative or a month key is malformed.
    """

    if lookback_days < 0:
        raise ValueError("lookback_days must be non-negative")
    available = tuple(sorted(set(available_months)))
    changed = tuple(sorted(set(changed_months)))
    for month in (*available, *changed):
        _validate_month(month)
    changed_set = set(changed)
    rebuild = tuple(
        month
        for month in available
        if month in changed_set or changed_set.intersection(lookback_month_keys(month, lookback_days=lookback_days))
    )
    return SilverIncrementalPlan(changed_months=changed, rebuild_months=rebuild, lookback_days=lookback_days)


def _validate_month(value: str) -> None:
    """Validate a canonical month key through the shared calendar implementation."""

    lookback_month_keys(value, lookback_days=0)
