"""Deterministic changed-month planning for incremental Gold M1 builds."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

from application.services.silver_incremental_planner import plan_incremental_months

_MONTH_SEGMENT = re.compile(r"(?:^|/)month=(\d{4}-\d{2})(?:/|$)")


@dataclass(frozen=True)
class GoldM1IncrementalPlan:
    """The Silver changes and Gold M1 months that must be republished."""

    changed_months: tuple[str, ...]
    rebuild_months: tuple[str, ...]
    feature_lookback_minutes: int


def plan_gold_m1_incremental_months(
    *,
    current_artifacts: dict[str, str],
    previous_artifacts: dict[str, str] | None,
    feature_lookback_minutes: int,
) -> GoldM1IncrementalPlan:
    """Plan impacted Gold M1 months from persisted Silver artifact identities.

    A Silver partition is changed when it is added, removed, or its exact parquet or
    source-manifest identity differs from the prior Gold manifest.  Months without a
    partition segment are intentionally excluded: they cannot be selectively rebuilt
    and remain part of the backward-compatible full-artifact path.

    Args:
        current_artifacts: Current root-relative artifact identities.
        previous_artifacts: Persisted identities from the last valid Gold manifest.
        feature_lookback_minutes: Longest trailing Gold feature dependency.

    Returns:
        A stable set of changed source months and affected target months.

    Raises:
        ValueError: If the requested lookback is negative.
    """

    if feature_lookback_minutes < 0:
        raise ValueError("feature_lookback_minutes must be non-negative")
    previous = previous_artifacts or {}
    changed_keys = {
        key for key in set(current_artifacts).union(previous) if current_artifacts.get(key) != previous.get(key)
    }
    available_months = sorted({month for key in current_artifacts if (month := _artifact_month(key)) is not None})
    changed_months = sorted({month for key in changed_keys if (month := _artifact_month(key)) is not None})
    plan = plan_incremental_months(
        available_months=available_months,
        changed_months=changed_months,
        lookback_days=math.ceil(feature_lookback_minutes / (24 * 60)),
    )
    return GoldM1IncrementalPlan(
        changed_months=plan.changed_months,
        rebuild_months=plan.rebuild_months,
        feature_lookback_minutes=feature_lookback_minutes,
    )


def _artifact_month(artifact_key: str) -> str | None:
    """Extract a canonical month partition key from one manifest artifact key."""

    match = _MONTH_SEGMENT.search(artifact_key)
    return match.group(1) if match is not None else None
