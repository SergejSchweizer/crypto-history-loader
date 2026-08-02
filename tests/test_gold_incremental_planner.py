"""Tests for incremental Gold M1 month planning."""

from __future__ import annotations

import pytest

from application.services.gold_incremental_planner import plan_gold_m1_incremental_months


def test_plan_gold_m1_rebuilds_changed_month_and_feature_lookback_dependent_month() -> None:
    """A changed monthly source includes its trailing-window dependent target month."""

    current = {
        "spot:symbol=BTC/timeframe=1m/month=2026-01/a.parquet": "one",
        "spot:symbol=BTC/timeframe=1m/month=2026-02/b.parquet": "two",
        "spot:symbol=BTC/timeframe=1m/month=2026-03/c.parquet": "three",
    }
    previous = {**current, "spot:symbol=BTC/timeframe=1m/month=2026-02/b.parquet": "old"}

    plan = plan_gold_m1_incremental_months(
        current_artifacts=current,
        previous_artifacts=previous,
        feature_lookback_minutes=15,
    )

    assert plan.changed_months == ("2026-02",)
    assert plan.rebuild_months == ("2026-02", "2026-03")
    assert plan.feature_lookback_minutes == 15


def test_plan_gold_m1_handles_initial_build_and_ignores_unpartitioned_artifacts() -> None:
    """Initial monthly artifacts schedule all known months without inventing a partition."""

    plan = plan_gold_m1_incremental_months(
        current_artifacts={
            "spot:month=2026-02/data.parquet": "two",
            "funding:unpartitioned/data.parquet": "one",
        },
        previous_artifacts=None,
        feature_lookback_minutes=0,
    )

    assert plan.changed_months == ("2026-02",)
    assert plan.rebuild_months == ("2026-02",)


def test_plan_gold_m1_rejects_negative_lookback() -> None:
    """An invalid feature dependency cannot silently produce an unsafe plan."""

    with pytest.raises(ValueError, match="non-negative"):
        plan_gold_m1_incremental_months(current_artifacts={}, previous_artifacts={}, feature_lookback_minutes=-1)
