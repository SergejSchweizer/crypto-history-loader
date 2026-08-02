"""Tests for deterministic incremental Silver partition planning."""

from __future__ import annotations

import pytest

from application.services.silver_incremental_planner import plan_incremental_months


def test_plan_rebuilds_only_changed_month_and_required_dependents() -> None:
    """Include only target months whose trailing window contains the changed source."""

    plan = plan_incremental_months(
        available_months=["2026-01", "2026-02", "2026-03", "2026-04"],
        changed_months=["2026-02"],
        lookback_days=30,
    )

    assert plan.changed_months == ("2026-02",)
    assert plan.rebuild_months == ("2026-02", "2026-03")


def test_plan_is_deterministic_and_excludes_unavailable_dependents() -> None:
    """Sort duplicate input and never invent a non-existent output partition."""

    plan = plan_incremental_months(
        available_months=["2026-03", "2026-01", "2026-02", "2026-02"],
        changed_months=["2025-12", "2026-01", "2025-12"],
        lookback_days=30,
    )

    assert plan.changed_months == ("2025-12", "2026-01")
    assert plan.rebuild_months == ("2026-01", "2026-02", "2026-03")


def test_plan_rejects_invalid_inputs() -> None:
    """Reject unsupported calendar contracts instead of silently planning wrong work."""

    with pytest.raises(ValueError, match="non-negative"):
        plan_incremental_months(available_months=["2026-01"], changed_months=["2026-01"], lookback_days=-1)
    with pytest.raises(ValueError):
        plan_incremental_months(available_months=["not-a-month"], changed_months=[], lookback_days=0)
