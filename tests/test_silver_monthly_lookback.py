"""Tests for cross-month rolling-state lookback arithmetic helpers (QC-02)."""

from __future__ import annotations

from datetime import UTC, datetime

from application.services.silver_monthly_lookback import (
    lookback_month_keys,
    month_end_exclusive,
    month_start,
)


def test_month_start_returns_utc_start_of_month() -> None:
    assert month_start("2024-03") == datetime(2024, 3, 1, tzinfo=UTC)


def test_month_end_exclusive_returns_next_month_start() -> None:
    assert month_end_exclusive("2024-03") == datetime(2024, 4, 1, tzinfo=UTC)


def test_month_end_exclusive_rolls_over_year_boundary() -> None:
    assert month_end_exclusive("2023-12") == datetime(2024, 1, 1, tzinfo=UTC)


def test_lookback_month_keys_excludes_target_month() -> None:
    assert "2024-03" not in lookback_month_keys("2024-03", lookback_days=30)


def test_lookback_month_keys_leap_year_february_stays_within_prior_two_months() -> None:
    # 2024 is a leap year: Feb has 29 days, so 30 days before March 1 reaches Jan 30.
    assert lookback_month_keys("2024-03", lookback_days=30) == ["2024-01", "2024-02"]


def test_lookback_month_keys_non_leap_year_february_reaches_january() -> None:
    # 2023 is not a leap year: Feb has 28 days, so 30 days before March 1 reaches Jan 30.
    assert lookback_month_keys("2023-03", lookback_days=30) == ["2023-01", "2023-02"]


def test_lookback_month_keys_crosses_year_boundary_for_january_target() -> None:
    assert lookback_month_keys("2024-01", lookback_days=30) == ["2023-12"]


def test_lookback_month_keys_small_lookback_stays_within_prior_month() -> None:
    assert lookback_month_keys("2024-02", lookback_days=1) == ["2024-01"]


def test_lookback_month_keys_zero_lookback_returns_empty() -> None:
    assert lookback_month_keys("2024-02", lookback_days=0) == []
