"""Tests for fetch runtime policy helpers."""

from __future__ import annotations

import pytest

from application.services.fetch_runtime_policy import (
    DEFAULT_FETCH_CONCURRENCY,
    MAX_TRADE_WINDOW_MS,
    MIN_TRADE_WINDOW_MS,
    OPTIONS_TRADES_WINDOW_MS,
    PERP_TRADES_WINDOW_MS,
    fetch_concurrency,
    heartbeat_seconds,
    load_fetch_runtime_policy,
    task_timeout_seconds,
    trade_window_ms,
)


def test_task_timeout_seconds_zero_or_invalid_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEPTH_FETCH_TASK_TIMEOUT_S", "0")
    assert task_timeout_seconds() is None
    monkeypatch.setenv("DEPTH_FETCH_TASK_TIMEOUT_S", "-5")
    assert task_timeout_seconds() is None
    monkeypatch.setenv("DEPTH_FETCH_TASK_TIMEOUT_S", "bad")
    assert task_timeout_seconds() is None


def test_task_timeout_seconds_positive_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEPTH_FETCH_TASK_TIMEOUT_S", "3.5")
    assert task_timeout_seconds() == 3.5


def test_heartbeat_seconds_default_and_positive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEPTH_FETCH_HEARTBEAT_S", raising=False)
    assert heartbeat_seconds() == 30.0
    monkeypatch.setenv("DEPTH_FETCH_HEARTBEAT_S", "5")
    assert heartbeat_seconds() == 5.0
    monkeypatch.setenv("DEPTH_FETCH_HEARTBEAT_S", "-1")
    assert heartbeat_seconds() == 30.0


def test_load_fetch_runtime_policy_accepts_explicit_env_mapping() -> None:
    policy = load_fetch_runtime_policy(
        {
            "DEPTH_FETCH_TASK_TIMEOUT_S": "12.5",
            "DEPTH_FETCH_HEARTBEAT_S": "4",
            "DEPTH_FETCH_CONCURRENCY": "6",
            "DEPTH_PERP_TRADES_WINDOW_MINUTES": "30",
            "DEPTH_OPTIONS_TRADES_WINDOW_MINUTES": "120",
        }
    )

    assert policy.task_timeout_s == 12.5
    assert policy.heartbeat_s == 4.0
    assert policy.concurrency == 6
    assert policy.perp_trade_window_ms == 30 * 60 * 1000
    assert policy.options_trade_window_ms == 120 * 60 * 1000


def test_load_fetch_runtime_policy_bounds_trade_windows() -> None:
    policy = load_fetch_runtime_policy(
        {
            "DEPTH_PERP_TRADES_WINDOW_MINUTES": "0",
            "DEPTH_OPTIONS_TRADES_WINDOW_MINUTES": str(48 * 60),
        }
    )

    assert policy.perp_trade_window_ms == MIN_TRADE_WINDOW_MS
    assert policy.options_trade_window_ms == MAX_TRADE_WINDOW_MS


def test_load_fetch_runtime_policy_falls_back_for_invalid_values() -> None:
    policy = load_fetch_runtime_policy(
        {
            "DEPTH_FETCH_TASK_TIMEOUT_S": "bad",
            "DEPTH_FETCH_HEARTBEAT_S": "bad",
            "DEPTH_FETCH_CONCURRENCY": "bad",
            "DEPTH_PERP_TRADES_WINDOW_MINUTES": "bad",
            "DEPTH_OPTIONS_TRADES_WINDOW_MINUTES": "bad",
        }
    )

    assert policy.task_timeout_s is None
    assert policy.heartbeat_s == 30.0
    assert policy.concurrency == DEFAULT_FETCH_CONCURRENCY
    assert policy.perp_trade_window_ms == PERP_TRADES_WINDOW_MS
    assert policy.options_trade_window_ms == OPTIONS_TRADES_WINDOW_MS


def test_fetch_concurrency_is_bounded() -> None:
    assert fetch_concurrency({"DEPTH_FETCH_CONCURRENCY": "0"}) == 1
    assert fetch_concurrency({"DEPTH_FETCH_CONCURRENCY": "6"}) == 6
    assert fetch_concurrency({"DEPTH_FETCH_CONCURRENCY": "99"}) == 8
    assert fetch_concurrency({"DEPTH_FETCH_CONCURRENCY": "bad"}) == DEFAULT_FETCH_CONCURRENCY


def test_trade_window_ms_uses_market_specific_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEPTH_PERP_TRADES_WINDOW_MINUTES", "20")
    monkeypatch.setenv("DEPTH_OPTIONS_TRADES_WINDOW_MINUTES", "90")

    assert trade_window_ms("perp") == 20 * 60 * 1000
    assert trade_window_ms("option") == 90 * 60 * 1000
