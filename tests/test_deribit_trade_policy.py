"""Tests for Deribit trade runtime policy readers."""

from __future__ import annotations

from ingestion.exchanges import deribit_trade_policy


def test_env_first_returns_first_configured_override(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DEPTH_SECOND", "second")
    monkeypatch.setenv("DEPTH_FIRST", "first")

    assert deribit_trade_policy.env_first("DEPTH_FIRST", "DEPTH_SECOND", default="default") == "first"
    assert deribit_trade_policy.env_first("DEPTH_MISSING", "DEPTH_SECOND", default="default") == "second"
    assert deribit_trade_policy.env_first("DEPTH_MISSING", default="default") == "default"


def test_normalized_base_url_strips_trailing_slashes(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DEPTH_BASE_URL", "https://example.org///")

    assert (
        deribit_trade_policy.normalized_base_url("DEPTH_BASE_URL", default="https://fallback") == "https://example.org"
    )


def test_numeric_policy_readers_are_bounded(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DEPTH_FLOAT", "-1")
    monkeypatch.setenv("DEPTH_INT", "-10")
    monkeypatch.setenv("DEPTH_PAGE_SIZE", "5000")
    monkeypatch.setenv("DEPTH_MAX_PAGES", "0")

    assert deribit_trade_policy.non_negative_float("DEPTH_FLOAT", default=0.25) == 0.0
    assert deribit_trade_policy.int_at_least("DEPTH_INT", default=5, minimum=1) == 1
    assert deribit_trade_policy.page_size("DEPTH_PAGE_SIZE", default=500, maximum=1000) == 1000
    assert deribit_trade_policy.max_pages_per_range("DEPTH_MAX_PAGES") == 0


def test_numeric_policy_readers_fall_back_on_invalid_values(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DEPTH_FLOAT", "not-a-float")
    monkeypatch.setenv("DEPTH_INT", "not-an-int")

    assert deribit_trade_policy.non_negative_float("DEPTH_FLOAT", default=0.25) == 0.25
    assert deribit_trade_policy.int_at_least("DEPTH_INT", default=5, minimum=1) == 5
