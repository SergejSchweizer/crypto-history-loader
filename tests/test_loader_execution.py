"""Tests for Bronze loader execution orchestration helpers."""

from __future__ import annotations

import logging
from typing import Any, cast

import pytest

from api.commands.loader_execution import fetch_all_task_groups


def test_fetch_all_task_groups_dispatches_all_task_kinds() -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def _fetch_candles_fn(
        **kwargs: object,
    ) -> tuple[dict[tuple[str, str, str, str], list[int]], dict[tuple[str, str, str, str], str]]:  # noqa: E501
        calls.append(("candle", dict(kwargs)))
        return ({("deribit", "spot_ohlcv", "BTC", "1m"): [1]}, {})

    def _fetch_open_interest_fn(
        **kwargs: object,
    ) -> tuple[dict[tuple[str, str, str], list[int]], dict[tuple[str, str, str], str]]:
        calls.append(("open_interest", dict(kwargs)))
        return ({("deribit", "BTC", "1m"): [2]}, {})

    def _fetch_funding_fn(
        **kwargs: object,
    ) -> tuple[dict[tuple[str, str, str], list[int]], dict[tuple[str, str, str], str]]:
        calls.append(("funding", dict(kwargs)))
        return ({("deribit", "BTC", "1m"): [3]}, {})

    def _fetch_trades_fn(
        **kwargs: object,
    ) -> tuple[dict[tuple[str, str, str], list[int]], dict[tuple[str, str, str], str]]:
        calls.append(("trade", dict(kwargs)))
        return ({("deribit", "perp", "BTC"): [4]}, {})

    result: tuple[
        dict[tuple[str, str, str, str], list[int]],
        dict[tuple[str, str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
    ] = fetch_all_task_groups(
        candle_tasks=[("deribit", "spot_ohlcv", "BTC", "1m")],
        open_interest_tasks=[("deribit", "BTC", "1m")],
        funding_tasks=[("deribit", "BTC", "1m")],
        trade_tasks=[("deribit", "perp", "BTC")],
        lake_root="lake/bronze",
        candle_concurrency=2,
        open_interest_concurrency=3,
        funding_concurrency=4,
        trade_concurrency=5,
        logger=logging.getLogger("test_loader_execution_dispatch"),
        fetch_candles_fn=_fetch_candles_fn,
        fetch_open_interest_fn=_fetch_open_interest_fn,
        fetch_funding_fn=_fetch_funding_fn,
        fetch_trades_fn=_fetch_trades_fn,
    )

    assert result[0] == {("deribit", "spot_ohlcv", "BTC", "1m"): [1]}
    assert result[2] == {("deribit", "BTC", "1m"): [2]}
    assert result[4] == {("deribit", "BTC", "1m"): [3]}
    assert result[6] == {("deribit", "perp", "BTC"): [4]}

    call_map = {name: kwargs for name, kwargs in calls}
    assert cast(dict[str, Any], call_map["candle"])["tasks"] == [("deribit", "spot_ohlcv", "BTC", "1m")]
    assert cast(dict[str, Any], call_map["open_interest"])["open_interest_tasks"] == [("deribit", "BTC", "1m")]
    assert cast(dict[str, Any], call_map["funding"])["funding_tasks"] == [("deribit", "BTC", "1m")]
    assert cast(dict[str, Any], call_map["trade"])["trade_tasks"] == [("deribit", "perp", "BTC")]


def test_fetch_all_task_groups_skips_empty_groups() -> None:
    calls: list[str] = []

    def _fetch_stub(**kwargs: object) -> tuple[dict[tuple[str, str, str], list[int]], dict[tuple[str, str, str], str]]:
        del kwargs
        calls.append("called")
        return ({}, {})

    result: tuple[
        dict[tuple[str, str, str, str], list[int]],
        dict[tuple[str, str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
    ] = fetch_all_task_groups(
        candle_tasks=[],
        open_interest_tasks=[],
        funding_tasks=[],
        trade_tasks=None,
        lake_root="lake/bronze",
        candle_concurrency=1,
        open_interest_concurrency=1,
        funding_concurrency=1,
        trade_concurrency=1,
        logger=logging.getLogger("test_loader_execution_empty"),
        fetch_candles_fn=_fetch_stub,
        fetch_open_interest_fn=_fetch_stub,
        fetch_funding_fn=_fetch_stub,
        fetch_trades_fn=_fetch_stub,
    )

    assert calls == []
    assert result == ({}, {}, {}, {}, {}, {}, {}, {})


def test_fetch_all_task_groups_requires_volatility_fetcher_for_volatility_tasks() -> None:
    with pytest.raises(ValueError, match="fetch_volatility_fn is required"):
        fetch_all_task_groups(
            candle_tasks=[],
            open_interest_tasks=[],
            funding_tasks=[],
            volatility_tasks=[("deribit", "BTC", "1m")],
            trade_tasks=None,
            lake_root="lake/bronze",
            candle_concurrency=1,
            open_interest_concurrency=1,
            funding_concurrency=1,
            trade_concurrency=1,
            logger=logging.getLogger("test_loader_execution_volatility_missing"),
            fetch_candles_fn=lambda **kwargs: ({}, {}),
            fetch_open_interest_fn=lambda **kwargs: ({}, {}),
            fetch_funding_fn=lambda **kwargs: ({}, {}),
            fetch_trades_fn=lambda **kwargs: ({}, {}),
        )
