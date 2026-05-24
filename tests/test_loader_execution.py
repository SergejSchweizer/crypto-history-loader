"""Tests for Bronze loader execution orchestration helpers."""

from __future__ import annotations

import logging
from typing import Any, cast

from api.commands.loader_execution import fetch_all_task_groups


def test_fetch_all_task_groups_dispatches_all_task_kinds() -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def _fetch_candles_fn(
        **kwargs: object,
    ) -> tuple[dict[tuple[str, str, str, str], list[int]], dict[tuple[str, str, str, str], str]]:  # noqa: E501
        calls.append(("candle", dict(kwargs)))
        return ({("deribit", "spot", "BTC", "1m"): [1]}, {})

    def _fetch_oi_fn(**kwargs: object) -> tuple[dict[tuple[str, str, str], list[int]], dict[tuple[str, str, str], str]]:
        calls.append(("oi", dict(kwargs)))
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

    def _fetch_historical_volatility_fn(
        **kwargs: object,
    ) -> tuple[dict[tuple[str, str, str], list[int]], dict[tuple[str, str, str], str]]:
        calls.append(("historical_volatility", dict(kwargs)))
        return ({("deribit", "BTC", "1m"): [5]}, {})

    def _fetch_volatility_index_data_fn(
        **kwargs: object,
    ) -> tuple[dict[tuple[str, str, str], list[int]], dict[tuple[str, str, str], str]]:
        calls.append(("volatility_index_data", dict(kwargs)))
        return ({("deribit", "BTC", "1m"): [6]}, {})

    result: tuple[
        dict[tuple[str, str, str, str], list[int]],
        dict[tuple[str, str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
    ] = fetch_all_task_groups(
        candle_tasks=[("deribit", "spot", "BTC", "1m")],
        oi_tasks=[("deribit", "BTC", "1m")],
        funding_tasks=[("deribit", "BTC", "1m")],
        historical_volatility_tasks=[("deribit", "BTC", "1m")],
        volatility_index_data_tasks=[("deribit", "BTC", "1m")],
        trade_tasks=[("deribit", "perp", "BTC")],
        lake_root="lake/bronze",
        candle_concurrency=2,
        oi_concurrency=3,
        funding_concurrency=4,
        volatility_concurrency=4,
        trade_concurrency=5,
        logger=logging.getLogger("test_loader_execution_dispatch"),
        fetch_candles_fn=_fetch_candles_fn,
        fetch_oi_fn=_fetch_oi_fn,
        fetch_funding_fn=_fetch_funding_fn,
        fetch_historical_volatility_fn=_fetch_historical_volatility_fn,
        fetch_volatility_index_data_fn=_fetch_volatility_index_data_fn,
        fetch_trades_fn=_fetch_trades_fn,
    )

    assert result[0] == {("deribit", "spot", "BTC", "1m"): [1]}
    assert result[2] == {("deribit", "BTC", "1m"): [2]}
    assert result[4] == {("deribit", "BTC", "1m"): [3]}
    assert result[6] == {("deribit", "BTC", "1m"): [5]}
    assert result[8] == {("deribit", "BTC", "1m"): [6]}
    assert result[10] == {("deribit", "perp", "BTC"): [4]}

    call_map = {name: kwargs for name, kwargs in calls}
    assert cast(dict[str, Any], call_map["candle"])["tasks"] == [("deribit", "spot", "BTC", "1m")]
    assert cast(dict[str, Any], call_map["oi"])["oi_tasks"] == [("deribit", "BTC", "1m")]
    assert cast(dict[str, Any], call_map["funding"])["funding_tasks"] == [("deribit", "BTC", "1m")]
    assert cast(dict[str, Any], call_map["historical_volatility"])["volatility_tasks"] == [("deribit", "BTC", "1m")]
    assert cast(dict[str, Any], call_map["volatility_index_data"])["volatility_tasks"] == [("deribit", "BTC", "1m")]
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
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
        dict[tuple[str, str, str], list[int]],
        dict[tuple[str, str, str], str],
    ] = fetch_all_task_groups(
        candle_tasks=[],
        oi_tasks=[],
        funding_tasks=[],
        historical_volatility_tasks=[],
        volatility_index_data_tasks=[],
        trade_tasks=None,
        lake_root="lake/bronze",
        candle_concurrency=1,
        oi_concurrency=1,
        funding_concurrency=1,
        volatility_concurrency=1,
        trade_concurrency=1,
        logger=logging.getLogger("test_loader_execution_empty"),
        fetch_candles_fn=_fetch_stub,
        fetch_oi_fn=_fetch_stub,
        fetch_funding_fn=_fetch_stub,
        fetch_historical_volatility_fn=_fetch_stub,
        fetch_volatility_index_data_fn=_fetch_stub,
        fetch_trades_fn=_fetch_stub,
    )

    assert calls == []
    assert result == ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
