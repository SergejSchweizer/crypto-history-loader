"""Tests for CLI-to-service parallel task wrapper contracts."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

from api.commands import loader


def test_parallel_task_wrappers_convert_tuples_to_dtos_and_forward_callbacks(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Each dataset wrapper must preserve task fields and shared orchestration callbacks."""

    captured: dict[str, dict[str, object]] = {}

    def _stub(name: str) -> Any:
        def _run(**kwargs: object) -> SimpleNamespace:
            captured[name] = kwargs
            return SimpleNamespace(rows={name: []}, errors={})

        return _run

    monkeypatch.setattr(loader, "fetch_candle_tasks_parallel", _stub("candles"))
    monkeypatch.setattr(loader, "fetch_open_interest_tasks_parallel", _stub("open_interest"))
    monkeypatch.setattr(loader, "fetch_funding_tasks_parallel", _stub("funding"))
    monkeypatch.setattr(loader, "fetch_volatility_tasks_parallel", _stub("volatility"))
    monkeypatch.setattr(loader, "fetch_trade_tasks_parallel", _stub("trades"))
    logger = logging.getLogger("test")

    def callback(*_args: object) -> None:
        return None

    assert loader._fetch_candle_tasks_parallel(
        [("deribit", "perp", "BTC", "1m")], "lake", 2, logger, on_task_complete=callback
    ) == ({"candles": []}, {})
    assert loader._fetch_open_interest_tasks_parallel(
        [("deribit", "BTC", "1m")], "lake", 2, logger, on_task_chunk=callback
    ) == ({"open_interest": []}, {})
    assert loader._fetch_funding_tasks_parallel([("deribit", "BTC", "8h")], "lake", 2, logger) == ({"funding": []}, {})
    assert loader._fetch_volatility_index_data_tasks_parallel([("deribit", "BTC", "1m")], "lake", 2, logger) == (
        {"volatility": []},
        {},
    )
    assert loader._fetch_trade_tasks_parallel([("deribit", "perp", "BTC")], "lake", 2, logger) == ({"trades": []}, {})

    assert captured["candles"]["tasks"][0].market == "perp"
    assert captured["open_interest"]["tasks"][0].symbol == "BTC"
    assert captured["funding"]["tasks"][0].timeframe == "8h"
    assert captured["volatility"]["tasks"][0].dataset_type == "volatility_index_data"
    assert captured["trades"]["tasks"][0].market == "perp"


def test_symbol_fetch_wrappers_forward_runtime_dependencies_and_chunks(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Source-family wrappers keep their explicit service-boundary arguments intact."""

    dependencies = object()
    runtime_context = object()
    captured: dict[str, dict[str, object]] = {}

    def _fetcher(name: str) -> Any:
        def _run(**kwargs: object) -> list[object]:
            captured[name] = kwargs
            return []

        return _run

    monkeypatch.setattr(loader, "_symbol_fetch_dependencies", lambda: dependencies)
    monkeypatch.setattr(loader, "_current_runtime_bounds_context", lambda: runtime_context)
    monkeypatch.setattr(loader._loader_fetchers, "fetch_symbol_candles", _fetcher("candles"))
    monkeypatch.setattr(loader._loader_fetchers, "fetch_symbol_open_interest", _fetcher("open_interest"))
    monkeypatch.setattr(loader._loader_fetchers, "fetch_symbol_funding", _fetcher("funding"))
    monkeypatch.setattr(loader._loader_fetchers, "fetch_symbol_volatility_index_data", _fetcher("volatility"))
    monkeypatch.setattr(loader._loader_fetchers, "fetch_symbol_trades", _fetcher("trades"))

    def callback(*_args: object) -> None:
        return None

    assert loader._fetch_symbol_candles("deribit", "perp", "BTC", "1m", "lake", callback) == []
    assert loader._fetch_symbol_open_interest("deribit", "perp", "BTC", "1m", "lake", callback) == []
    assert loader._fetch_symbol_funding("deribit", "perp", "BTC", "8h", "lake", callback) == []
    assert loader._fetch_symbol_volatility_index_data("deribit", "perp", "BTC", "1m", "lake", callback) == []
    assert loader._fetch_symbol_trades("deribit", "perp", "BTC", "lake", callback) == []

    for kwargs in captured.values():
        assert kwargs["dependencies"] is dependencies
        assert kwargs["runtime_context"] is runtime_context
        assert kwargs["lake_root"] == "lake"
        assert kwargs["on_history_chunk"] is callback
