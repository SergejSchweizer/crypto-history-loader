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
