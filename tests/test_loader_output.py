"""Tests for Bronze loader output persistence helpers."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from api.commands.loader_output import IncrementalPersistor
from application.dto import TradeFetchTaskDTO
from ingestion.trades import TradeTick


class _PersistResult:
    """Minimal persistence result used by output-helper tests."""

    parquet_files = ["lake/bronze/dataset_type=perp_trades/date=2026-04-27/data.parquet"]

    def to_output_dict(self) -> dict[str, object]:
        """Return a serializable result payload."""

        return {"parquet_files": self.parquet_files}


def test_trade_chunk_persists_without_marking_full_task_complete() -> None:
    """Trade chunk persistence must not checkpoint a task before all windows finish."""

    checkpoint_marks: list[tuple[str, tuple[object, ...]]] = []
    persist_calls: list[dict[str, Any]] = []
    task = TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="BTC")
    row = TradeTick(
        exchange="deribit",
        symbol="BTC",
        instrument_type="perp",
        trade_id="t-1",
        trade_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        price=100.0,
        quantity=1.0,
        side="buy",
        is_maker=False,
        source_endpoint="public_trades",
    )

    def _persist_fn(**kwargs: Any) -> _PersistResult:
        persist_calls.append(kwargs)
        return _PersistResult()

    persistor = IncrementalPersistor(
        lake_root="lake/bronze",
        mark_checkpoint_complete=lambda dataset, key: checkpoint_marks.append((dataset, key)),
        persist_fn=_persist_fn,
    )

    persistor.on_trade_task_chunk(task, [row], logging.getLogger("test"))
    persistor.on_trade_task_complete(task, [row], logging.getLogger("test"))

    assert len(persist_calls) == 1
    assert checkpoint_marks == []
