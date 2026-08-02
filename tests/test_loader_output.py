"""Tests for Bronze loader output persistence helpers."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from api.commands.loader_output import BronzeRunState, IncrementalPersistor
from application.dto import (
    BronzeFetchPlanDTO,
    FundingFetchTaskDTO,
    OpenInterestFetchTaskDTO,
    TradeFetchTaskDTO,
    VolatilityFetchTaskDTO,
)
from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot_ohlcv import SpotCandle
from ingestion.trades import TradeTick
from ingestion.volatility import VolatilityPoint


class _PersistResult:
    """Minimal persistence result used by output-helper tests."""

    parquet_files = ["lake/bronze/dataset_type=perps_trades/date=2026-04-27/data.parquet"]

    def to_output_dict(self) -> dict[str, object]:
        """Return a serializable result payload."""

        return {"parquet_files": self.parquet_files}


def test_bronze_run_state_initializes_output_and_tasks_from_plan() -> None:
    plan = BronzeFetchPlanDTO(
        exchanges=["deribit"],
        data_types=["spot_ohlcv", "open_interest", "funding", "volatility_index_data", "perps_trades"],
        ohlcv_markets=["spot_ohlcv"],
        symbols=["BTC"],
        perp_trade_symbols=["BTC"],
        options_trade_symbols=[],
        candle_tasks=[("deribit", "spot_ohlcv", "BTC", "1m")],
        open_interest_tasks=[("deribit", "BTC", "1m")],
        funding_tasks=[("deribit", "BTC", "1m")],
        volatility_index_data_tasks=[("deribit", "BTC", "1m")],
        trade_tasks=[("deribit", "perp", "BTC")],
    )

    state = BronzeRunState.from_plan(plan)

    assert state.output == {"deribit": {}}
    assert state.candle_tasks == [("deribit", "spot_ohlcv", "BTC", "1m")]
    assert state.open_interest_tasks == [("deribit", "BTC", "1m")]
    assert state.funding_tasks == [("deribit", "BTC", "1m")]
    assert state.volatility_index_data_tasks == [("deribit", "BTC", "1m")]
    assert state.trade_tasks == [("deribit", "perp", "BTC")]


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


def test_volatility_chunk_persists_and_marks_task_complete() -> None:
    """Volatility chunks are complete daily intervals and can checkpoint immediately."""

    checkpoint_marks: list[tuple[str, tuple[object, ...]]] = []
    persist_calls: list[dict[str, Any]] = []
    task = VolatilityFetchTaskDTO(
        exchange="deribit",
        symbol="BTC",
        timeframe="1m",
        dataset_type="volatility_index_data",
    )
    row = VolatilityPoint(
        exchange="deribit",
        symbol="BTC",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        value=51.5,
        source_endpoint="public_get_volatility_index_data",
        dataset_type="volatility_index",
    )

    def _persist_fn(**kwargs: Any) -> _PersistResult:
        persist_calls.append(kwargs)
        return _PersistResult()

    persistor = IncrementalPersistor(
        lake_root="lake/bronze",
        mark_checkpoint_complete=lambda dataset, key: checkpoint_marks.append((dataset, key)),
        persist_fn=_persist_fn,
    )

    persistor.on_volatility_index_data_task_chunk(task, [row], logging.getLogger("test"))

    assert len(persist_calls) == 1
    assert checkpoint_marks == [("volatility_index_data", ("deribit", "BTC", "1m"))]


def test_empty_volatility_chunk_skips_persistence() -> None:
    """Empty volatility chunks should not persist or checkpoint."""

    checkpoint_marks: list[tuple[str, tuple[object, ...]]] = []
    persist_calls: list[dict[str, Any]] = []
    task = VolatilityFetchTaskDTO(
        exchange="deribit",
        symbol="BTC",
        timeframe="1m",
        dataset_type="volatility_index_data",
    )

    def _persist_fn(**kwargs: Any) -> _PersistResult:
        persist_calls.append(kwargs)
        return _PersistResult()

    persistor = IncrementalPersistor(
        lake_root="lake/bronze",
        mark_checkpoint_complete=lambda dataset, key: checkpoint_marks.append((dataset, key)),
        persist_fn=_persist_fn,
    )

    persistor.on_volatility_index_data_task_chunk(task, [], logging.getLogger("test"))
    persistor._persist_volatility_task(task, [], logging.getLogger("test"))  # pyright: ignore[reportPrivateUsage] - covers the empty internal persist guard.

    assert persist_calls == []
    assert checkpoint_marks == []


def test_incremental_persistor_persists_each_market_family_and_logs_once() -> None:
    """All incremental source families should use their dataset-specific storage DTO."""

    calls: list[dict[str, Any]] = []
    checkpoints: list[tuple[str, tuple[object, ...]]] = []

    def _persist_fn(**kwargs: Any) -> _PersistResult:
        calls.append(kwargs)
        return _PersistResult()

    persistor = IncrementalPersistor(
        lake_root="lake/bronze",
        mark_checkpoint_complete=lambda dataset, key: checkpoints.append((dataset, key)),
        persist_fn=_persist_fn,
    )
    logger = logging.getLogger("persistor-family-test")
    timestamp = datetime(2026, 4, 27, 10, 0, tzinfo=UTC)
    candle = SpotCandle(
        exchange="deribit",
        symbol="BTC",
        interval="1m",
        open_time=timestamp,
        close_time=timestamp,
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=1.0,
        quote_volume=100.0,
        trade_count=1,
    )
    oi = OpenInterestPoint("deribit", "BTC", "1m", timestamp, timestamp, 10.0, 1000.0)
    funding = FundingPoint("deribit", "BTC", "8h", timestamp, timestamp, 0.001, 100.0, 101.0)
    candle_task = type(
        "CandleTask", (), {"exchange": "deribit", "market": "spot_ohlcv", "symbol": "BTC", "timeframe": "1m"}
    )()
    persistor.on_candle_task_complete(candle_task, [candle], logger)
    persistor.on_open_interest_task_complete(OpenInterestFetchTaskDTO("deribit", "BTC", "1m"), [oi], logger)
    persistor.on_funding_task_complete(FundingFetchTaskDTO("deribit", "BTC", "8h"), [funding], logger)
    persistor.on_trade_task_complete(
        TradeFetchTaskDTO("deribit", "option", "BTC"),
        [
            TradeTick(
                exchange="deribit",
                symbol="BTC",
                instrument_type="option",
                trade_id="option-1",
                trade_time=timestamp,
                price=1.0,
                quantity=1.0,
                side="buy",
                is_maker=False,
                source_endpoint="public_get_last_trades_by_currency",
            )
        ],
        logger,
    )
    assert len(calls) == 4
    assert checkpoints == []
    assert all(call["options"].save_parquet_lake is True for call in calls)
