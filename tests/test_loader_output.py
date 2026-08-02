"""Tests for Bronze loader output persistence helpers."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from api.commands.loader_output import BronzeRunState, IncrementalPersistor, finalize_bronze_output
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


def test_finalize_bronze_output_populates_requested_outputs_and_sidecars() -> None:
    """Finalization should persist, enrich output, and report trade failures in one pass."""

    from types import SimpleNamespace

    calls: list[str] = []
    output: dict[str, object] = {}
    parquet_path = "lake/dataset_type=perps_trades/date=2026-04-27/data.parquet"

    def populate(name: str) -> Callable[..., None]:
        def _populate(**kwargs: object) -> None:
            calls.append(name)
            assert kwargs["output"] is output

        return _populate

    def persist(**kwargs: object) -> _PersistResult:
        assert cast(Any, kwargs["options"]).save_parquet_lake is True
        return _PersistResult()

    finalize_bronze_output(
        logger=logging.getLogger("finalize-test"),
        output=output,
        tasks=[],
        open_interest_tasks=[],
        funding_tasks=[],
        volatility_index_data_tasks=[],
        trade_tasks=[("deribit", "perp", "BTC")],
        task_results={},
        task_errors={},
        open_interest_results={},
        open_interest_errors={},
        funding_results={},
        funding_errors={},
        volatility_index_data_results={},
        volatility_index_data_errors={},
        trade_results={},
        trade_errors={("deribit", "perp", "BTC"): "timeout"},
        multi_market=False,
        open_interest_requested=True,
        funding_requested=True,
        volatility_index_data_requested=True,
        perps_trades_requested=True,
        options_trades_requested=False,
        candles_for_storage={},
        open_interest_for_storage={},
        funding_for_storage={},
        volatility_index_data_for_storage={},
        trades_for_storage={},
        ohlcv_markets=["spot_ohlcv", "perp"],
        args=SimpleNamespace(save_parquet_lake=True, lake_root="lake/bronze"),
        incremental_parquet_on_fetch=False,
        incremental_parquet_files=[],
        open_interest_dataset_type="open_interest",
        sidecar_path_list_fn=lambda paths, suffix: [f"{path}{suffix}" for path in paths],
        ensure_bronze_sidecars_fn=lambda **kwargs: [parquet_path],
        populate_ohlcv_output_fn=populate("ohlcv"),
        populate_open_interest_output_fn=populate("open_interest"),
        populate_funding_output_fn=populate("funding"),
        populate_volatility_output_fn=populate("volatility"),
        populate_trades_output_fn=populate("trades"),
        symbol_progress_rows_fn=lambda **kwargs: [],
        fairness_rows=None,
        trade_error_breakdown_fn=lambda errors: {
            "total": len(errors),
            "net_unreachable": 0,
            "net_timeout": 1,
            "other": 0,
        },
        candle_serializer=lambda candle: {"open_time": candle.open_time},
        persist_fn=persist,
    )

    assert calls == ["ohlcv", "open_interest", "funding", "trades"]
    assert output["parquet_files"] == _PersistResult.parquet_files
    assert output["_manifest_files"] == [f"{parquet_path}.json"]
    assert output["_trade_error_breakdown"] == {"total": 1, "net_unreachable": 0, "net_timeout": 1, "other": 0}


def test_incremental_persistor_ignores_empty_and_completed_task_results() -> None:
    """Empty chunks and already streamed tasks must never create duplicate parquet writes."""

    persistor = IncrementalPersistor(
        lake_root="lake",
        mark_checkpoint_complete=lambda _dataset, _key: None,
        persist_fn=lambda **_kwargs: pytest.fail("persistence should not run"),
    )
    logger = logging.getLogger("empty-persistor-test")
    candle_task = type(
        "CandleTask", (), {"exchange": "deribit", "market": "spot_ohlcv", "symbol": "BTC", "timeframe": "1m"}
    )()
    open_interest_task = OpenInterestFetchTaskDTO("deribit", "BTC", "1m")
    funding_task = FundingFetchTaskDTO("deribit", "BTC", "8h")
    trade_task = TradeFetchTaskDTO("deribit", "perp", "BTC")

    persistor.on_candle_task_chunk(candle_task, [], logger)
    persistor.on_open_interest_task_chunk(open_interest_task, [], logger)
    persistor.on_funding_task_chunk(funding_task, [], logger)
    persistor.on_trade_task_chunk(trade_task, [], logger)
    persistor.streamed_candle_tasks.add(("deribit", "spot_ohlcv", "BTC", "1m"))
    persistor.streamed_open_interest_tasks.add(("deribit", "BTC", "1m"))
    persistor.streamed_funding_tasks.add(("deribit", "BTC", "8h"))
    persistor.streamed_trade_tasks.add(("deribit", "perp", "BTC"))
    persistor.on_candle_task_complete(candle_task, [], logger)
    persistor.on_open_interest_task_complete(open_interest_task, [], logger)
    persistor.on_funding_task_complete(funding_task, [], logger)
    persistor.on_trade_task_complete(trade_task, [], logger)


def test_finalize_bronze_output_records_parquet_write_failure() -> None:
    """A parquet failure is surfaced in output while fetch results remain available."""

    from types import SimpleNamespace

    output: dict[str, object] = {}
    finalize_bronze_output(
        logger=logging.getLogger("finalize-failure-test"),
        output=output,
        tasks=[],
        open_interest_tasks=[],
        funding_tasks=[],
        volatility_index_data_tasks=[],
        trade_tasks=[],
        task_results={},
        task_errors={},
        open_interest_results={},
        open_interest_errors={},
        funding_results={},
        funding_errors={},
        volatility_index_data_results={},
        volatility_index_data_errors={},
        trade_results={},
        trade_errors={},
        multi_market=False,
        open_interest_requested=False,
        funding_requested=False,
        volatility_index_data_requested=False,
        perps_trades_requested=False,
        options_trades_requested=False,
        candles_for_storage={},
        open_interest_for_storage={},
        funding_for_storage={},
        volatility_index_data_for_storage={},
        trades_for_storage={},
        ohlcv_markets=[],
        args=SimpleNamespace(save_parquet_lake=True, lake_root="lake"),
        incremental_parquet_on_fetch=False,
        incremental_parquet_files=[],
        open_interest_dataset_type="open_interest",
        sidecar_path_list_fn=lambda _paths, _suffix: [],
        ensure_bronze_sidecars_fn=lambda **_kwargs: [],
        populate_ohlcv_output_fn=lambda **_kwargs: None,
        populate_open_interest_output_fn=lambda **_kwargs: None,
        populate_funding_output_fn=lambda **_kwargs: None,
        populate_volatility_output_fn=lambda **_kwargs: None,
        populate_trades_output_fn=lambda **_kwargs: None,
        symbol_progress_rows_fn=lambda **_kwargs: [],
        fairness_rows=[],
        trade_error_breakdown_fn=lambda _errors: {"total": 0, "net_unreachable": 0, "net_timeout": 0, "other": 0},
        candle_serializer=lambda _candle: {},
        persist_fn=lambda **_kwargs: (_ for _ in ()).throw(OSError("disk full")),
    )
    assert output["_parquet_error"] == "disk full"
