"""Tests for bronze loader output persistence helpers."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from api.commands.loader_output import IncrementalPersistor, finalize_bronze_output
from application.dto import (
    CandleFetchTaskDTO,
    FundingFetchTaskDTO,
    LoaderStorageDTO,
    OpenInterestFetchTaskDTO,
    PersistOptionsDTO,
    PersistResultDTO,
    TradeFetchTaskDTO,
    VolatilityFetchTaskDTO,
)
from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle
from ingestion.trades import TradeTick
from ingestion.volatility import VolatilityPoint


def _spot_candle(symbol: str = "BTC") -> SpotCandle:
    return SpotCandle(
        exchange="deribit",
        symbol=symbol,
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=1.0,
        high_price=1.1,
        low_price=0.9,
        close_price=1.0,
        volume=1.0,
        quote_volume=1.0,
        trade_count=1,
    )


def _oi_point(symbol: str = "BTC") -> OpenInterestPoint:
    return OpenInterestPoint(
        exchange="deribit",
        symbol=symbol,
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_interest=1000.0,
        open_interest_value=0.0,
    )


def _funding_point(symbol: str = "BTC") -> FundingPoint:
    return FundingPoint(
        exchange="deribit",
        symbol=symbol,
        interval="8h",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 7, 59, 59, 999000, tzinfo=UTC),
        funding_rate=0.001,
        index_price=100.0,
        mark_price=101.0,
    )


def _trade_tick(symbol: str = "BTC") -> TradeTick:
    return TradeTick(
        exchange="deribit",
        symbol=symbol,
        instrument_type="perp",
        trade_id="t1",
        trade_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        price=100.0,
        quantity=1.0,
        side="buy",
        is_maker=True,
        source_endpoint="public_trades",
    )


def _vol_point(symbol: str = "BTC", dataset_type: str = "historical_volatility") -> VolatilityPoint:
    return VolatilityPoint(
        exchange="deribit",
        symbol=symbol,
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        value=55.0,
        source_endpoint="public_get_historical_volatility",
        dataset_type=dataset_type,  # type: ignore[arg-type]
    )


def test_incremental_persistor_marks_checkpoints_and_deduplicates_streamed_tasks() -> None:
    checkpoint_marks: list[tuple[str, tuple[object, ...]]] = []
    persist_calls: list[dict[str, Any]] = []

    def _persist_fn(**kwargs: object) -> PersistResultDTO:
        persist_calls.append(dict(kwargs))
        return PersistResultDTO(parquet_files=["lake/bronze/dataset_type=spot/date=2026-05-01/data.parquet"])

    persistor = IncrementalPersistor(
        lake_root="lake/bronze",
        mark_checkpoint_complete=lambda kind, key: checkpoint_marks.append((kind, key)),
        persist_fn=_persist_fn,
    )

    logger = logging.getLogger("test_loader_output_stream")
    candle_task = CandleFetchTaskDTO(exchange="deribit", market="spot", symbol="BTC", timeframe="1m")
    persistor.on_candle_task_chunk(candle_task, [_spot_candle()], logger)
    persistor.on_candle_task_complete(candle_task, [_spot_candle()], logger)

    oi_task = OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="1m")
    persistor.on_oi_task_chunk(oi_task, [_oi_point()], logger)
    persistor.on_oi_task_complete(oi_task, [_oi_point()], logger)

    funding_task = FundingFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="8h")
    persistor.on_funding_task_chunk(funding_task, [_funding_point()], logger)
    persistor.on_funding_task_complete(funding_task, [_funding_point()], logger)

    trade_task = TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="BTC")
    persistor.on_trade_task_chunk(trade_task, [_trade_tick()], logger)
    persistor.on_trade_task_complete(trade_task, [_trade_tick()], logger)

    assert checkpoint_marks == [
        ("candle", ("deribit", "spot", "BTC", "1m")),
        ("oi", ("deribit", "BTC", "1m")),
        ("funding", ("deribit", "BTC", "8h")),
        ("trade", ("deribit", "perp", "BTC")),
    ]
    assert len(persist_calls) == 4


def test_incremental_persistor_persists_volatility_variants_with_expected_options() -> None:
    persist_calls: list[dict[str, Any]] = []

    def _persist_fn(**kwargs: object) -> PersistResultDTO:
        persist_calls.append(dict(kwargs))
        return PersistResultDTO(
            parquet_files=["lake/bronze/dataset_type=historical_volatility/date=2026-05-01/data.parquet"]
        )

    persistor = IncrementalPersistor(
        lake_root="lake/bronze",
        mark_checkpoint_complete=lambda _kind, _key: None,
        persist_fn=_persist_fn,
    )
    logger = logging.getLogger("test_loader_output_vol")

    hv_task = VolatilityFetchTaskDTO(
        exchange="deribit", symbol="BTC", timeframe="1m", dataset_type="historical_volatility"
    )
    vi_task = VolatilityFetchTaskDTO(
        exchange="deribit", symbol="BTC", timeframe="1m", dataset_type="volatility_index_data"
    )

    persistor.on_historical_volatility_task_chunk(hv_task, [_vol_point(dataset_type="historical_volatility")], logger)
    persistor.on_volatility_index_data_task_chunk(vi_task, [_vol_point(dataset_type="volatility_index")], logger)

    hv_options = persist_calls[0]["options"]
    vi_options = persist_calls[1]["options"]
    assert hv_options.historical_volatility_requested is True
    assert hv_options.volatility_index_data_requested is False
    assert vi_options.historical_volatility_requested is False
    assert vi_options.volatility_index_data_requested is True


def test_finalize_bronze_output_writes_sidecars_and_trade_summary() -> None:
    output: dict[str, object] = {}
    logger = logging.getLogger("test_loader_output_finalize")

    t = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    candle = _spot_candle()
    oi = _oi_point()
    funding = _funding_point()
    trade = _trade_tick()
    hv = _vol_point(dataset_type="historical_volatility")
    vi = _vol_point(dataset_type="volatility_index")

    candle_task = ("deribit", "spot", "BTC", "1m")
    vol_task = ("deribit", "BTC", "1m")
    trade_task = ("deribit", "perp", "BTC")

    def _persist_fn(**kwargs: object) -> PersistResultDTO:
        storage = kwargs["storage"]
        options = kwargs["options"]
        assert isinstance(storage, LoaderStorageDTO)
        assert isinstance(options, PersistOptionsDTO)
        return PersistResultDTO(
            parquet_files=[
                "lake/bronze/dataset_type=perp_trades/date=2026-05-01/data.parquet",
                "lake/bronze/dataset_type=historical_volatility/date=2026-05-01/data.parquet",
            ]
        )

    finalize_bronze_output(
        logger=logger,
        output=output,
        tasks=[candle_task],
        oi_tasks=[vol_task],
        funding_tasks=[vol_task],
        historical_volatility_tasks=[vol_task],
        volatility_index_data_tasks=[vol_task],
        trade_tasks=[trade_task],
        task_results={candle_task: [candle]},
        task_errors={},
        oi_results={vol_task: [oi]},
        oi_errors={},
        funding_results={vol_task: [funding]},
        funding_errors={},
        historical_volatility_results={vol_task: [hv]},
        historical_volatility_errors={},
        volatility_index_data_results={vol_task: [vi]},
        volatility_index_data_errors={},
        trade_results={trade_task: [trade]},
        trade_errors={trade_task: "timeout"},
        multi_market=True,
        oi_requested=True,
        funding_requested=True,
        historical_volatility_requested=True,
        volatility_index_data_requested=True,
        perp_trades_requested=True,
        option_trades_requested=False,
        candles_for_storage={"spot": {"deribit": {"BTC": [candle]}}},
        open_interest_for_storage={"perp": {"deribit": {"BTC": [oi]}}},
        funding_for_storage={"perp": {"deribit": {"BTC": [funding]}}},
        historical_volatility_for_storage={"perp": {"deribit": {"BTC": [hv]}}},
        volatility_index_data_for_storage={"perp": {"deribit": {"BTC": [vi]}}},
        trades_for_storage={"perp": {"deribit": {"BTC": [trade]}}},
        ohlcv_markets=["spot"],
        args=SimpleNamespace(save_parquet_lake=True, lake_root="lake/bronze"),
        incremental_parquet_on_fetch=False,
        incremental_parquet_files=[],
        oi_dataset_type="oi",
        sidecar_path_list_fn=lambda paths, suffix: [f"{path}{suffix}" for path in paths],
        ensure_bronze_sidecars_fn=lambda **kwargs: [
            "lake/bronze/dataset_type=option_trades/date=2026-05-01/data.parquet"
        ],
        populate_ohlcv_output_fn=lambda **kwargs: None,
        populate_oi_output_fn=lambda **kwargs: None,
        populate_funding_output_fn=lambda **kwargs: None,
        populate_volatility_output_fn=lambda **kwargs: None,
        populate_trades_output_fn=lambda **kwargs: None,
        symbol_progress_rows_fn=lambda **kwargs: [{"symbol": "BTC", "completed": 1, "scheduled": 1}],
        fairness_rows=None,
        trade_error_breakdown_fn=lambda errors: {
            "total": len(errors),
            "net_unreachable": 0,
            "net_timeout": 1,
            "other": 0,
        },
        candle_serializer=lambda _c: {"ts": t.isoformat()},
        persist_fn=_persist_fn,
    )

    assert "_parquet_files" in output
    assert "_manifest_files" in output
    assert "_plot_files" in output
    assert output["_trade_error_breakdown"] == {
        "total": 1,
        "net_unreachable": 0,
        "net_timeout": 1,
        "other": 0,
    }


def test_finalize_bronze_output_handles_parquet_errors_and_option_trades() -> None:
    output: dict[str, object] = {}
    logger = logging.getLogger("test_loader_output_finalize_error")

    hv_task = VolatilityFetchTaskDTO(
        exchange="deribit", symbol="BTC", timeframe="1m", dataset_type="historical_volatility"
    )
    vi_task = VolatilityFetchTaskDTO(
        exchange="deribit", symbol="BTC", timeframe="1m", dataset_type="volatility_index_data"
    )

    persistor = IncrementalPersistor(
        lake_root="lake/bronze",
        mark_checkpoint_complete=lambda _kind, _key: None,
        persist_fn=lambda **_kwargs: PersistResultDTO(
            parquet_files=["lake/bronze/dataset_type=option_trades/date=2026-05-01/data.parquet"]
        ),
    )
    persistor.on_historical_volatility_task_complete(
        hv_task,
        [_vol_point(dataset_type="historical_volatility")],
        logger,
    )
    persistor.on_volatility_index_data_task_complete(
        vi_task,
        [_vol_point(dataset_type="volatility_index")],
        logger,
    )

    def _persist_raises(**_kwargs: object) -> PersistResultDTO:
        raise RuntimeError("disk_full")

    finalize_bronze_output(
        logger=logger,
        output=output,
        tasks=[],
        oi_tasks=[],
        funding_tasks=[],
        historical_volatility_tasks=[],
        volatility_index_data_tasks=[],
        trade_tasks=[("deribit", "option", "BTC")],
        task_results={},
        task_errors={},
        oi_results={},
        oi_errors={},
        funding_results={},
        funding_errors={},
        historical_volatility_results={},
        historical_volatility_errors={},
        volatility_index_data_results={},
        volatility_index_data_errors={},
        trade_results={},
        trade_errors={("deribit", "option", "BTC"): "network"},
        multi_market=True,
        oi_requested=False,
        funding_requested=False,
        historical_volatility_requested=False,
        volatility_index_data_requested=False,
        perp_trades_requested=False,
        option_trades_requested=True,
        candles_for_storage={},
        open_interest_for_storage={},
        funding_for_storage={},
        historical_volatility_for_storage={},
        volatility_index_data_for_storage={},
        trades_for_storage={},
        ohlcv_markets=[],
        args=SimpleNamespace(save_parquet_lake=True, lake_root="lake/bronze"),
        incremental_parquet_on_fetch=False,
        incremental_parquet_files=[],
        oi_dataset_type="oi",
        sidecar_path_list_fn=lambda paths, suffix: [f"{path}{suffix}" for path in paths],
        ensure_bronze_sidecars_fn=lambda **_kwargs: [
            "lake/bronze/dataset_type=option_trades/date=2026-05-01/data.parquet"
        ],
        populate_ohlcv_output_fn=lambda **_kwargs: None,
        populate_oi_output_fn=lambda **_kwargs: None,
        populate_funding_output_fn=lambda **_kwargs: None,
        populate_volatility_output_fn=lambda **_kwargs: None,
        populate_trades_output_fn=lambda **_kwargs: None,
        symbol_progress_rows_fn=lambda **_kwargs: [],
        fairness_rows=[],
        trade_error_breakdown_fn=lambda errors: {
            "total": len(errors),
            "net_unreachable": 1,
            "net_timeout": 0,
            "other": 0,
        },
        candle_serializer=lambda _c: {},
        persist_fn=_persist_raises,
    )

    assert output["_parquet_error"] == "disk_full"
    assert "_manifest_files" in output
    assert output["_trade_error_breakdown"] == {
        "total": 1,
        "net_unreachable": 1,
        "net_timeout": 0,
        "other": 0,
    }


def test_incremental_persistor_complete_paths_and_empty_chunk_noops() -> None:
    persist_calls: list[dict[str, Any]] = []

    def _persist_fn(**kwargs: object) -> PersistResultDTO:
        persist_calls.append(dict(kwargs))
        return PersistResultDTO(parquet_files=["lake/bronze/dataset_type=spot/date=2026-05-01/data.parquet"])

    persistor = IncrementalPersistor(
        lake_root="lake/bronze",
        mark_checkpoint_complete=lambda _kind, _key: None,
        persist_fn=_persist_fn,
    )
    logger = logging.getLogger("test_loader_output_complete")

    persistor.on_oi_task_complete(
        OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="1m"), [_oi_point()], logger
    )
    persistor.on_funding_task_complete(
        FundingFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="8h"), [_funding_point()], logger
    )
    persistor.on_trade_task_complete(
        TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="BTC"), [_trade_tick()], logger
    )
    persistor.on_historical_volatility_task_complete(
        VolatilityFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="1m", dataset_type="historical_volatility"),
        [_vol_point(dataset_type="historical_volatility")],
        logger,
    )
    persistor.on_volatility_index_data_task_complete(
        VolatilityFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="1m", dataset_type="volatility_index_data"),
        [_vol_point(dataset_type="volatility_index")],
        logger,
    )

    # Empty chunks must not persist or mark checkpoints.
    persistor.on_candle_task_chunk(
        CandleFetchTaskDTO(exchange="deribit", market="spot", symbol="BTC", timeframe="1m"), [], logger
    )
    persistor.on_oi_task_chunk(OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="1m"), [], logger)
    persistor.on_funding_task_chunk(FundingFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="8h"), [], logger)
    persistor.on_trade_task_chunk(TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="BTC"), [], logger)
    persistor.on_historical_volatility_task_chunk(
        VolatilityFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="1m", dataset_type="historical_volatility"),
        [],
        logger,
    )
    persistor.on_volatility_index_data_task_chunk(
        VolatilityFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="1m", dataset_type="volatility_index_data"),
        [],
        logger,
    )

    assert len(persist_calls) == 5
