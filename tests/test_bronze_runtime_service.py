"""Tests for bronze runtime service helpers."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from application.datasets import DatasetTask
from application.dto import BronzeFetchPlanDTO
from application.services import bronze_runtime_service as runtime


def _plan() -> BronzeFetchPlanDTO:
    return BronzeFetchPlanDTO(
        exchanges=["deribit"],
        data_types=["spot_ohlcv"],
        ohlcv_markets=["spot_ohlcv"],
        symbols=["BTC"],
        perp_trade_symbols=["BTC"],
        options_trade_symbols=["BTC"],
        candle_tasks=[("deribit", "spot_ohlcv", "BTC", "1m")],
        open_interest_tasks=[],
        funding_tasks=[],
        volatility_index_data_tasks=[],
        trade_tasks=[],
    )


def _registry_plan() -> BronzeFetchPlanDTO:
    return BronzeFetchPlanDTO(
        exchanges=["deribit"],
        data_types=["funding", "open_interest", "perps_trades", "spot_ohlcv", "volatility_index_data"],
        ohlcv_markets=["spot_ohlcv"],
        symbols=["BTC"],
        perp_trade_symbols=["BTC"],
        options_trade_symbols=["BTC"],
        candle_tasks=[("deribit", "spot_ohlcv", "BTC", "1m")],
        open_interest_tasks=[("deribit", "BTC", "1m")],
        funding_tasks=[("deribit", "BTC", "1m")],
        volatility_index_data_tasks=[("deribit", "BTC", "1m")],
        trade_tasks=[("deribit", "perp", "BTC")],
        dataset_tasks=[
            DatasetTask("deribit", "spot_ohlcv", "spot_ohlcv", "BTC", "1m", "spot_ohlcv"),
            DatasetTask("deribit", "open_interest", "perp", "BTC", "1m", "perp"),
            DatasetTask("deribit", "funding", "perp", "BTC", "1m", "perp"),
            DatasetTask("deribit", "volatility_index_data", "perp", "BTC", "1m", "perp"),
            DatasetTask("deribit", "perps_trades", "perp", "BTC", "tick", "perp"),
        ],
    )


def _serialize(parts: tuple[object, ...]) -> str:
    return "|".join(str(item) for item in parts)


def test_load_checkpoint_handles_unreadable_stale_and_missing_completed(tmp_path: Path) -> None:
    path = tmp_path / "chk.json"
    logger = logging.getLogger("test")

    path.write_text("{", encoding="utf-8")
    unreadable = runtime.load_bronze_checkpoint(path, "fp", logger)
    assert unreadable == {
        "candle": set(),
        "open_interest": set(),
        "funding": set(),
        "volatility_index_data": set(),
        "trade": set(),
    }
    unreadable["candle"].add("mutated")
    assert runtime.load_bronze_checkpoint(tmp_path / "missing.json", "fp", logger)["candle"] == set()

    path.write_text(json.dumps({"fingerprint": "other", "completed": {}}), encoding="utf-8")
    assert runtime.load_bronze_checkpoint(path, "fp", logger) == {
        "candle": set(),
        "open_interest": set(),
        "funding": set(),
        "volatility_index_data": set(),
        "trade": set(),
    }

    path.write_text(json.dumps({"fingerprint": "fp", "completed": []}), encoding="utf-8")
    assert runtime.load_bronze_checkpoint(path, "fp", logger) == {
        "candle": set(),
        "open_interest": set(),
        "funding": set(),
        "volatility_index_data": set(),
        "trade": set(),
    }


def test_build_bronze_execution_policy_preserves_bounded_concurrency() -> None:
    policy = runtime.build_bronze_execution_policy(configured_concurrency=3)

    assert policy.effective_concurrency == 3
    assert policy.candle_concurrency == 3
    assert policy.open_interest_concurrency == 3
    assert policy.funding_concurrency == 3
    assert policy.trade_concurrency == 3


def test_build_bronze_runtime_bounds_context_parses_start_bounds() -> None:
    context = runtime.build_bronze_runtime_bounds_context(
        tail_delta_only=True,
        start_date="2023-04-24",
        symbol_start_dates=["BTCUSDT=2023-04-25"],
        exchange_symbol_start_dates=["DERIBIT:SOL=2024-02-27"],
        logger=logging.getLogger("test"),
    )

    assert context.tail_delta_only is True
    assert context.global_start_open_ms == runtime.parse_start_date_to_open_ms("2023-04-24")
    assert context.symbol_start_open_ms["BTC"] == runtime.parse_start_date_to_open_ms("2023-04-25")
    assert context.exchange_symbol_start_open_ms["deribit:SOL"] == runtime.parse_start_date_to_open_ms("2024-02-27")


def test_bronze_runtime_adapter_defaults_to_disabled_bounds() -> None:
    adapter = runtime.BronzeRuntimeAdapter()

    assert adapter.context == runtime.BronzeRuntimeBoundsContext(
        tail_delta_only=False,
        global_start_open_ms=None,
        symbol_start_open_ms={},
        exchange_symbol_start_open_ms={},
    )


def test_bronze_runtime_adapter_configure_rebuilds_and_stores_context() -> None:
    adapter = runtime.BronzeRuntimeAdapter()

    returned = adapter.configure(
        tail_delta_only=True,
        start_date="2023-04-24",
        symbol_start_dates=["BTCUSDT=2023-04-25"],
        exchange_symbol_start_dates=["DERIBIT:SOL=2024-02-27"],
        logger=logging.getLogger("test"),
    )

    assert returned is adapter.context
    assert adapter.context.tail_delta_only is True
    assert adapter.context.global_start_open_ms == runtime.parse_start_date_to_open_ms("2023-04-24")
    assert adapter.context.symbol_start_open_ms["BTC"] == runtime.parse_start_date_to_open_ms("2023-04-25")
    assert adapter.context.exchange_symbol_start_open_ms["deribit:SOL"] == runtime.parse_start_date_to_open_ms(
        "2024-02-27"
    )


def test_bronze_runtime_adapter_context_setter_replaces_active_context() -> None:
    adapter = runtime.BronzeRuntimeAdapter()
    replacement = runtime.BronzeRuntimeBoundsContext(
        tail_delta_only=True,
        global_start_open_ms=5000,
        symbol_start_open_ms={"BTC": 6000},
        exchange_symbol_start_open_ms={},
    )

    adapter.context = replacement

    assert adapter.context is replacement


def test_resolve_symbol_start_open_ms_bound_applies_specific_and_tail_bounds() -> None:
    context = runtime.BronzeRuntimeBoundsContext(
        tail_delta_only=False,
        global_start_open_ms=1000,
        symbol_start_open_ms={"BTC": 2000},
        exchange_symbol_start_open_ms={"deribit:BTC": 3000},
    )

    assert runtime.resolve_symbol_start_open_ms_bound(exchange="deribit", symbol="BTCUSDT", context=context) == 3000
    assert runtime.resolve_symbol_start_open_ms_bound(exchange="deribit", symbol="ETHUSDT", context=context) == 1000

    tail_context = runtime.BronzeRuntimeBoundsContext(
        tail_delta_only=True,
        global_start_open_ms=None,
        symbol_start_open_ms={},
        exchange_symbol_start_open_ms={"deribit:BTC": 1000},
    )
    resolved = runtime.resolve_symbol_start_open_ms_bound(exchange="deribit", symbol="BTCUSDT", context=tail_context)
    assert isinstance(resolved, int)
    assert resolved > 1000


def test_fingerprint_and_write_checkpoint_roundtrip(tmp_path: Path) -> None:
    args = argparse.Namespace(
        exchange="deribit",
        lake_root="lake/bronze",
        tail_delta_only=True,
        start_date=None,
        symbol_start_dates=None,
        exchange_symbol_start_dates=None,
    )
    fp = runtime.bronze_checkpoint_fingerprint(args, _plan())
    assert len(fp) == 64

    path = tmp_path / "x" / "chk.json"
    completed = {
        "candle": {"a"},
        "open_interest": set(),
        "funding": set(),
        "volatility_index_data": set(),
        "trade": {"b"},
    }
    runtime.write_bronze_checkpoint(path, fingerprint=fp, completed=completed)
    loaded = runtime.load_bronze_checkpoint(path, fp, logging.getLogger("test"))
    assert loaded["candle"] == {"a"}
    assert loaded["trade"] == {"b"}


def test_dataset_task_key_maps_use_registry_checkpoint_keys() -> None:
    candle_map, open_interest_map, funding_map, trade_map = runtime.dataset_task_key_maps(_registry_plan())

    assert candle_map[("deribit", "spot_ohlcv", "BTC", "1m")] == "deribit|spot_ohlcv|spot_ohlcv|BTC|1m|spot_ohlcv"
    assert open_interest_map[("deribit", "BTC", "1m")] == "deribit|open_interest|perp|BTC|1m|perp"
    assert funding_map[("deribit", "BTC", "1m")] == "deribit|funding|perp|BTC|1m|perp"
    assert trade_map[("deribit", "perp", "BTC")] == "deribit|perps_trades|perp|BTC|tick|perp"


def test_bronze_checkpoint_key_maps_include_volatility_tasks() -> None:
    key_maps = runtime.bronze_checkpoint_key_maps(_registry_plan())

    assert key_maps.candle[("deribit", "spot_ohlcv", "BTC", "1m")] == "deribit|spot_ohlcv|spot_ohlcv|BTC|1m|spot_ohlcv"
    assert key_maps.volatility_index_data[("deribit", "BTC", "1m")] == "deribit|volatility_index_data|perp|BTC|1m|perp"


def test_checkpoint_key_for_task_falls_back_to_legacy_tuple_keys() -> None:
    key_maps = runtime.bronze_checkpoint_key_maps(_registry_plan())

    assert (
        runtime.checkpoint_key_for_task("candle", ("deribit", "spot_ohlcv", "BTC", "1m"), key_maps)
        == "deribit|spot_ohlcv|spot_ohlcv|BTC|1m|spot_ohlcv"
    )
    assert runtime.checkpoint_key_for_task("open_interest", ("deribit", "ETH", "1m"), key_maps) == "deribit|ETH|1m"


def test_checkpoint_task_keys_and_completion_use_dataset_maps() -> None:
    key_maps = runtime.bronze_checkpoint_key_maps(_registry_plan())
    completed = runtime.empty_checkpoint_state()

    serialized = runtime.add_completed_checkpoint_key(
        completed=completed,
        dataset="trade",
        key=("deribit", "perp", "BTC"),
        key_maps=key_maps,
    )
    task_keys = runtime.checkpoint_task_keys(
        candle_tasks=[("deribit", "spot_ohlcv", "BTC", "1m")],
        open_interest_tasks=[],
        funding_tasks=[],
        volatility_index_data_tasks=[],
        trade_tasks=[("deribit", "perp", "BTC")],
        key_maps=key_maps,
    )

    assert serialized == "deribit|perps_trades|perp|BTC|tick|perp"
    assert completed["trade"] == {serialized}
    assert task_keys == {"deribit|spot_ohlcv|spot_ohlcv|BTC|1m|spot_ohlcv", serialized}


def test_apply_checkpoint_filter_with_key_maps_drops_registry_completed_tasks() -> None:
    plan = _registry_plan()
    key_maps = runtime.bronze_checkpoint_key_maps(plan)
    completed = runtime.empty_checkpoint_state()
    completed["candle"].add("deribit|spot_ohlcv|spot_ohlcv|BTC|1m|spot_ohlcv")
    completed["trade"].add("deribit|perps_trades|perp|BTC|tick|perp")

    pending = runtime.apply_checkpoint_filter_with_key_maps(
        candle_tasks=plan.candle_tasks,
        open_interest_tasks=plan.open_interest_tasks,
        funding_tasks=plan.funding_tasks,
        volatility_index_data_tasks=plan.volatility_index_data_tasks,
        trade_tasks=plan.trade_tasks,
        completed=completed,
        key_maps=key_maps,
    )

    assert pending.candle_tasks == []
    assert pending.open_interest_tasks == [("deribit", "BTC", "1m")]
    assert pending.funding_tasks == [("deribit", "BTC", "1m")]
    assert pending.trade_tasks == []


def test_hydrate_checkpoint_aliases_adds_registry_keys_for_legacy_completed_keys() -> None:
    plan = _registry_plan()
    candle_map, open_interest_map, funding_map, trade_map = runtime.dataset_task_key_maps(plan)
    completed = {
        "candle": {"deribit|spot_ohlcv|BTC|1m"},
        "open_interest": {"deribit|BTC|1m"},
        "funding": {"deribit|BTC|1m"},
        "volatility_index_data": {"deribit|BTC|1m"},
        "trade": {"deribit|perp|BTC"},
    }

    runtime.hydrate_checkpoint_aliases(
        completed=completed,
        candle_tasks=plan.candle_tasks,
        open_interest_tasks=plan.open_interest_tasks,
        funding_tasks=plan.funding_tasks,
        volatility_index_data_tasks=plan.volatility_index_data_tasks,
        trade_tasks=plan.trade_tasks,
        candle_key_map=candle_map,
        open_interest_key_map=open_interest_map,
        funding_key_map=funding_map,
        volatility_key_map={("deribit", "BTC", "1m"): "deribit|volatility_index_data|perp|BTC|1m|perp"},
        trade_key_map=trade_map,
    )

    assert "deribit|spot_ohlcv|spot_ohlcv|BTC|1m|spot_ohlcv" in completed["candle"]
    assert "deribit|open_interest|perp|BTC|1m|perp" in completed["open_interest"]
    assert "deribit|funding|perp|BTC|1m|perp" in completed["funding"]
    assert "deribit|perps_trades|perp|BTC|tick|perp" in completed["trade"]


def test_apply_checkpoint_filter_drops_completed_tasks() -> None:
    candle_tasks = [("deribit", "spot_ohlcv", "BTC", "1m"), ("deribit", "perp", "ETH", "1m")]
    open_interest_tasks = [("deribit", "BTC", "1m")]
    funding_tasks = [("deribit", "ETH", "1m")]
    volatility_index_data_tasks = [("deribit", "SOL", "1m")]
    trade_tasks = [("deribit", "perp", "BTC"), ("deribit", "option", "ETH")]
    completed = {
        "candle": {_serialize(("deribit", "spot_ohlcv", "BTC", "1m"))},
        "open_interest": set(),
        "funding": {_serialize(("deribit", "ETH", "1m"))},
        "volatility_index_data": set(),
        "trade": {_serialize(("deribit", "option", "ETH"))},
    }

    pending = runtime.apply_checkpoint_filter(
        candle_tasks=candle_tasks,
        open_interest_tasks=open_interest_tasks,
        funding_tasks=funding_tasks,
        volatility_index_data_tasks=volatility_index_data_tasks,
        trade_tasks=trade_tasks,
        completed=completed,
        candle_key_serializer=_serialize,
        open_interest_key_serializer=_serialize,
        funding_key_serializer=_serialize,
        volatility_key_serializer=_serialize,
        trade_key_serializer=_serialize,
    )

    assert pending.candle_tasks == [("deribit", "perp", "ETH", "1m")]
    assert pending.open_interest_tasks == [("deribit", "BTC", "1m")]
    assert pending.funding_tasks == []
    assert pending.volatility_index_data_tasks == [("deribit", "SOL", "1m")]
    assert pending.trade_tasks == [("deribit", "perp", "BTC")]


def test_has_checkpoint_state_detects_any_completed_bucket() -> None:
    empty = {"candle": set(), "open_interest": set(), "funding": set(), "volatility_index_data": set(), "trade": set()}
    assert not runtime.has_checkpoint_state(empty)
    non_empty = {
        "candle": {"x"},
        "open_interest": set(),
        "funding": set(),
        "volatility_index_data": set(),
        "trade": set(),
    }
    assert runtime.has_checkpoint_state(non_empty)
