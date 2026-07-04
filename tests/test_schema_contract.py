"""Tests for canonical dataset contract mapping."""

from __future__ import annotations

from application.datasets import DATASET_REGISTRY, dataset_names_for_task_kind, dataset_spec
from application.schema import dataset_contract


def test_dataset_contract_maps_spot_ohlcv_perp_open_interest() -> None:
    spot_ohlcv = dataset_contract("spot_ohlcv")
    perps_ohlcv = dataset_contract("perps_ohlcv")
    open_interest = dataset_contract("open_interest")
    funding = dataset_contract("funding")
    trades = dataset_contract("perps_trades")
    options_trades = dataset_contract("options_trades")
    volatility_index_data = dataset_contract("volatility_index_data")

    assert spot_ohlcv.dataset_type == "spot_ohlcv"
    assert spot_ohlcv.instrument_type == "spot_ohlcv"

    assert perps_ohlcv.dataset_type == "perps_ohlcv"
    assert perps_ohlcv.instrument_type == "perp"

    assert open_interest.dataset_type == "open_interest"
    assert open_interest.instrument_type == "perp"

    assert funding.dataset_type == "funding"
    assert funding.instrument_type == "perp"
    assert trades.dataset_type == "perps_trades"
    assert trades.instrument_type == "perp"
    assert options_trades.dataset_type == "options_trades"
    assert options_trades.instrument_type == "option"
    assert volatility_index_data.dataset_type == "volatility_index_data"
    assert volatility_index_data.instrument_type == "perp"


def test_dataset_registry_covers_contract_names() -> None:
    assert set(DATASET_REGISTRY) == {
        "spot_ohlcv",
        "perps_ohlcv",
        "open_interest",
        "funding",
        "perps_trades",
        "options_trades",
        "volatility_index_data",
    }
    assert dataset_spec("perps_trades").bronze_task_kind == "trade"
    assert dataset_spec("options_trades").symbol_group == "options_trade_symbols"
    assert dataset_names_for_task_kind("trade") == {"perps_trades", "options_trades"}
    assert dataset_names_for_task_kind("volatility") == {"volatility_index_data"}
