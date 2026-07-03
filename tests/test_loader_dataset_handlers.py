"""Tests for dataset-specific loader helpers."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from api.commands.loader_dataset_handlers import (
    build_trade_tasks,
    build_trade_tasks_from_specs,
    populate_volatility_output,
)
from application.datasets import DatasetSpec, dataset_spec
from ingestion.volatility import VolatilityPoint


def test_build_trade_tasks_uses_only_perp_market() -> None:
    tasks = build_trade_tasks(
        exchanges=["deribit"],
        perp_trade_symbols=["BTC", "ETH"],
        option_trade_symbols=["BTC", "ETH"],
        perps_trades_requested=True,
        option_trades_requested=False,
    )

    assert tasks == [
        ("deribit", "perp", "BTC"),
        ("deribit", "perp", "ETH"),
    ]


def test_build_trade_tasks_returns_empty_when_not_requested() -> None:
    tasks = build_trade_tasks(
        exchanges=["deribit"],
        perp_trade_symbols=["BTC"],
        option_trade_symbols=["BTC"],
        perps_trades_requested=False,
        option_trades_requested=False,
    )

    assert tasks == []


def test_build_trade_tasks_includes_option_market_when_requested() -> None:
    tasks = build_trade_tasks(
        exchanges=["deribit"],
        perp_trade_symbols=["BTC"],
        option_trade_symbols=["ETH"],
        perps_trades_requested=True,
        option_trades_requested=True,
    )
    assert tasks == [("deribit", "perp", "BTC"), ("deribit", "option", "ETH")]


def test_build_trade_tasks_from_specs_keeps_symbol_first_ordering() -> None:
    tasks = build_trade_tasks_from_specs(
        exchanges=["deribit"],
        specs=[dataset_spec("perps_trades"), dataset_spec("option_trades")],
        symbols_by_group={
            "perp_trade_symbols": ["BTC", "ETH"],
            "option_trade_symbols": ["BTC"],
        },
    )

    assert tasks == [
        ("deribit", "perp", "BTC"),
        ("deribit", "option", "BTC"),
        ("deribit", "perp", "ETH"),
    ]


def test_build_trade_tasks_from_specs_rejects_non_trade_dataset() -> None:
    spec = DatasetSpec(
        cli_data_type="spot_ohlcv",
        dataset_type="spot_ohlcv",
        instrument_type="spot_ohlcv",
        bronze_task_kind="ohlcv",
        symbol_group="symbols",
        market=None,
    )

    with pytest.raises(ValueError, match="not a trade dataset"):
        build_trade_tasks_from_specs(
            exchanges=["deribit"],
            specs=[spec],
            symbols_by_group={"symbols": ["BTC"]},
        )


def test_populate_volatility_output_writes_payload_and_storage() -> None:
    """Volatility output should mirror JSON payload and storage side effects."""

    row = VolatilityPoint(
        exchange="deribit",
        symbol="BTC",
        interval="1m",
        open_time=datetime(2026, 5, 1, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, tzinfo=UTC),
        value=42.0,
        source_endpoint="public_get_volatility_index_data",
        dataset_type="volatility_index",
    )
    output: dict[str, object] = {"deribit": {}}
    storage = {}

    populate_volatility_output(
        output=output,
        tasks=[("deribit", "BTC", "1m")],
        results={("deribit", "BTC", "1m"): [row]},
        errors={},
        multi_market=True,
        storage=storage,
        dataset_key="volatility_index_data",
    )

    assert output["deribit"] == {
        "volatility_index_data": {
            "BTC": [
                {
                    "exchange": "deribit",
                    "symbol": "BTC",
                    "interval": "1m",
                    "open_time": "2026-05-01T00:00:00+00:00",
                    "close_time": "2026-05-01T00:00:00+00:00",
                    "value": 42.0,
                    "source_endpoint": "public_get_volatility_index_data",
                    "dataset_type": "volatility_index",
                }
            ]
        }
    }
    assert storage == {"perp": {"deribit": {"BTC": [row]}}}


def test_populate_volatility_output_records_errors_without_storage() -> None:
    """Volatility output should expose task errors without storage side effects."""

    output: dict[str, object] = {"deribit": {}}
    storage = {}

    populate_volatility_output(
        output=output,
        tasks=[("deribit", "ETH", "1m")],
        results={},
        errors={("deribit", "ETH", "1m"): "route failed"},
        multi_market=False,
        storage=storage,
        dataset_key="volatility_index_data",
    )

    assert output["deribit"] == {"ETH": {"error": "route failed"}}
    assert storage == {}
