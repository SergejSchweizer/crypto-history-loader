"""Tests for bronze reporting helpers."""

from __future__ import annotations

from application.datasets import DatasetTask
from application.services.bronze_reporting_service import (
    symbol_progress_rows,
    symbol_progress_rows_from_dataset_tasks,
    trade_error_breakdown,
)


def test_trade_error_breakdown_counts_classes() -> None:
    errors = {
        ("deribit", "perp", "BTC"): "[NET_UNREACHABLE] x",
        ("deribit", "perp", "ETH"): "[NET_TIMEOUT] y",
        ("deribit", "option", "BTC"): "z",
    }
    summary = trade_error_breakdown(errors)
    assert summary == {"total": 3, "net_unreachable": 1, "net_timeout": 1, "other": 1}


def test_symbol_progress_rows_calculates_ratios() -> None:
    rows = symbol_progress_rows(
        candle_tasks=[("deribit", "spot", "BTC", "1m"), ("deribit", "spot", "ETH", "1m")],
        oi_tasks=[],
        funding_tasks=[],
        volatility_index_data_tasks=[],
        trade_tasks=[("deribit", "perp", "BTC")],
        candle_results={("deribit", "spot", "BTC", "1m"): object()},
        oi_results={},
        funding_results={},
        volatility_index_data_results={},
        trade_results={("deribit", "perp", "BTC"): object()},
    )
    assert rows == [
        {"symbol": "BTC", "success": 2, "total": 2, "ratio": 1.0},
        {"symbol": "ETH", "success": 0, "total": 1, "ratio": 0.0},
    ]


def test_symbol_progress_rows_counts_oi_and_funding_success() -> None:
    rows = symbol_progress_rows(
        candle_tasks=[],
        oi_tasks=[("deribit", "BTC", "1m")],
        funding_tasks=[("deribit", "BTC", "8h")],
        volatility_index_data_tasks=[],
        trade_tasks=[],
        candle_results={},
        oi_results={("deribit", "BTC", "1m"): object()},
        funding_results={("deribit", "BTC", "8h"): object()},
        volatility_index_data_results={},
        trade_results={},
    )
    assert rows == [{"symbol": "BTC", "success": 2, "total": 2, "ratio": 1.0}]


def test_symbol_progress_rows_from_dataset_tasks_uses_checkpoint_keys() -> None:
    tasks = [
        DatasetTask(
            exchange="deribit",
            dataset_type="spot",
            instrument_type="spot",
            symbol="BTC",
            timeframe="1m",
            market="spot",
        ),
        DatasetTask(
            exchange="deribit",
            dataset_type="oi",
            instrument_type="perp",
            symbol="BTC",
            timeframe="1m",
            market="perp",
        ),
        DatasetTask(
            exchange="deribit",
            dataset_type="perps_trades",
            instrument_type="perp",
            symbol="ETH",
            timeframe="tick",
            market="perp",
        ),
    ]
    success_keys = {tasks[0].checkpoint_key(), tasks[2].checkpoint_key()}

    rows = symbol_progress_rows_from_dataset_tasks(dataset_tasks=tasks, success_keys=success_keys)

    assert rows == [
        {"symbol": "BTC", "success": 1, "total": 2, "ratio": 0.5},
        {"symbol": "ETH", "success": 1, "total": 1, "ratio": 1.0},
    ]
