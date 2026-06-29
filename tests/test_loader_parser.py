"""Tests for Bronze loader parser registration."""

from __future__ import annotations

import argparse

from api.commands.loader_parser import add_bronze_build_parser


def _build_parser() -> argparse.ArgumentParser:
    """Build a minimal parser containing only the Bronze command."""

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    add_bronze_build_parser(subparsers)
    return parser


def test_bronze_parser_defaults_to_restart_safe_full_gap_fill() -> None:
    """The Bronze parser preserves its default full-gap-fill behavior."""

    args = _build_parser().parse_args(["bronze-build"])

    assert args.command == "bronze-build"
    assert args.exchange == "deribit"
    assert args.dataset == ["spot"]
    assert args.symbols == ["BTC", "ETH", "SOL"]
    assert args.tail_delta_only is False
    assert args.lake_root == "lake/bronze"


def test_bronze_parser_accepts_trade_datasets_and_runtime_bounds() -> None:
    """The extracted parser keeps trade datasets and runtime-bound options available."""

    args = _build_parser().parse_args(
        [
            "bronze-build",
            "--dataset",
            "perp_trades",
            "option_trades",
            "--symbols",
            "BTC",
            "ETH",
            "--tail-delta-only",
            "--start-date",
            "2023-04-24",
            "--exchange-symbol-start-dates",
            "deribit:BTC=2023-04-24",
        ]
    )

    assert args.dataset == ["perp_trades", "option_trades"]
    assert args.symbols == ["BTC", "ETH"]
    assert args.tail_delta_only is True
    assert args.start_date == "2023-04-24"
    assert args.exchange_symbol_start_dates == ["deribit:BTC=2023-04-24"]
