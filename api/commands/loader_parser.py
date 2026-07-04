"""Argument parser registration for the Bronze loader command."""

from __future__ import annotations

from typing import Any

from application.datasets import DATASET_REGISTRY

MARKET_CHOICES = tuple(DATASET_REGISTRY.keys())


def add_ingest_parser(
    subparsers: Any,
    *,
    command_name: str,
    help_text: str,
) -> None:
    """Register a Bronze ingest parser under the provided command name."""

    parser = subparsers.add_parser(command_name, help=help_text)
    parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["deribit"],
        help="Optional list of exchanges to fetch in one run",
    )
    parser.add_argument(
        "--dataset",
        nargs="+",
        choices=MARKET_CHOICES,
        default=["spot_ohlcv"],
        help="One or more data types to fetch, e.g. --dataset spot_ohlcv perps_ohlcv open_interest funding",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTC", "ETH", "SOL"],
        help="Symbols used for all selected markets/datasets.",
    )
    parser.set_defaults(tail_delta_only=False)
    parser.add_argument(
        "--save-parquet-lake",
        action="store_true",
        help="Save fetched candles to parquet lake partitions",
    )
    parser.add_argument(
        "--lake-root",
        default="lake/bronze",
        help="Root directory for parquet lake files",
    )
    parser.add_argument(
        "--no-json-output",
        action="store_true",
        help="Suppress JSON output from bronze-build command",
    )
    parser.add_argument(
        "--tail-delta-only",
        dest="tail_delta_only",
        action="store_true",
        help="Fetch only new tail data after latest stored point (overrides default full-gap-fill mode).",
    )
    parser.add_argument(
        "--full-gap-fill",
        dest="tail_delta_only",
        action="store_false",
        help="Run full historical internal gap checks (default behavior).",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Inclusive UTC date boundary (YYYY-MM-DD) for Bronze ingestion history.",
    )
    parser.add_argument(
        "--symbol-start-dates",
        nargs="+",
        default=None,
        help="Per-symbol inclusive UTC start dates (SYMBOL=YYYY-MM-DD), e.g. BTC=2023-04-24",
    )
    parser.add_argument(
        "--exchange-symbol-start-dates",
        nargs="+",
        default=None,
        help=(
            "Per exchange-symbol inclusive UTC start dates (EXCHANGE:SYMBOL=YYYY-MM-DD), e.g. deribit:BTC=2023-04-24"
        ),
    )


def add_bronze_build_parser(subparsers: Any) -> None:
    """Register canonical ``bronze-build`` parser."""

    add_ingest_parser(
        subparsers,
        command_name="bronze-build",
        help_text="Bronze medallion ingest from supported exchanges",
    )
