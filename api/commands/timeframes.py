"""List supported timeframe command."""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any, cast

from ingestion.spot_ohlcv import Exchange, list_supported_intervals


def add_list_spot_ohlcv_timeframes_parser(subparsers: Any) -> None:
    """Register ``list-spot_ohlcv-timeframes`` parser."""

    parser = subparsers.add_parser("list-spot_ohlcv-timeframes", help="List exchange-supported candle timeframes")
    parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["deribit"],
        help="Optional list of exchanges to list in one run",
    )


def run_list_spot_ohlcv_timeframes(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run ``list-spot_ohlcv-timeframes`` command."""

    exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
    output = {exchange: list(list_supported_intervals(exchange=exchange)) for exchange in exchanges}
    print(json.dumps(output, indent=2))
    logger.info("Command complete: list-spot_ohlcv-timeframes")
