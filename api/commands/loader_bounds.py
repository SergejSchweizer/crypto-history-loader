"""Compatibility exports for Bronze start-bound helpers."""

from __future__ import annotations

import argparse
import logging
from typing import cast

from application.services.bronze_runtime_service import (
    build_bronze_runtime_bounds_context,
    symbol_start_open_ms_bound,
)


def configure_bronze_start_bounds(
    *,
    args: argparse.Namespace,
    logger: logging.Logger,
) -> tuple[int | None, dict[str, int], dict[str, int]]:
    """Compute Bronze start-bound maps from CLI/config args and emit boundary logs."""

    context = build_bronze_runtime_bounds_context(
        tail_delta_only=bool(getattr(args, "tail_delta_only", False)),
        start_date=cast(str | None, getattr(args, "start_date", None)),
        symbol_start_dates=cast(list[str] | None, getattr(args, "symbol_start_dates", None)),
        exchange_symbol_start_dates=cast(list[str] | None, getattr(args, "exchange_symbol_start_dates", None)),
        logger=logger,
    )
    return context.global_start_open_ms, context.symbol_start_open_ms, context.exchange_symbol_start_open_ms


__all__ = ["configure_bronze_start_bounds", "symbol_start_open_ms_bound"]
