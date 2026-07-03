"""Planning helpers for bronze loader command orchestration."""

from __future__ import annotations

import argparse
import logging
from typing import cast

from api.commands.loader_dataset_handlers import build_trade_tasks_from_specs
from application.datasets import CliDataType, DatasetSpec, DatasetTask, dataset_specs
from application.dto import BronzeFetchPlanDTO
from application.services.bronze_runtime_service import (
    canonical_symbol_key as _canonical_symbol_key,
)
from application.services.bronze_runtime_service import (
    parse_exchange_symbol_start_dates as _parse_exchange_symbol_start_dates,
)
from application.services.bronze_runtime_service import (
    parse_start_date_to_open_ms as _parse_start_date_to_open_ms,
)
from application.services.bronze_runtime_service import (
    parse_symbol_start_dates as _parse_symbol_start_dates,
)
from ingestion.spot_ohlcv import Exchange, Market, normalize_timeframe

BRONZE_FIXED_TIMEFRAME = "1m"


def sanitize_symbols(raw_symbols: object, logger: logging.Logger) -> list[str]:
    """Return validated symbol list, dropping null/blank/non-string entries."""

    if not isinstance(raw_symbols, list):
        raise ValueError("Symbols must be provided as a list")
    cleaned: list[str] = []
    dropped = 0
    for raw in cast(list[object], raw_symbols):
        if not isinstance(raw, str):
            dropped += 1
            continue
        symbol = raw.strip()
        if not symbol:
            dropped += 1
            continue
        cleaned.append(symbol)
    if dropped > 0:
        logger.warning("Dropped %s invalid symbol entries from configured symbol list", dropped)
    if not cleaned:
        raise ValueError("No valid symbols configured. Provide at least one non-empty symbol.")
    return cleaned


def resolved_symbol_groups(args: argparse.Namespace, logger: logging.Logger) -> tuple[list[str], list[str], list[str]]:
    """Return deterministically ordered symbol groups for Bronze task planning.

    All dataset groups currently resolve from ``--symbols``.
    """

    validated_symbols = sorted(sanitize_symbols(cast(object, args.symbols), logger=logger))
    validated_perp_trade_symbols = list(validated_symbols)
    validated_option_trade_symbols = list(validated_symbols)
    return (
        validated_symbols,
        validated_perp_trade_symbols,
        validated_option_trade_symbols,
    )


def build_bronze_fetch_plan(args: argparse.Namespace, logger: logging.Logger) -> BronzeFetchPlanDTO:
    """Build deterministic Bronze task plan shared across all dataset fetchers."""

    exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
    selected = getattr(args, "dataset", getattr(args, "market", None))
    if selected is None:
        raise ValueError("Missing dataset selection. Provide --dataset.")
    data_types = sorted(cast(list[CliDataType], selected))
    specs = dataset_specs(data_types)
    ohlcv_markets = cast(list[Market], [spec.market for spec in specs if spec.bronze_task_kind == "ohlcv"])
    symbols, perp_trade_symbols, option_trade_symbols = resolved_symbol_groups(args=args, logger=logger)

    dataset_tasks: list[DatasetTask] = []
    candle_tasks: list[tuple[Exchange, Market, str, str]] = []
    oi_tasks: list[tuple[Exchange, str, str]] = []
    funding_tasks: list[tuple[Exchange, str, str]] = []
    volatility_index_data_tasks: list[tuple[Exchange, str, str]] = []
    for exchange in sorted(exchanges):
        normalized_timeframe = normalize_timeframe(exchange=exchange, value=BRONZE_FIXED_TIMEFRAME)
        for symbol in symbols:
            for spec in specs:
                if spec.bronze_task_kind != "ohlcv":
                    continue
                task = spec.build_task(exchange=exchange, symbol=symbol, timeframe=normalized_timeframe)
                dataset_tasks.append(task)
                candle_tasks.append(task.candle_tuple())
        for spec in specs:
            if spec.bronze_task_kind in {"ohlcv", "trade"}:
                continue
            for symbol in _symbols_for_spec(
                spec=spec,
                symbols=symbols,
                perp_trade_symbols=perp_trade_symbols,
                option_trade_symbols=option_trade_symbols,
            ):
                task = spec.build_task(exchange=exchange, symbol=symbol, timeframe=normalized_timeframe)
                dataset_tasks.append(task)
                if spec.bronze_task_kind == "open_interest":
                    oi_tasks.append(task.interval_tuple())
                elif spec.bronze_task_kind == "funding":
                    funding_tasks.append(task.interval_tuple())

    trade_specs = [spec for spec in specs if spec.bronze_task_kind == "trade"]
    trade_tasks = build_trade_tasks_from_specs(
        exchanges=sorted(exchanges),
        specs=trade_specs,
        symbols_by_group={
            "perp_trade_symbols": perp_trade_symbols,
            "option_trade_symbols": option_trade_symbols,
        },
    )
    for exchange, market, symbol in trade_tasks:
        spec = _trade_spec_for_market(specs=trade_specs, market=market)
        dataset_tasks.append(spec.build_task(exchange=exchange, symbol=symbol))

    return BronzeFetchPlanDTO(
        exchanges=sorted(exchanges),
        data_types=list(data_types),
        ohlcv_markets=ohlcv_markets,
        symbols=symbols,
        perp_trade_symbols=perp_trade_symbols,
        option_trade_symbols=option_trade_symbols,
        candle_tasks=candle_tasks,
        oi_tasks=oi_tasks,
        funding_tasks=funding_tasks,
        volatility_index_data_tasks=volatility_index_data_tasks,
        trade_tasks=trade_tasks,
        dataset_tasks=dataset_tasks,
    )


def _symbols_for_spec(
    *,
    spec: DatasetSpec,
    symbols: list[str],
    perp_trade_symbols: list[str],
    option_trade_symbols: list[str],
) -> list[str]:
    """Return the configured symbol group for one registered dataset."""

    if spec.symbol_group == "symbols":
        return symbols
    if spec.symbol_group == "perp_trade_symbols":
        return perp_trade_symbols
    if spec.symbol_group == "option_trade_symbols":
        return option_trade_symbols
    raise ValueError(f"Unsupported symbol group '{spec.symbol_group}'")


def _trade_spec_for_market(*, specs: list[DatasetSpec], market: str) -> DatasetSpec:
    """Return the registered trade spec matching one trade market."""

    for spec in specs:
        if spec.market == market:
            return spec
    raise ValueError(f"No registered trade dataset for market '{market}'")


def parse_start_date_to_open_ms(start_date: str | None) -> int | None:
    """Parse inclusive UTC start date ``YYYY-MM-DD`` to epoch milliseconds."""

    return _parse_start_date_to_open_ms(start_date)


def canonical_symbol_key(symbol: str) -> str:
    """Return canonical base symbol key for per-symbol start-date matching."""

    return _canonical_symbol_key(symbol)


def parse_symbol_start_dates(entries: list[str] | None) -> dict[str, int]:
    """Parse ``SYMBOL=YYYY-MM-DD`` entries into canonical symbol-to-epoch-ms map."""

    return _parse_symbol_start_dates(entries)


def parse_exchange_symbol_start_dates(entries: list[str] | None) -> dict[str, int]:
    """Parse ``EXCHANGE:SYMBOL=YYYY-MM-DD`` entries into canonical exchange:symbol map."""

    return _parse_exchange_symbol_start_dates(entries)
