"""Bronze runtime planning/policy/checkpoint helpers."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from application.dto import BronzeExecutionPolicyDTO, BronzeFetchPlanDTO
from ingestion.spot import Exchange, Market
from ingestion.trades import TradeMarket

CandleTaskKey = tuple[Exchange, Market, str, str]
IntervalTaskKey = tuple[Exchange, str, str]
TradeTaskKey = tuple[Exchange, TradeMarket, str]


@dataclass(frozen=True)
class PendingTaskGroups:
    """Pending task groups after applying completed-checkpoint filtering."""

    candle_tasks: list[CandleTaskKey]
    oi_tasks: list[IntervalTaskKey]
    funding_tasks: list[IntervalTaskKey]
    trade_tasks: list[TradeTaskKey]


@dataclass(frozen=True)
class BronzeRuntimeBoundsContext:
    """Runtime bounds state for Bronze fetch behavior."""

    tail_delta_only: bool
    global_start_open_ms: int | None
    symbol_start_open_ms: dict[str, int]
    exchange_symbol_start_open_ms: dict[str, int]


def build_bronze_execution_policy(configured_concurrency: int) -> BronzeExecutionPolicyDTO:
    """Build standardized Bronze execution policy."""

    effective_concurrency = max(1, configured_concurrency)
    return BronzeExecutionPolicyDTO(
        configured_concurrency=configured_concurrency,
        effective_concurrency=effective_concurrency,
        candle_concurrency=effective_concurrency,
        oi_concurrency=effective_concurrency,
        funding_concurrency=effective_concurrency,
        trade_concurrency=effective_concurrency,
    )


def task_key_tuple_to_string(parts: tuple[object, ...]) -> str:
    """Serialize tuple task key to stable checkpoint string."""

    return "|".join(str(part) for part in parts)


def parse_start_date_to_open_ms(start_date: str | None) -> int | None:
    """Parse inclusive UTC start date ``YYYY-MM-DD`` to epoch milliseconds."""

    if start_date is None:
        return None
    value = start_date.strip()
    if not value:
        return None
    start_dt = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    return int(start_dt.timestamp() * 1000)


def canonical_symbol_key(symbol: str) -> str:
    """Return canonical base symbol key for per-symbol start-date matching."""

    upper = symbol.upper().strip()
    if not upper:
        return upper
    if upper.endswith("-PERPETUAL"):
        return upper.split("-", 1)[0]
    if "_" in upper:
        return upper.split("_", 1)[0]
    if upper.endswith("USDC"):
        return upper[:-4]
    if upper.endswith("USDT"):
        return upper[:-4]
    if upper.endswith("USD"):
        return upper[:-3]
    return upper


def parse_symbol_start_dates(entries: list[str] | None) -> dict[str, int]:
    """Parse ``SYMBOL=YYYY-MM-DD`` entries into canonical symbol-to-epoch-ms map."""

    if not entries:
        return {}
    parsed: dict[str, int] = {}
    for raw in entries:
        item = raw.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Invalid symbol start date '{item}'. Expected SYMBOL=YYYY-MM-DD")
        symbol_part, date_part = item.split("=", 1)
        symbol_key = canonical_symbol_key(symbol_part)
        if not symbol_key:
            raise ValueError(f"Invalid symbol in symbol start date '{item}'")
        start_ms = parse_start_date_to_open_ms(date_part)
        if start_ms is None:
            raise ValueError(f"Invalid start date in symbol start date '{item}'")
        parsed[symbol_key] = start_ms
    return parsed


def parse_exchange_symbol_start_dates(entries: list[str] | None) -> dict[str, int]:
    """Parse ``EXCHANGE:SYMBOL=YYYY-MM-DD`` entries into canonical exchange:symbol map."""

    if not entries:
        return {}
    parsed: dict[str, int] = {}
    for raw in entries:
        item = raw.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Invalid exchange-symbol start date '{item}'. Expected EXCHANGE:SYMBOL=YYYY-MM-DD")
        pair_part, date_part = item.split("=", 1)
        if ":" not in pair_part:
            raise ValueError(f"Invalid exchange-symbol pair '{pair_part}'. Expected EXCHANGE:SYMBOL")
        exchange_part, symbol_part = pair_part.split(":", 1)
        exchange_key = exchange_part.strip().lower()
        symbol_key = canonical_symbol_key(symbol_part)
        if not exchange_key or not symbol_key:
            raise ValueError(f"Invalid exchange-symbol in '{item}'")
        start_ms = parse_start_date_to_open_ms(date_part)
        if start_ms is None:
            raise ValueError(f"Invalid start date in exchange-symbol start date '{item}'")
        parsed[f"{exchange_key}:{symbol_key}"] = start_ms
    return parsed


def symbol_start_open_ms_bound(
    *,
    exchange: Exchange,
    symbol: str,
    global_start_open_ms: int | None,
    symbol_start_open_ms: dict[str, int],
    exchange_symbol_start_open_ms: dict[str, int],
) -> int | None:
    """Resolve the effective Bronze start boundary before tail-mode capping."""

    exchange_key = exchange.lower()
    symbol_key = canonical_symbol_key(symbol)
    exchange_symbol_key = f"{exchange_key}:{symbol_key}"
    specific_bound = None
    if exchange_symbol_key in exchange_symbol_start_open_ms:
        specific_bound = exchange_symbol_start_open_ms[exchange_symbol_key]
    else:
        specific_bound = symbol_start_open_ms.get(symbol_key)
    if global_start_open_ms is None:
        return specific_bound
    if specific_bound is None:
        return global_start_open_ms
    return max(global_start_open_ms, specific_bound)


def build_bronze_runtime_bounds_context(
    *,
    tail_delta_only: bool,
    start_date: str | None,
    symbol_start_dates: list[str] | None,
    exchange_symbol_start_dates: list[str] | None,
    logger: logging.Logger,
) -> BronzeRuntimeBoundsContext:
    """Build runtime bounds context from CLI/config arguments and emit boundary logs."""

    global_start_open_ms = parse_start_date_to_open_ms(start_date)
    symbol_start_open_ms = parse_symbol_start_dates(symbol_start_dates)
    exchange_symbol_start_open_ms = parse_exchange_symbol_start_dates(exchange_symbol_start_dates)
    if global_start_open_ms is not None:
        logger.info(
            "Bronze start-date boundary enabled start_date=%s start_open_ms=%s",
            start_date,
            global_start_open_ms,
        )
    if symbol_start_open_ms:
        logger.info("Bronze symbol start-date boundaries enabled symbol_bounds=%s", symbol_start_open_ms)
    if exchange_symbol_start_open_ms:
        logger.info(
            "Bronze exchange-symbol start-date boundaries enabled exchange_symbol_bounds=%s",
            exchange_symbol_start_open_ms,
        )
    return BronzeRuntimeBoundsContext(
        tail_delta_only=tail_delta_only,
        global_start_open_ms=global_start_open_ms,
        symbol_start_open_ms=symbol_start_open_ms,
        exchange_symbol_start_open_ms=exchange_symbol_start_open_ms,
    )


def resolve_symbol_start_open_ms_bound(
    *,
    exchange: Exchange,
    symbol: str,
    context: BronzeRuntimeBoundsContext,
) -> int | None:
    """Resolve effective start bound for Bronze fetches."""

    configured_bound = symbol_start_open_ms_bound(
        exchange=exchange,
        symbol=symbol,
        global_start_open_ms=context.global_start_open_ms,
        symbol_start_open_ms=context.symbol_start_open_ms,
        exchange_symbol_start_open_ms=context.exchange_symbol_start_open_ms,
    )
    if not context.tail_delta_only:
        return configured_bound
    rolling_bound = int((datetime.now(UTC) - timedelta(days=30)).timestamp() * 1000)
    if configured_bound is None:
        return rolling_bound
    return max(configured_bound, rolling_bound)


def dataset_task_key_maps(
    plan: BronzeFetchPlanDTO,
) -> tuple[
    dict[CandleTaskKey, str],
    dict[IntervalTaskKey, str],
    dict[IntervalTaskKey, str],
    dict[TradeTaskKey, str],
]:
    """Return tuple-to-checkpoint-key mappings derived from registered dataset tasks."""

    candle_map: dict[CandleTaskKey, str] = {}
    oi_map: dict[IntervalTaskKey, str] = {}
    funding_map: dict[IntervalTaskKey, str] = {}
    trade_map: dict[TradeTaskKey, str] = {}
    for task in plan.dataset_tasks:
        key = task.checkpoint_key()
        if task.dataset_type in {"spot", "perp"}:
            candle_map[task.candle_tuple()] = key
        elif task.dataset_type == "oi":
            oi_map[task.interval_tuple()] = key
        elif task.dataset_type == "funding":
            funding_map[task.interval_tuple()] = key
        elif task.dataset_type in {"perp_trades", "option_trades"}:
            trade_map[task.trade_tuple()] = key
    return candle_map, oi_map, funding_map, trade_map


def hydrate_checkpoint_aliases(
    *,
    completed: dict[str, set[str]],
    candle_tasks: list[CandleTaskKey],
    oi_tasks: list[IntervalTaskKey],
    funding_tasks: list[IntervalTaskKey],
    trade_tasks: list[TradeTaskKey],
    candle_key_map: dict[CandleTaskKey, str],
    oi_key_map: dict[IntervalTaskKey, str],
    funding_key_map: dict[IntervalTaskKey, str],
    trade_key_map: dict[TradeTaskKey, str],
) -> None:
    """Augment completed checkpoint keys with registry aliases for backward compatibility."""

    for candle_task in candle_tasks:
        prior_key = task_key_tuple_to_string((candle_task[0], candle_task[1], candle_task[2], candle_task[3]))
        if prior_key in completed["candle"]:
            completed["candle"].add(candle_key_map.get(candle_task, prior_key))
    for oi_task in oi_tasks:
        prior_key = task_key_tuple_to_string((oi_task[0], oi_task[1], oi_task[2]))
        if prior_key in completed["oi"]:
            completed["oi"].add(oi_key_map.get(oi_task, prior_key))
    for funding_task in funding_tasks:
        prior_key = task_key_tuple_to_string((funding_task[0], funding_task[1], funding_task[2]))
        if prior_key in completed["funding"]:
            completed["funding"].add(funding_key_map.get(funding_task, prior_key))
    for trade_task in trade_tasks:
        prior_key = task_key_tuple_to_string((trade_task[0], trade_task[1], trade_task[2]))
        if prior_key in completed["trade"]:
            completed["trade"].add(trade_key_map.get(trade_task, prior_key))


def apply_checkpoint_filter(
    *,
    candle_tasks: list[CandleTaskKey],
    oi_tasks: list[IntervalTaskKey],
    funding_tasks: list[IntervalTaskKey],
    trade_tasks: list[TradeTaskKey],
    completed: dict[str, set[str]],
    candle_key_serializer: Callable[[CandleTaskKey], str],
    oi_key_serializer: Callable[[IntervalTaskKey], str],
    funding_key_serializer: Callable[[IntervalTaskKey], str],
    trade_key_serializer: Callable[[TradeTaskKey], str],
) -> PendingTaskGroups:
    """Filter task groups against completed checkpoint sets."""

    pending_candles = [task for task in candle_tasks if candle_key_serializer(task) not in completed["candle"]]
    pending_oi = [task for task in oi_tasks if oi_key_serializer(task) not in completed["oi"]]
    pending_funding = [task for task in funding_tasks if funding_key_serializer(task) not in completed["funding"]]
    pending_trades = [task for task in trade_tasks if trade_key_serializer(task) not in completed["trade"]]
    return PendingTaskGroups(
        candle_tasks=pending_candles,
        oi_tasks=pending_oi,
        funding_tasks=pending_funding,
        trade_tasks=pending_trades,
    )


def has_checkpoint_state(completed: dict[str, set[str]]) -> bool:
    """Return whether any checkpoint category has completed entries."""

    return any(completed[name] for name in ("candle", "oi", "funding", "trade"))


def bronze_checkpoint_fingerprint(args: argparse.Namespace, plan: BronzeFetchPlanDTO) -> str:
    """Build stable fingerprint for one Bronze invocation plan."""

    payload = {
        "exchange": args.exchange,
        "exchanges": plan.exchanges,
        "market": plan.data_types,
        "symbols": plan.symbols,
        "perp_trade_symbols": plan.perp_trade_symbols,
        "option_trade_symbols": plan.option_trade_symbols,
        "lake_root": cast(str, args.lake_root),
        "tail_delta_only": bool(args.tail_delta_only),
        "start_date": cast(str | None, getattr(args, "start_date", None)),
        "symbol_start_dates": cast(list[str] | None, getattr(args, "symbol_start_dates", None)),
        "exchange_symbol_start_dates": cast(list[str] | None, getattr(args, "exchange_symbol_start_dates", None)),
    }
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def bronze_checkpoint_path() -> Path:
    """Return Bronze restart-checkpoint path."""

    return Path(".run") / "checkpoints" / "bronze-build.json"


def load_bronze_checkpoint(path: Path, fingerprint: str, logger: logging.Logger) -> dict[str, set[str]]:
    """Load matching Bronze checkpoint completed-task sets."""

    if not path.exists():
        return {"candle": set(), "oi": set(), "funding": set(), "trade": set()}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ignoring unreadable Bronze checkpoint '%s': %s", path, exc)
        return {"candle": set(), "oi": set(), "funding": set(), "trade": set()}
    if not isinstance(payload, dict) or payload.get("fingerprint") != fingerprint:
        logger.info("Ignoring stale Bronze checkpoint '%s' (fingerprint mismatch)", path)
        return {"candle": set(), "oi": set(), "funding": set(), "trade": set()}
    completed = payload.get("completed")
    if not isinstance(completed, dict):
        return {"candle": set(), "oi": set(), "funding": set(), "trade": set()}
    return {
        "candle": set(str(value) for value in cast(list[object], completed.get("candle", []))),
        "oi": set(str(value) for value in cast(list[object], completed.get("oi", []))),
        "funding": set(str(value) for value in cast(list[object], completed.get("funding", []))),
        "trade": set(str(value) for value in cast(list[object], completed.get("trade", []))),
    }


def write_bronze_checkpoint(path: Path, *, fingerprint: str, completed: dict[str, set[str]]) -> None:
    """Persist Bronze checkpoint atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "fingerprint": fingerprint,
        "completed": {name: sorted(values) for name, values in completed.items()},
    }
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)
