"""Bronze runtime planning/policy/checkpoint helpers."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from pathlib import Path
from typing import cast

from application.dto import BronzeExecutionPolicyDTO, BronzeFetchPlanDTO

_EMPTY_CHECKPOINT: dict[str, set[str]] = {
    "candle": set(),
    "oi": set(),
    "funding": set(),
    "historical_volatility": set(),
    "volatility_index_data": set(),
    "trade": set(),
}


def build_bronze_execution_policy(configured_concurrency: int) -> BronzeExecutionPolicyDTO:
    """Build standardized Bronze execution policy."""

    effective_concurrency = 1
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
        return _EMPTY_CHECKPOINT.copy()
    try:
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ignoring unreadable Bronze checkpoint '%s': %s", path, exc)
        return _EMPTY_CHECKPOINT.copy()
    if not isinstance(raw_payload, dict):
        return _EMPTY_CHECKPOINT.copy()
    payload = cast(dict[str, object], raw_payload)
    if payload.get("fingerprint") != fingerprint:
        logger.info("Ignoring stale Bronze checkpoint '%s' (fingerprint mismatch)", path)
        return _EMPTY_CHECKPOINT.copy()
    raw_completed = payload.get("completed")
    if not isinstance(raw_completed, dict):
        return _EMPTY_CHECKPOINT.copy()
    completed = cast(dict[str, object], raw_completed)
    return {
        "candle": set(str(value) for value in cast(list[object], completed.get("candle", []))),
        "oi": set(str(value) for value in cast(list[object], completed.get("oi", []))),
        "funding": set(str(value) for value in cast(list[object], completed.get("funding", []))),
        "historical_volatility": set(
            str(value) for value in cast(list[object], completed.get("historical_volatility", []))
        ),
        "volatility_index_data": set(
            str(value) for value in cast(list[object], completed.get("volatility_index_data", []))
        ),
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
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)
