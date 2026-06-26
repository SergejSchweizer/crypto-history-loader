"""Typed runtime policy helpers for fetch task execution behavior."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

PERP_TRADES_WINDOW_MS = 15 * 60 * 1000
OPTION_TRADES_WINDOW_MS = 60 * 60 * 1000
MIN_TRADE_WINDOW_MS = 60 * 1000
MAX_TRADE_WINDOW_MS = 24 * 60 * 60 * 1000
DEFAULT_FETCH_HEARTBEAT_S = 30.0


@dataclass(frozen=True)
class FetchRuntimePolicy:
    """Resolved fetch execution policy derived from environment-style config.

    Attributes:
        task_timeout_s: Optional hard timeout for individual fetch tasks. ``None``
            disables timeout enforcement.
        heartbeat_s: Interval for long-running task progress messages.
        perp_trade_window_ms: Bounded inclusive window size for perp trade fetches.
        option_trade_window_ms: Bounded inclusive window size for option trade fetches.
    """

    task_timeout_s: float | None
    heartbeat_s: float
    perp_trade_window_ms: int
    option_trade_window_ms: int


def load_fetch_runtime_policy(env: Mapping[str, str] | None = None) -> FetchRuntimePolicy:
    """Resolve fetch runtime policy from environment values.

    Args:
        env: Environment-style mapping. Defaults to ``os.environ``.

    Returns:
        Resolved, bounded runtime policy for fetch orchestration.
    """

    source = os.environ if env is None else env
    task_timeout = _env_float(source, "DEPTH_FETCH_TASK_TIMEOUT_S", 0.0)
    heartbeat = _env_float(source, "DEPTH_FETCH_HEARTBEAT_S", DEFAULT_FETCH_HEARTBEAT_S)
    return FetchRuntimePolicy(
        task_timeout_s=task_timeout if task_timeout > 0 else None,
        heartbeat_s=heartbeat if heartbeat > 0 else DEFAULT_FETCH_HEARTBEAT_S,
        perp_trade_window_ms=_env_window_ms(
            source,
            env_name="DEPTH_PERP_TRADES_WINDOW_MINUTES",
            default_ms=PERP_TRADES_WINDOW_MS,
        ),
        option_trade_window_ms=_env_window_ms(
            source,
            env_name="DEPTH_OPTION_TRADES_WINDOW_MINUTES",
            default_ms=OPTION_TRADES_WINDOW_MS,
        ),
    )


def task_timeout_seconds() -> float | None:
    """Return optional per-task timeout in seconds from environment."""

    return load_fetch_runtime_policy().task_timeout_s


def heartbeat_seconds() -> float:
    """Return heartbeat interval in seconds for long-running fetch tasks."""

    return load_fetch_runtime_policy().heartbeat_s


def trade_window_ms(market: str) -> int:
    """Return bounded trade fetch window size by dataset family."""

    policy = load_fetch_runtime_policy()
    if market == "option":
        return policy.option_trade_window_ms
    return policy.perp_trade_window_ms


def _env_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_window_ms(env: Mapping[str, str], *, env_name: str, default_ms: int) -> int:
    raw = env.get(env_name)
    if raw is None:
        return default_ms
    try:
        minutes = int(raw)
    except ValueError:
        return default_ms
    return min(MAX_TRADE_WINDOW_MS, max(MIN_TRADE_WINDOW_MS, minutes * 60 * 1000))
