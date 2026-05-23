"""Runtime policy helpers for bronze loader orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from api.commands.loader_bounds import symbol_start_open_ms_bound
from ingestion.spot import Exchange


@dataclass(frozen=True)
class BronzeRuntimeBoundsContext:
    """Runtime bounds state for bronze fetch behavior."""

    tail_delta_only: bool
    global_start_open_ms: int | None
    symbol_start_open_ms: dict[str, int]
    exchange_symbol_start_open_ms: dict[str, int]


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
