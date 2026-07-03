"""Compatibility aliases for the Deribit perps_trades adapter."""

from __future__ import annotations

from collections.abc import Callable

from ingestion.exchanges.deribit_perps_trades import (
    DERIBIT_PERP_TRADES_BASE_URL_DEFAULT,
    DERIBIT_PERP_TRADES_DEFAULT_PAGE_SIZE,
    DERIBIT_PERP_TRADES_FALLBACK_BASE_URL,
    DERIBIT_PERP_TRADES_MAX_PAGE_SIZE,
    fetch_perps_trades_all,
    fetch_perps_trades_range,
)

DERIBIT_TRADES_MAX_PAGE_SIZE = DERIBIT_PERP_TRADES_MAX_PAGE_SIZE
DERIBIT_TRADES_DEFAULT_PAGE_SIZE = DERIBIT_PERP_TRADES_DEFAULT_PAGE_SIZE
DERIBIT_TRADES_BASE_URL_DEFAULT = DERIBIT_PERP_TRADES_BASE_URL_DEFAULT
DERIBIT_TRADES_FALLBACK_BASE_URL = DERIBIT_PERP_TRADES_FALLBACK_BASE_URL


def fetch_trades_range(
    *,
    symbol: str,
    market: str,
    start_open_ms: int,
    end_open_ms: int,
    count: int | None = None,
) -> list[dict[str, object]]:
    """Fetch Deribit perps_trades, preserving the previous generic function name."""

    return fetch_perps_trades_range(
        symbol=symbol,
        market=market,
        start_open_ms=start_open_ms,
        end_open_ms=end_open_ms,
        count=count,
    )


def fetch_trades_all(
    *,
    symbol: str,
    market: str,
    on_page: Callable[[list[dict[str, object]]], None] | None = None,
) -> list[dict[str, object]]:
    """Fetch all Deribit perps_trades, preserving the previous generic function name."""

    return fetch_perps_trades_all(symbol=symbol, market=market, on_page=on_page)
