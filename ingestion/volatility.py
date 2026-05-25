"""Volatility ingestion interface (Deribit-only)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast

from ingestion.exchanges import deribit_volatility
from ingestion.http_client import HttpClientError
from ingestion.spot import Exchange, Market

VolatilityDatasetType = Literal["volatility_index"]


@dataclass(frozen=True)
class VolatilityPoint:
    """Volatility datapoint for one instrument interval."""

    exchange: str
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    value: float
    source_endpoint: str
    dataset_type: VolatilityDatasetType


def normalize_volatility_timeframe(exchange: Exchange, value: str) -> str:
    """Normalize volatility timeframe by exchange."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    normalized = value.strip().lower()
    aliases = {
        "m1": "1m",
        "1m": "1m",
        "m5": "5m",
        "5m": "5m",
        "m15": "15m",
        "15m": "15m",
        "m30": "30m",
        "30m": "30m",
        "h1": "1h",
        "1h": "1h",
        "h4": "4h",
        "4h": "4h",
        "h12": "12h",
        "12h": "12h",
        "d1": "1d",
        "1d": "1d",
    }
    if normalized not in aliases:
        raise ValueError(f"Unsupported volatility timeframe '{value}' for {exchange}")
    return aliases[normalized]


def volatility_interval_to_milliseconds(exchange: Exchange, interval: str) -> int:
    """Convert volatility interval to milliseconds."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    if interval.endswith("m"):
        return int(interval[:-1]) * 60_000
    if interval.endswith("h"):
        return int(interval[:-1]) * 3_600_000
    if interval.endswith("d"):
        return int(interval[:-1]) * 86_400_000
    raise ValueError(f"Unsupported volatility interval '{interval}'")


def _canonical_currency(symbol: str) -> str:
    upper = symbol.upper().strip()
    if not upper:
        raise ValueError("symbol cannot be empty")
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


def _parse_volatility_index_row(
    exchange: Exchange, symbol: str, interval: str, row: dict[str, object]
) -> VolatilityPoint:
    ts_ms = int(cast(Any, row.get("timestamp", 0)))
    open_time = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
    return VolatilityPoint(
        exchange=exchange,
        symbol=_canonical_currency(symbol),
        interval=interval,
        open_time=open_time,
        close_time=open_time,
        value=float(cast(Any, row.get("index_value", 0.0))),
        source_endpoint="public_get_volatility_index_data",
        dataset_type="volatility_index",
    )


def fetch_volatility_index_all_history(
    exchange: Exchange,
    symbol: str,
    interval: str,
    market: Market,
    on_history_chunk: Callable[[list[VolatilityPoint]], None] | None = None,
) -> list[VolatilityPoint]:
    """Fetch all available volatility index rows."""

    if market != "perp":
        return []
    normalized_interval = normalize_volatility_timeframe(exchange=exchange, value=interval)
    if exchange != "deribit":
        return []
    try:
        rows = deribit_volatility.fetch_volatility_index_data_all(
            currency=_canonical_currency(symbol),
            resolution=normalized_interval,
        )
    except HttpClientError:
        return []
    points = [_parse_volatility_index_row(exchange, symbol, normalized_interval, row) for row in rows]
    if on_history_chunk is not None and points:
        on_history_chunk(points)
        return []
    return points


def fetch_volatility_index_range(
    exchange: Exchange,
    symbol: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
    market: Market,
) -> list[VolatilityPoint]:
    """Fetch volatility index rows by inclusive open-time range."""

    if market != "perp":
        return []
    normalized_interval = normalize_volatility_timeframe(exchange=exchange, value=interval)
    if exchange != "deribit":
        return []
    try:
        rows = deribit_volatility.fetch_volatility_index_data_range(
            currency=_canonical_currency(symbol),
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
            resolution=normalized_interval,
        )
    except HttpClientError:
        return []
    return [_parse_volatility_index_row(exchange, symbol, normalized_interval, row) for row in rows]
