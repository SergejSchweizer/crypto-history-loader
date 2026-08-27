"""Spot/perpetual candle ingestion interface (Deribit-only)."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from math import isfinite
from typing import Any, Literal

from ingestion.exchanges import deribit

Exchange = Literal["deribit"]
Market = Literal["spot_ohlcv", "perp"]
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SpotCandle:
    """OHLCV candle for an instrument.

    Example:
        ```python
        from datetime import UTC, datetime
        from ingestion.spot_ohlcv import SpotCandle

        candle = SpotCandle(
            exchange="deribit",
            symbol="BTCUSDT",
            interval="1m",
            open_time=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            close_time=datetime(2026, 1, 1, 0, 0, 59, 999000, tzinfo=UTC),
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.5,
            volume=12.0,
            quote_volume=1200.0,
            trade_count=34,
        )
        ```
    """

    exchange: str
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_volume: float | None
    trade_count: int


def _ms_to_utc(ts_ms: int) -> datetime:
    """Convert epoch milliseconds to timezone-aware UTC datetime."""

    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC)


def parse_kline(exchange: Exchange, symbol: str, interval: str, row: list[Any]) -> SpotCandle:
    """Parse a common kline row into a typed candle object."""

    if len(row) not in {9, 12}:
        raise ValueError("OHLCV row must contain the 9-field base or 12-field extended layout")
    interval_ms = interval_to_milliseconds(exchange=exchange, interval=interval)
    open_time_ms = _non_negative_integer(row[0], "open_time")
    close_time_ms = _non_negative_integer(row[6], "close_time")
    if open_time_ms % interval_ms != 0:
        raise ValueError("OHLCV open_time must align to the candle interval")
    if close_time_ms != open_time_ms + interval_ms - 1:
        raise ValueError("OHLCV close_time must match the candle interval")

    open_price = _finite_number(row[1], "open_price", positive=True)
    high_price = _finite_number(row[2], "high_price", positive=True)
    low_price = _finite_number(row[3], "low_price", positive=True)
    close_price = _finite_number(row[4], "close_price", positive=True)
    volume = _finite_number(row[5], "volume", positive=False)
    quote_volume_raw = row[7]
    quote_volume = (
        None if quote_volume_raw is None else _finite_number(quote_volume_raw, "quote_volume", positive=False)
    )
    trade_count = _non_negative_integer(row[8], "trade_count")

    if high_price < max(open_price, close_price, low_price):
        raise ValueError("OHLCV high_price must be the greatest price")
    if low_price > min(open_price, close_price, high_price):
        raise ValueError("OHLCV low_price must be the lowest price")

    return SpotCandle(
        exchange=exchange,
        symbol=symbol,
        interval=interval,
        open_time=_ms_to_utc(open_time_ms),
        close_time=_ms_to_utc(close_time_ms),
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        volume=volume,
        quote_volume=quote_volume,
        trade_count=trade_count,
    )


def _finite_number(value: object, field_name: str, *, positive: bool) -> float:
    """Validate finite OHLCV numeric semantics without silently accepting invalid values."""

    try:
        numeric_value = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"OHLCV {field_name} must be numeric") from exc
    if not isfinite(numeric_value) or numeric_value < 0 or (positive and numeric_value == 0):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"OHLCV {field_name} must be finite and {qualifier}")
    return numeric_value


def _non_negative_integer(value: object, field_name: str) -> int:
    """Validate integer timestamp/count fields without truncating fractional values."""

    if isinstance(value, bool):
        raise ValueError(f"OHLCV {field_name} must be a non-negative integer")
    try:
        integer_value = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"OHLCV {field_name} must be a non-negative integer") from exc
    if integer_value < 0 or str(value).strip() not in {str(integer_value), f"{integer_value}.0"}:
        raise ValueError(f"OHLCV {field_name} must be a non-negative integer")
    return integer_value


def list_supported_intervals(exchange: Exchange) -> tuple[str, ...]:
    """List supported intervals for the requested exchange."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return deribit.list_supported_intervals()


def normalize_timeframe(exchange: Exchange, value: str) -> str:
    """Normalize timeframe aliases per exchange format."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return deribit.normalize_timeframe(value)


def max_candles_per_request(exchange: Exchange) -> int:
    """Return max single-request candle count for exchange."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return deribit.max_limit()


def interval_to_milliseconds(exchange: Exchange, interval: str) -> int:
    """Convert normalized interval into milliseconds for exchange."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return deribit.interval_to_milliseconds(interval)


def normalize_storage_symbol(exchange: Exchange, symbol: str, market: Market) -> str:
    """Normalize symbol to storage form for selected exchange/market."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return deribit.normalize_symbol(symbol=symbol, market=market)


def fetch_candles(
    exchange: Exchange,
    symbol: str,
    interval: str = "1m",
    limit: int = 100,
    market: Market = "spot_ohlcv",
) -> list[SpotCandle]:
    """Fetch latest candles from supported exchanges."""

    normalized_interval = normalize_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    rows = deribit.fetch_klines(
        symbol=symbol,
        market=market,
        interval=normalized_interval,
        limit=limit,
    )

    return [
        parse_kline(exchange=exchange, symbol=normalized_symbol, interval=normalized_interval, row=row) for row in rows
    ]


def fetch_candles_all_history(
    exchange: Exchange,
    symbol: str,
    interval: str = "1m",
    market: Market = "spot_ohlcv",
    on_history_chunk: Callable[[list[SpotCandle]], None] | None = None,
) -> list[SpotCandle]:
    """Fetch all available candles from exchange history."""

    normalized_interval = normalize_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")

    def _on_page(page: list[list[object]]) -> None:
        if on_history_chunk is None:
            return
        on_history_chunk(
            [
                parse_kline(exchange=exchange, symbol=normalized_symbol, interval=normalized_interval, row=row)
                for row in page
            ]
        )

    try:
        rows = deribit.fetch_klines_all(
            symbol=symbol,
            market=market,
            interval=normalized_interval,
            on_page=_on_page if on_history_chunk is not None else None,
        )
    except TypeError as exc:
        if "on_page" not in str(exc):
            raise
        rows = deribit.fetch_klines_all(
            symbol=symbol,
            market=market,
            interval=normalized_interval,
        )

    return [
        parse_kline(exchange=exchange, symbol=normalized_symbol, interval=normalized_interval, row=row) for row in rows
    ]


def fetch_candles_range(
    exchange: Exchange,
    symbol: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
    market: Market = "spot_ohlcv",
) -> list[SpotCandle]:
    """Fetch candles by open-time range inclusive."""

    normalized_interval = normalize_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    started = time.monotonic()
    rows = deribit.fetch_klines_range(
        symbol=symbol,
        market=market,
        interval=normalized_interval,
        start_open_ms=start_open_ms,
        end_open_ms=end_open_ms,
    )

    candles = [
        parse_kline(exchange=exchange, symbol=normalized_symbol, interval=normalized_interval, row=row) for row in rows
    ]
    logger.info(
        "OHLCV day fetch done exchange=%s market=%s symbol=%s interval=%s start_ms=%s end_ms=%s rows=%s elapsed_s=%.2f",
        exchange,
        market,
        normalized_symbol,
        normalized_interval,
        start_open_ms,
        end_open_ms,
        len(candles),
        time.monotonic() - started,
    )
    return candles
