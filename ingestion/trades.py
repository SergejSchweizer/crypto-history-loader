"""Tick-trade ingestion interface (Deribit-only)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from math import isfinite
from typing import Any, Literal, cast

from ingestion.exchanges import deribit_options_trades, deribit_perps_trades
from ingestion.spot_ohlcv import Exchange, normalize_storage_symbol

TradeMarket = Literal["spot_ohlcv", "perp", "option"]
OptionType = Literal["call", "put"]


@dataclass(frozen=True)
class TradeTick:
    """Historical trade tick."""

    exchange: str
    symbol: str
    instrument_type: TradeMarket
    trade_id: str
    trade_time: datetime
    price: float
    quantity: float
    side: str
    is_maker: bool
    source_endpoint: str


@dataclass(frozen=True)
class OptionTradeTick:
    """Historical option trade tick."""

    exchange: str
    symbol: str
    instrument_type: Literal["option"]
    instrument_name: str
    expiry: str
    strike: float
    option_type: OptionType
    trade_id: str
    trade_time: datetime
    price: float
    quantity: float
    side: str
    is_maker: bool
    source_endpoint: str


def _canonical_underlying_symbol(symbol: str) -> str:
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


def _normalize_trade_symbol(exchange: Exchange, symbol: str, market: TradeMarket) -> str:
    if market == "option":
        return _canonical_underlying_symbol(symbol)
    return normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)


def _parse_trade_row(
    exchange: Exchange, symbol: str, market: Literal["spot_ohlcv", "perp"], row: dict[str, object]
) -> TradeTick:
    _validate_trade_row(row)
    ts_ms = _positive_timestamp_ms(row["timestamp"])
    trade_id = _non_empty_text(row["trade_id"], "trade_id")
    side = _parse_side(row)
    is_maker = _parse_is_maker(row)
    return TradeTick(
        exchange=exchange,
        symbol=normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market),
        instrument_type=market,
        trade_id=trade_id,
        trade_time=datetime.fromtimestamp(ts_ms / 1000, tz=UTC),
        price=_positive_finite_number(row["price"], "price"),
        quantity=_positive_finite_number(row["amount"], "amount"),
        side=side,
        is_maker=is_maker,
        source_endpoint="public_trades",
    )


def _parse_options_trade_row(exchange: Exchange, symbol: str, row: dict[str, object]) -> OptionTradeTick:
    _validate_trade_row(row)
    ts_ms = _positive_timestamp_ms(row["timestamp"])
    trade_id = _non_empty_text(row["trade_id"], "trade_id")
    side = _parse_side(row)
    is_maker = _parse_is_maker(row)
    instrument_name = _non_empty_text(row.get("instrument_name"), "instrument_name")
    expiry, strike, option_type = _parse_option_contract_fields(instrument_name)
    return OptionTradeTick(
        exchange=exchange,
        symbol=_canonical_underlying_symbol(symbol),
        instrument_type="option",
        instrument_name=instrument_name,
        expiry=expiry,
        strike=strike,
        option_type=option_type,
        trade_id=trade_id,
        trade_time=datetime.fromtimestamp(ts_ms / 1000, tz=UTC),
        price=_positive_finite_number(row["price"], "price"),
        quantity=_positive_finite_number(row["amount"], "amount"),
        side=side,
        is_maker=is_maker,
        source_endpoint="public_options_trades",
    )


def _parse_side(row: dict[str, object]) -> str:
    """Normalize Deribit trade side field."""

    direction = str(row.get("direction", "")).lower()
    if direction == "buy":
        return "buy"
    if direction == "sell":
        return "sell"
    raise ValueError("direction must be exactly 'buy' or 'sell'")


def _parse_is_maker(row: dict[str, object]) -> bool:
    """Normalize Deribit maker/taker marker."""

    return str(cast(Any, row).get("liquidation", "")).lower() == "m"


def _parse_option_contract_fields(instrument_name: str) -> tuple[str, float, OptionType]:
    """Parse expiry/strike/option_type from Deribit option instrument name."""

    parts = instrument_name.split("-")
    if len(parts) != 4 or not parts[0].strip():
        raise ValueError("instrument_name must be a complete Deribit option contract")
    expiry = parts[1].upper()
    try:
        datetime.strptime(expiry, "%d%b%y")
    except ValueError as exc:
        raise ValueError("instrument_name expiry must use Deribit DDMMMYY format") from exc
    strike = _positive_finite_number(parts[2], "option strike")
    suffix = parts[3].upper()
    if suffix == "C":
        return expiry, strike, "call"
    if suffix == "P":
        return expiry, strike, "put"
    raise ValueError("instrument_name option type must be C or P")


def _validate_trade_row(row: object) -> None:
    """Reject non-object provider payload entries before parsing their fields."""

    if not isinstance(row, dict):
        raise TypeError("trade payload entry must be an object")
    required_fields = ("timestamp", "trade_id", "price", "amount", "direction")
    missing_fields = [field_name for field_name in required_fields if field_name not in row]
    if missing_fields:
        raise ValueError(f"trade payload entry is missing required fields: {', '.join(missing_fields)}")


def _non_empty_text(value: object, field_name: str) -> str:
    """Return a required non-empty text field."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _positive_timestamp_ms(value: object) -> int:
    """Return a positive integral millisecond timestamp."""

    if isinstance(value, bool):
        raise ValueError("timestamp must be a positive integer")
    try:
        timestamp = int(cast(Any, value))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("timestamp must be a positive integer") from exc
    if timestamp <= 0:
        raise ValueError("timestamp must be a positive integer")
    return timestamp


def _positive_finite_number(value: object, field_name: str) -> float:
    """Return a finite provider numeric field that must be strictly positive."""

    try:
        numeric_value = float(cast(Any, value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite positive number") from exc
    if not isfinite(numeric_value) or numeric_value <= 0:
        raise ValueError(f"{field_name} must be a finite positive number")
    return numeric_value


def fetch_trades_all_history(
    exchange: Exchange,
    symbol: str,
    market: TradeMarket,
    on_history_chunk: Callable[[list[TradeTick | OptionTradeTick]], None] | None = None,
) -> list[TradeTick | OptionTradeTick]:
    """Fetch all available historical trades."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    normalized_symbol = _normalize_trade_symbol(exchange=exchange, symbol=symbol, market=market)
    if market == "option":
        rows = deribit_options_trades.fetch_options_trades_all(currency=normalized_symbol)
        parsed: list[TradeTick | OptionTradeTick] = [
            _parse_options_trade_row(exchange, normalized_symbol, row) for row in rows
        ]
        if on_history_chunk is not None and parsed:
            on_history_chunk(parsed)
            return []
        return parsed
    market_non_option: Literal["spot_ohlcv", "perp"] = market

    def _on_page(rows: list[dict[str, object]]) -> None:
        if on_history_chunk is None:
            return
        on_history_chunk([_parse_trade_row(exchange, normalized_symbol, market_non_option, row) for row in rows])

    rows = deribit_perps_trades.fetch_perps_trades_all(
        symbol=normalized_symbol,
        market=market_non_option,
        on_page=_on_page if on_history_chunk is not None else None,
    )
    if on_history_chunk is not None:
        return []
    return [_parse_trade_row(exchange, normalized_symbol, market_non_option, row) for row in rows]


def fetch_trades_range(
    exchange: Exchange,
    symbol: str,
    market: TradeMarket,
    start_open_ms: int,
    end_open_ms: int,
    page_size: int | None = None,
) -> list[TradeTick | OptionTradeTick]:
    """Fetch historical trades by inclusive range."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    normalized_symbol = _normalize_trade_symbol(exchange=exchange, symbol=symbol, market=market)
    if market == "option":
        rows = deribit_options_trades.fetch_options_trades_range(
            currency=normalized_symbol,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
            count=page_size,
        )
        return [_parse_options_trade_row(exchange, normalized_symbol, row) for row in rows]
    market_non_option: Literal["spot_ohlcv", "perp"] = market
    rows = deribit_perps_trades.fetch_perps_trades_range(
        symbol=normalized_symbol,
        market=market_non_option,
        start_open_ms=start_open_ms,
        end_open_ms=end_open_ms,
        count=page_size,
    )
    return [_parse_trade_row(exchange, normalized_symbol, market_non_option, row) for row in rows]
