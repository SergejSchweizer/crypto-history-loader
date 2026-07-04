"""Dataset-specific task planning and output assembly helpers for bronze loader."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import cast

from application.datasets import CliDataType, DatasetSpec, dataset_spec
from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot_ohlcv import Exchange, Market, SpotCandle
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick
from ingestion.volatility import VolatilityPoint


def build_trade_tasks(
    *,
    exchanges: list[Exchange],
    perp_trade_symbols: list[str],
    option_trade_symbols: list[str],
    perps_trades_requested: bool,
    options_trades_requested: bool,
) -> list[tuple[Exchange, TradeMarket, str]]:
    """Build trade task tuples with symbol-first round-robin ordering."""

    requested_specs: list[DatasetSpec] = []
    if perps_trades_requested:
        requested_specs.append(dataset_spec("perps_trades"))
    if options_trades_requested:
        requested_specs.append(dataset_spec("options_trades"))
    return build_trade_tasks_from_specs(
        exchanges=exchanges,
        specs=requested_specs,
        symbols_by_group={
            "perp_trade_symbols": perp_trade_symbols,
            "option_trade_symbols": option_trade_symbols,
        },
    )


def build_trade_tasks_from_specs(
    *,
    exchanges: list[Exchange],
    specs: list[DatasetSpec],
    symbols_by_group: dict[str, list[str]],
) -> list[tuple[Exchange, TradeMarket, str]]:
    """Build trade task tuples from registry specs with symbol-first ordering."""

    if not specs:
        return []
    tasks: list[tuple[Exchange, TradeMarket, str]] = []
    seen_symbols: set[str] = set()
    ordered_symbols: list[str] = []
    requested_symbols_by_dataset: dict[CliDataType, set[str]] = {}
    for spec in specs:
        symbols = symbols_by_group.get(spec.symbol_group, [])
        requested_symbols_by_dataset[spec.cli_data_type] = set(symbols)
        for symbol in symbols:
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            ordered_symbols.append(symbol)
    for exchange in exchanges:
        for symbol in ordered_symbols:
            for spec in specs:
                if symbol not in requested_symbols_by_dataset[spec.cli_data_type]:
                    continue
                if spec.market not in {"spot_ohlcv", "perp", "option"}:
                    raise ValueError(f"Dataset '{spec.cli_data_type}' is not a trade dataset")
                tasks.append((exchange, spec.market, symbol))
    return tasks


def _trade_dataset_key(market: TradeMarket) -> str:
    """Return output dataset key for one trade market."""

    return "options_trades" if market == "option" else "perps_trades"


def _serialize_trade_row(item: TradeTick | OptionTradeTick) -> dict[str, object]:
    """Serialize one trade row for command JSON output."""

    row: dict[str, object] = {
        "exchange": item.exchange,
        "symbol": item.symbol,
        "instrument_type": item.instrument_type,
        "trade_id": item.trade_id,
        "trade_time": item.trade_time.isoformat(),
        "price": item.price,
        "quantity": item.quantity,
        "side": item.side,
        "is_maker": item.is_maker,
    }
    if isinstance(item, OptionTradeTick):
        row["instrument_name"] = item.instrument_name
        row["expiry"] = item.expiry
        row["strike"] = item.strike
        row["option_type"] = item.option_type
    return row


def populate_ohlcv_output(
    *,
    output: dict[str, object],
    tasks: Iterable[tuple[Exchange, Market, str, str]],
    task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]],
    task_errors: dict[tuple[Exchange, Market, str, str], str],
    multi_market: bool,
    candle_serializer: Callable[[SpotCandle], dict[str, object]],
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
) -> None:
    """Populate JSON output and storage bucket for OHLCV tasks."""

    for exchange, market, symbol, timeframe in tasks:
        exchange_output = cast(dict[str, object], output[exchange])
        symbol_key = symbol.upper()
        result_key = (exchange, market, symbol, timeframe)
        if multi_market:
            market_bucket = cast(dict[str, object], exchange_output.setdefault(market, {}))
        else:
            market_bucket = exchange_output
        if result_key in task_errors:
            market_bucket[symbol_key] = {"error": task_errors[result_key]}
            continue
        candles = task_results.get(result_key, [])
        market_bucket[symbol_key] = [candle_serializer(item) for item in candles]
        by_market = candles_for_storage.setdefault(market, {})
        by_exchange = by_market.setdefault(exchange, {})
        by_exchange[symbol_key] = candles


def populate_open_interest_output(
    *,
    output: dict[str, object],
    tasks: Iterable[tuple[Exchange, str, str]],
    results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]],
    errors: dict[tuple[Exchange, str, str], str],
    multi_market: bool,
    storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
) -> None:
    """Populate JSON output and storage bucket for Open Interest tasks."""

    for exchange, symbol, timeframe in tasks:
        symbol_key = symbol.upper()
        open_interest_key = (exchange, symbol, timeframe)
        exchange_output = cast(dict[str, object], output[exchange])
        if multi_market:
            market_bucket = cast(dict[str, object], exchange_output.setdefault("open_interest", {}))
        else:
            market_bucket = exchange_output
        if open_interest_key in errors:
            market_bucket[symbol_key] = {"error": errors[open_interest_key]}
            continue
        rows = results.get(open_interest_key, [])
        market_bucket[symbol_key] = [
            {
                "exchange": item.exchange,
                "symbol": item.symbol,
                "interval": item.interval,
                "open_time": item.open_time.isoformat(),
                "close_time": item.close_time.isoformat(),
                "open_interest": item.open_interest,
                "open_interest_value": item.open_interest_value,
            }
            for item in rows
        ]
        by_market = storage.setdefault("perp", {})
        by_exchange = by_market.setdefault(exchange, {})
        by_exchange[symbol_key] = rows


def populate_funding_output(
    *,
    output: dict[str, object],
    tasks: Iterable[tuple[Exchange, str, str]],
    results: dict[tuple[Exchange, str, str], list[FundingPoint]],
    errors: dict[tuple[Exchange, str, str], str],
    multi_market: bool,
    storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]],
) -> None:
    """Populate JSON output and storage bucket for funding tasks."""

    for exchange, symbol, timeframe in tasks:
        symbol_key = symbol.upper()
        funding_key = (exchange, symbol, timeframe)
        exchange_output = cast(dict[str, object], output[exchange])
        if multi_market:
            market_bucket = cast(dict[str, object], exchange_output.setdefault("funding", {}))
        else:
            market_bucket = exchange_output
        if funding_key in errors:
            market_bucket[symbol_key] = {"error": errors[funding_key]}
            continue
        rows = results.get(funding_key, [])
        market_bucket[symbol_key] = [
            {
                "exchange": item.exchange,
                "symbol": item.symbol,
                "interval": item.interval,
                "open_time": item.open_time.isoformat(),
                "close_time": item.close_time.isoformat(),
                "funding_rate": item.funding_rate,
                "index_price": item.index_price,
                "mark_price": item.mark_price,
            }
            for item in rows
        ]
        by_market = storage.setdefault("perp", {})
        by_exchange = by_market.setdefault(exchange, {})
        by_exchange[symbol_key] = rows


def populate_volatility_output(
    *,
    output: dict[str, object],
    tasks: Iterable[tuple[Exchange, str, str]],
    results: dict[tuple[Exchange, str, str], list[VolatilityPoint]],
    errors: dict[tuple[Exchange, str, str], str],
    multi_market: bool,
    storage: dict[Market, dict[str, dict[str, list[VolatilityPoint]]]],
    dataset_key: str,
) -> None:
    """Populate JSON output and storage bucket for volatility tasks."""

    for exchange, symbol, timeframe in tasks:
        symbol_key = symbol.upper()
        key = (exchange, symbol, timeframe)
        exchange_output = cast(dict[str, object], output[exchange])
        if multi_market:
            market_bucket = cast(dict[str, object], exchange_output.setdefault(dataset_key, {}))
        else:
            market_bucket = exchange_output
        if key in errors:
            market_bucket[symbol_key] = {"error": errors[key]}
            continue
        rows = results.get(key, [])
        market_bucket[symbol_key] = [
            {
                "exchange": item.exchange,
                "symbol": item.symbol,
                "interval": item.interval,
                "open_time": item.open_time.isoformat(),
                "close_time": item.close_time.isoformat(),
                "value": item.value,
                "source_endpoint": item.source_endpoint,
                "dataset_type": item.dataset_type,
            }
            for item in rows
        ]
        by_market = storage.setdefault("perp", {})
        by_exchange = by_market.setdefault(exchange, {})
        by_exchange[symbol_key] = rows


def populate_trades_output(
    *,
    output: dict[str, object],
    tasks: Iterable[tuple[Exchange, TradeMarket, str]],
    results: dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]],
    errors: dict[tuple[Exchange, TradeMarket, str], str],
    multi_market: bool,
    storage: dict[TradeMarket, dict[str, dict[str, list[TradeTick | OptionTradeTick]]]],
) -> None:
    """Populate JSON output and storage bucket for trade tasks."""

    for exchange, market, symbol in tasks:
        symbol_key = symbol.upper()
        trade_key = (exchange, market, symbol)
        exchange_output = cast(dict[str, object], output[exchange])
        dataset_key = _trade_dataset_key(market)
        if multi_market:
            trades_bucket = cast(dict[str, object], exchange_output.setdefault(dataset_key, {}))
        else:
            trades_bucket = exchange_output
        market_bucket = cast(dict[str, object], trades_bucket.setdefault(market, {}))
        if trade_key in errors:
            market_bucket[symbol_key] = {"error": errors[trade_key]}
            continue
        rows = results.get(trade_key, [])
        market_bucket[symbol_key] = [_serialize_trade_row(item) for item in rows]
        by_market = storage.setdefault(market, {})
        by_exchange = by_market.setdefault(exchange, {})
        by_exchange[symbol_key] = rows
