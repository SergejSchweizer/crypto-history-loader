"""Dataset-specific symbol fetch adapters for the Bronze loader command."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime

from application.services import fetch_service
from application.services.bronze_runtime_service import BronzeRuntimeBoundsContext, resolve_symbol_start_open_ms_bound
from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot_ohlcv import Exchange, Market, SpotCandle
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick
from ingestion.volatility import VolatilityPoint


@dataclass(frozen=True)
class BronzeSymbolFetchDependencies:
    """External readers and adapter functions used by Bronze symbol fetchers."""

    open_times_in_lake: Callable[..., list[datetime]]
    open_times_in_lake_by_dataset: Callable[..., list[datetime]]
    latest_open_time_in_lake: Callable[..., datetime | None]
    latest_open_time_in_lake_by_dataset: Callable[..., datetime | None]
    normalize_storage_symbol: Callable[..., str]
    interval_to_milliseconds: Callable[..., int]
    open_interest_interval_to_milliseconds: Callable[..., int]
    funding_interval_to_milliseconds: Callable[..., int]
    volatility_interval_to_milliseconds: Callable[..., int]
    normalize_open_interest_timeframe: Callable[..., str]
    normalize_funding_timeframe: Callable[..., str]
    normalize_volatility_timeframe: Callable[..., str]
    last_closed_open_ms: Callable[..., int]
    missing_ranges_ms: Callable[..., list[tuple[int, int]]]
    fetch_candles_all_history: Callable[..., list[SpotCandle]]
    fetch_candles_range: Callable[..., list[SpotCandle]]
    fetch_open_interest_all_history: Callable[..., list[OpenInterestPoint]]
    fetch_open_interest_range: Callable[..., list[OpenInterestPoint]]
    fetch_funding_all_history: Callable[..., list[FundingPoint]]
    fetch_funding_range: Callable[..., list[FundingPoint]]
    fetch_volatility_index_all_history: Callable[..., list[VolatilityPoint]]
    fetch_volatility_index_range: Callable[..., list[VolatilityPoint]]
    fetch_trades_all_history: Callable[..., list[TradeTick | OptionTradeTick]]
    fetch_trades_range: Callable[..., list[TradeTick | OptionTradeTick]]


def build_symbol_fetch_dependencies(
    *,
    open_times_in_lake: Callable[..., list[datetime]],
    open_times_in_lake_by_dataset: Callable[..., list[datetime]],
    latest_open_time_in_lake: Callable[..., datetime | None],
    latest_open_time_in_lake_by_dataset: Callable[..., datetime | None],
    normalize_storage_symbol: Callable[..., str],
    interval_to_milliseconds: Callable[..., int],
    open_interest_interval_to_milliseconds: Callable[..., int],
    funding_interval_to_milliseconds: Callable[..., int],
    volatility_interval_to_milliseconds: Callable[..., int],
    normalize_open_interest_timeframe: Callable[..., str],
    normalize_funding_timeframe: Callable[..., str],
    normalize_volatility_timeframe: Callable[..., str],
    last_closed_open_ms: Callable[..., int],
    missing_ranges_ms: Callable[..., list[tuple[int, int]]],
    fetch_candles_all_history: Callable[..., list[SpotCandle]],
    fetch_candles_range: Callable[..., list[SpotCandle]],
    fetch_open_interest_all_history: Callable[..., list[OpenInterestPoint]],
    fetch_open_interest_range: Callable[..., list[OpenInterestPoint]],
    fetch_funding_all_history: Callable[..., list[FundingPoint]],
    fetch_funding_range: Callable[..., list[FundingPoint]],
    fetch_volatility_index_all_history: Callable[..., list[VolatilityPoint]],
    fetch_volatility_index_range: Callable[..., list[VolatilityPoint]],
    fetch_trades_all_history: Callable[..., list[TradeTick | OptionTradeTick]],
    fetch_trades_range: Callable[..., list[TradeTick | OptionTradeTick]],
) -> BronzeSymbolFetchDependencies:
    """Build the dependency bundle used by Bronze symbol fetch adapters."""

    return BronzeSymbolFetchDependencies(
        open_times_in_lake=open_times_in_lake,
        open_times_in_lake_by_dataset=open_times_in_lake_by_dataset,
        latest_open_time_in_lake=latest_open_time_in_lake,
        latest_open_time_in_lake_by_dataset=latest_open_time_in_lake_by_dataset,
        normalize_storage_symbol=normalize_storage_symbol,
        interval_to_milliseconds=interval_to_milliseconds,
        open_interest_interval_to_milliseconds=open_interest_interval_to_milliseconds,
        funding_interval_to_milliseconds=funding_interval_to_milliseconds,
        volatility_interval_to_milliseconds=volatility_interval_to_milliseconds,
        normalize_open_interest_timeframe=normalize_open_interest_timeframe,
        normalize_funding_timeframe=normalize_funding_timeframe,
        normalize_volatility_timeframe=normalize_volatility_timeframe,
        last_closed_open_ms=last_closed_open_ms,
        missing_ranges_ms=missing_ranges_ms,
        fetch_candles_all_history=fetch_candles_all_history,
        fetch_candles_range=fetch_candles_range,
        fetch_open_interest_all_history=fetch_open_interest_all_history,
        fetch_open_interest_range=fetch_open_interest_range,
        fetch_funding_all_history=fetch_funding_all_history,
        fetch_funding_range=fetch_funding_range,
        fetch_volatility_index_all_history=fetch_volatility_index_all_history,
        fetch_volatility_index_range=fetch_volatility_index_range,
        fetch_trades_all_history=fetch_trades_all_history,
        fetch_trades_range=fetch_trades_range,
    )


def serialize_candle(candle: SpotCandle) -> dict[str, object]:
    """Serialize a candle for command JSON output."""

    data = asdict(candle)
    for key in ("open_time", "close_time"):
        value = data[key]
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


def fetch_symbol_candles(
    *,
    dependencies: BronzeSymbolFetchDependencies,
    runtime_context: BronzeRuntimeBoundsContext,
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[SpotCandle]], None] | None = None,
) -> list[SpotCandle]:
    """Fetch one OHLCV symbol using loader-provided adapter dependencies."""

    return fetch_service.fetch_symbol_candles(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=dependencies.open_times_in_lake,
        symbol_normalizer=dependencies.normalize_storage_symbol,
        interval_ms_resolver=dependencies.interval_to_milliseconds,
        now_open_resolver=dependencies.last_closed_open_ms,
        ranges_builder=dependencies.missing_ranges_ms,
        history_fetcher=dependencies.fetch_candles_all_history,
        range_fetcher=dependencies.fetch_candles_range,
        latest_open_time_reader=dependencies.latest_open_time_in_lake,
        tail_delta_only=runtime_context.tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=_symbol_start_open_ms_bound(
            exchange=exchange,
            symbol=symbol,
            runtime_context=runtime_context,
        ),
    )


def fetch_symbol_open_interest(
    *,
    dependencies: BronzeSymbolFetchDependencies,
    runtime_context: BronzeRuntimeBoundsContext,
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[OpenInterestPoint]], None] | None = None,
) -> list[OpenInterestPoint]:
    """Fetch one open-interest symbol using loader-provided adapter dependencies."""

    return fetch_service.fetch_symbol_open_interest(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=dependencies.open_times_in_lake_by_dataset,
        timeframe_normalizer=dependencies.normalize_open_interest_timeframe,
        symbol_normalizer=dependencies.normalize_storage_symbol,
        interval_ms_resolver=dependencies.open_interest_interval_to_milliseconds,
        now_open_resolver=dependencies.last_closed_open_ms,
        ranges_builder=dependencies.missing_ranges_ms,
        history_fetcher=dependencies.fetch_open_interest_all_history,
        range_fetcher=dependencies.fetch_open_interest_range,
        latest_open_time_reader=dependencies.latest_open_time_in_lake_by_dataset,
        tail_delta_only=runtime_context.tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=_symbol_start_open_ms_bound(
            exchange=exchange,
            symbol=symbol,
            runtime_context=runtime_context,
        ),
    )


def fetch_symbol_funding(
    *,
    dependencies: BronzeSymbolFetchDependencies,
    runtime_context: BronzeRuntimeBoundsContext,
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[FundingPoint]], None] | None = None,
) -> list[FundingPoint]:
    """Fetch one funding symbol using loader-provided adapter dependencies."""

    return fetch_service.fetch_symbol_funding(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=dependencies.open_times_in_lake_by_dataset,
        timeframe_normalizer=dependencies.normalize_funding_timeframe,
        symbol_normalizer=dependencies.normalize_storage_symbol,
        interval_ms_resolver=dependencies.funding_interval_to_milliseconds,
        now_open_resolver=dependencies.last_closed_open_ms,
        ranges_builder=dependencies.missing_ranges_ms,
        history_fetcher=dependencies.fetch_funding_all_history,
        range_fetcher=dependencies.fetch_funding_range,
        latest_open_time_reader=dependencies.latest_open_time_in_lake_by_dataset,
        tail_delta_only=runtime_context.tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=_symbol_start_open_ms_bound(
            exchange=exchange,
            symbol=symbol,
            runtime_context=runtime_context,
        ),
    )


def fetch_symbol_volatility_index_data(
    *,
    dependencies: BronzeSymbolFetchDependencies,
    runtime_context: BronzeRuntimeBoundsContext,
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[VolatilityPoint]], None] | None = None,
) -> list[VolatilityPoint]:
    """Fetch one volatility-index symbol using loader-provided adapter dependencies."""

    return fetch_service.fetch_symbol_volatility(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        dataset_type="volatility_index_data",
        open_times_reader=dependencies.open_times_in_lake_by_dataset,
        timeframe_normalizer=dependencies.normalize_volatility_timeframe,
        interval_ms_resolver=dependencies.volatility_interval_to_milliseconds,
        now_open_resolver=dependencies.last_closed_open_ms,
        ranges_builder=dependencies.missing_ranges_ms,
        history_fetcher=dependencies.fetch_volatility_index_all_history,
        range_fetcher=dependencies.fetch_volatility_index_range,
        latest_open_time_reader=dependencies.latest_open_time_in_lake_by_dataset,
        tail_delta_only=runtime_context.tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=_symbol_start_open_ms_bound(
            exchange=exchange,
            symbol=symbol,
            runtime_context=runtime_context,
        ),
    )


def fetch_symbol_trades(
    *,
    dependencies: BronzeSymbolFetchDependencies,
    runtime_context: BronzeRuntimeBoundsContext,
    exchange: Exchange,
    market: TradeMarket,
    symbol: str,
    lake_root: str,
    on_history_chunk: Callable[[list[TradeTick | OptionTradeTick]], None] | None = None,
) -> list[TradeTick | OptionTradeTick]:
    """Fetch one trade symbol using loader-provided adapter dependencies."""

    return fetch_service.fetch_symbol_trades(
        exchange=exchange,
        market=market,
        symbol=symbol,
        lake_root=lake_root,
        symbol_normalizer=dependencies.normalize_storage_symbol,
        history_fetcher=dependencies.fetch_trades_all_history,
        range_fetcher=dependencies.fetch_trades_range,
        latest_open_time_reader=dependencies.latest_open_time_in_lake_by_dataset,
        tail_delta_only=runtime_context.tail_delta_only,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=_symbol_start_open_ms_bound(
            exchange=exchange,
            symbol=symbol,
            runtime_context=runtime_context,
        ),
    )


def _symbol_start_open_ms_bound(
    *,
    exchange: Exchange,
    symbol: str,
    runtime_context: BronzeRuntimeBoundsContext,
) -> int | None:
    """Resolve one symbol start bound from the active Bronze runtime context."""

    return resolve_symbol_start_open_ms_bound(
        exchange=exchange,
        symbol=symbol,
        context=runtime_context,
    )
