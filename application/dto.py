"""Shared DTOs for loader orchestration and service boundaries."""

from __future__ import annotations

from dataclasses import dataclass, field

from application.datasets import DatasetTask
from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import Exchange, Market, SpotCandle
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick
from ingestion.volatility import VolatilityPoint


def _empty_candle_rows() -> dict[tuple[Exchange, Market, str, str], list[SpotCandle]]:
    return {}


def _empty_candle_errors() -> dict[tuple[Exchange, Market, str, str], str]:
    return {}


def _empty_open_interest_rows() -> dict[tuple[Exchange, str, str], list[OpenInterestPoint]]:
    return {}


def _empty_open_interest_errors() -> dict[tuple[Exchange, str, str], str]:
    return {}


def _empty_funding_rows() -> dict[tuple[Exchange, str, str], list[FundingPoint]]:
    return {}


def _empty_funding_errors() -> dict[tuple[Exchange, str, str], str]:
    return {}


def _empty_volatility_rows() -> dict[tuple[Exchange, str, str], list[VolatilityPoint]]:
    return {}


def _empty_volatility_errors() -> dict[tuple[Exchange, str, str], str]:
    return {}


def _empty_trade_rows() -> dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]]:
    return {}


def _empty_trade_errors() -> dict[tuple[Exchange, TradeMarket, str], str]:
    return {}


def _empty_dataset_tasks() -> list[DatasetTask]:
    return []


def _empty_candle_storage() -> dict[Market, dict[str, dict[str, list[SpotCandle]]]]:
    return {}


def _empty_open_interest_storage() -> dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]]:
    return {}


def _empty_funding_storage() -> dict[Market, dict[str, dict[str, list[FundingPoint]]]]:
    return {}


def _empty_historical_volatility_storage() -> dict[Market, dict[str, dict[str, list[VolatilityPoint]]]]:
    return {}


def _empty_volatility_index_data_storage() -> dict[Market, dict[str, dict[str, list[VolatilityPoint]]]]:
    return {}


def _empty_trade_storage() -> dict[TradeMarket, dict[str, dict[str, list[TradeTick | OptionTradeTick]]]]:
    return {}


def _empty_parquet_files() -> list[str]:
    return []


@dataclass(frozen=True)
class CandleFetchTaskDTO:
    """One OHLCV fetch task request.

    Example:
        ```python
        task = CandleFetchTaskDTO(exchange="deribit", market="spot", symbol="BTC", timeframe="1m")
        ```
    """

    exchange: Exchange
    market: Market
    symbol: str
    timeframe: str


@dataclass(frozen=True)
class OpenInterestFetchTaskDTO:
    """One open-interest fetch task request.

    Example:
        ```python
        task = OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="1m")
        ```
    """

    exchange: Exchange
    symbol: str
    timeframe: str


@dataclass(frozen=True)
class FundingFetchTaskDTO:
    """One funding fetch task request.

    Example:
        ```python
        task = FundingFetchTaskDTO(exchange="deribit", symbol="ETH", timeframe="1h")
        ```
    """

    exchange: Exchange
    symbol: str
    timeframe: str


@dataclass(frozen=True)
class VolatilityFetchTaskDTO:
    """One volatility fetch task request."""

    exchange: Exchange
    symbol: str
    timeframe: str
    dataset_type: str


@dataclass(frozen=True)
class TradeFetchTaskDTO:
    """One trades fetch task request."""

    exchange: Exchange
    market: TradeMarket
    symbol: str


@dataclass
class CandleFetchResultDTO:
    """OHLCV fetch outcomes keyed by task tuple.

    Example:
        ```python
        result = CandleFetchResultDTO()
        ```
    """

    rows: dict[tuple[Exchange, Market, str, str], list[SpotCandle]] = field(default_factory=_empty_candle_rows)
    errors: dict[tuple[Exchange, Market, str, str], str] = field(default_factory=_empty_candle_errors)


@dataclass
class OpenInterestFetchResultDTO:
    """Open-interest fetch outcomes keyed by task tuple.

    Example:
        ```python
        result = OpenInterestFetchResultDTO()
        ```
    """

    rows: dict[tuple[Exchange, str, str], list[OpenInterestPoint]] = field(default_factory=_empty_open_interest_rows)
    errors: dict[tuple[Exchange, str, str], str] = field(default_factory=_empty_open_interest_errors)


@dataclass
class FundingFetchResultDTO:
    """Funding fetch outcomes keyed by task tuple.

    Example:
        ```python
        result = FundingFetchResultDTO()
        ```
    """

    rows: dict[tuple[Exchange, str, str], list[FundingPoint]] = field(default_factory=_empty_funding_rows)
    errors: dict[tuple[Exchange, str, str], str] = field(default_factory=_empty_funding_errors)


@dataclass
class VolatilityFetchResultDTO:
    """Volatility fetch outcomes keyed by task tuple."""

    rows: dict[tuple[Exchange, str, str], list[VolatilityPoint]] = field(default_factory=_empty_volatility_rows)
    errors: dict[tuple[Exchange, str, str], str] = field(default_factory=_empty_volatility_errors)


@dataclass
class TradeFetchResultDTO:
    """Trades fetch outcomes keyed by task tuple."""

    rows: dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]] = field(
        default_factory=_empty_trade_rows
    )
    errors: dict[tuple[Exchange, TradeMarket, str], str] = field(default_factory=_empty_trade_errors)


@dataclass(frozen=True)
class BronzeFetchPlanDTO:
    """Deterministic Bronze fetch plan shared by command orchestration.

    This contract centralizes symbol/data-type normalization and task ordering
    before execution so all Bronze dataset fetchers share the same scheduling
    semantics.
    """

    exchanges: list[Exchange]
    data_types: list[str]
    ohlcv_markets: list[Market]
    symbols: list[str]
    perp_trade_symbols: list[str]
    option_trade_symbols: list[str]
    candle_tasks: list[tuple[Exchange, Market, str, str]]
    oi_tasks: list[tuple[Exchange, str, str]]
    funding_tasks: list[tuple[Exchange, str, str]]
    historical_volatility_tasks: list[tuple[Exchange, str, str]]
    volatility_index_data_tasks: list[tuple[Exchange, str, str]]
    trade_tasks: list[tuple[Exchange, TradeMarket, str]]
    dataset_tasks: list[DatasetTask] = field(default_factory=_empty_dataset_tasks)


@dataclass(frozen=True)
class BronzeExecutionPolicyDTO:
    """Standardized Bronze runtime execution policy."""

    configured_concurrency: int
    effective_concurrency: int
    candle_concurrency: int
    oi_concurrency: int
    funding_concurrency: int
    trade_concurrency: int


@dataclass
class LoaderStorageDTO:
    """Normalized in-memory storage payload for loader side effects.

    Example:
        ```python
        storage = LoaderStorageDTO()
        ```
    """

    candles: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = field(default_factory=_empty_candle_storage)
    open_interest: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = field(
        default_factory=_empty_open_interest_storage
    )
    funding: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = field(default_factory=_empty_funding_storage)
    historical_volatility: dict[Market, dict[str, dict[str, list[VolatilityPoint]]]] = field(
        default_factory=_empty_historical_volatility_storage
    )
    volatility_index_data: dict[Market, dict[str, dict[str, list[VolatilityPoint]]]] = field(
        default_factory=_empty_volatility_index_data_storage
    )
    trades: dict[TradeMarket, dict[str, dict[str, list[TradeTick | OptionTradeTick]]]] = field(
        default_factory=_empty_trade_storage
    )


@dataclass(frozen=True)
class PersistOptionsDTO:
    """Controls which storage sinks are executed.

    Example:
        ```python
        options = PersistOptionsDTO(
            save_parquet_lake=True,
            lake_root="lake/bronze",
            oi_requested=True,
        )
        ```
    """

    save_parquet_lake: bool
    lake_root: str
    oi_requested: bool
    funding_requested: bool = False
    historical_volatility_requested: bool = False
    volatility_index_data_requested: bool = False
    trades_requested: bool = False


@dataclass
class PersistResultDTO:
    """Persist side-effect summary payload.

    Example:
        ```python
        result = PersistResultDTO(parquet_files=["lake/bronze/.../data.parquet"])
        ```
    """

    parquet_files: list[str] = field(default_factory=_empty_parquet_files)

    def to_output_dict(self) -> dict[str, object]:
        """Convert DTO to existing CLI output keys."""

        output: dict[str, object] = {}
        if self.parquet_files:
            output["_parquet_files"] = self.parquet_files
        return output
