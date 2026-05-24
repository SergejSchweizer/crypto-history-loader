"""Dataset registry contracts for planning and storage semantics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

from ingestion.spot import Exchange, Market
from ingestion.trades import TradeMarket

CliDataType = Literal[
    "spot",
    "perp",
    "oi",
    "funding",
    "perp_trades",
    "option_trades",
    "historical_volatility",
    "volatility_index_data",
]
DatasetType = Literal[
    "spot",
    "perp",
    "oi",
    "funding",
    "perp_trades",
    "option_trades",
    "historical_volatility",
    "volatility_index_data",
    "l2_orderbook",
]
InstrumentType = Literal["spot", "perp", "option"]
BronzeTaskKind = Literal["ohlcv", "open_interest", "funding", "trade", "volatility"]
SymbolGroup = Literal["symbols", "perp_trade_symbols", "option_trade_symbols"]


@dataclass(frozen=True)
class DatasetTask:
    """Generic Bronze dataset task identity used by registry-driven planning."""

    exchange: Exchange
    dataset_type: DatasetType
    instrument_type: InstrumentType
    symbol: str
    timeframe: str
    market: Market | TradeMarket | None = None

    def checkpoint_key(self) -> str:
        """Return stable checkpoint identity that does not depend on tuple layouts."""

        market_value = "" if self.market is None else str(self.market)
        return "|".join(
            (
                self.exchange,
                self.dataset_type,
                self.instrument_type,
                self.symbol,
                self.timeframe,
                market_value,
            )
        )

    def candle_tuple(self) -> tuple[Exchange, Market, str, str]:
        """Convert an OHLCV task to the legacy candle tuple contract."""

        if self.market not in {"spot", "perp"}:
            raise ValueError(f"Dataset task '{self.dataset_type}' is not an OHLCV task")
        return (self.exchange, cast(Market, self.market), self.symbol, self.timeframe)

    def interval_tuple(self) -> tuple[Exchange, str, str]:
        """Convert an interval task to the legacy exchange/symbol/timeframe tuple."""

        return (self.exchange, self.symbol, self.timeframe)

    def trade_tuple(self) -> tuple[Exchange, TradeMarket, str]:
        """Convert a trade task to the legacy exchange/market/symbol tuple."""

        if self.market not in {"spot", "perp", "option"}:
            raise ValueError(f"Dataset task '{self.dataset_type}' is not a trade task")
        return (self.exchange, self.market, self.symbol)


@dataclass(frozen=True)
class DatasetSpec:
    """Static dataset metadata shared by CLI planning, schema, and storage."""

    cli_data_type: CliDataType
    dataset_type: DatasetType
    instrument_type: InstrumentType
    bronze_task_kind: BronzeTaskKind
    symbol_group: SymbolGroup
    default_timeframe: str = "1m"
    market: Market | TradeMarket | None = None

    def build_task(self, *, exchange: Exchange, symbol: str, timeframe: str | None = None) -> DatasetTask:
        """Build one generic task using this dataset's storage semantics."""

        return DatasetTask(
            exchange=exchange,
            dataset_type=self.dataset_type,
            instrument_type=self.instrument_type,
            symbol=symbol,
            timeframe=timeframe or self.default_timeframe,
            market=self.market,
        )


DATASET_REGISTRY: dict[CliDataType, DatasetSpec] = {
    "spot": DatasetSpec(
        cli_data_type="spot",
        dataset_type="spot",
        instrument_type="spot",
        bronze_task_kind="ohlcv",
        symbol_group="symbols",
        market="spot",
    ),
    "perp": DatasetSpec(
        cli_data_type="perp",
        dataset_type="perp",
        instrument_type="perp",
        bronze_task_kind="ohlcv",
        symbol_group="symbols",
        market="perp",
    ),
    "oi": DatasetSpec(
        cli_data_type="oi",
        dataset_type="oi",
        instrument_type="perp",
        bronze_task_kind="open_interest",
        symbol_group="symbols",
        market="perp",
    ),
    "funding": DatasetSpec(
        cli_data_type="funding",
        dataset_type="funding",
        instrument_type="perp",
        bronze_task_kind="funding",
        symbol_group="symbols",
        market="perp",
    ),
    "perp_trades": DatasetSpec(
        cli_data_type="perp_trades",
        dataset_type="perp_trades",
        instrument_type="perp",
        bronze_task_kind="trade",
        symbol_group="perp_trade_symbols",
        default_timeframe="tick",
        market="perp",
    ),
    "option_trades": DatasetSpec(
        cli_data_type="option_trades",
        dataset_type="option_trades",
        instrument_type="option",
        bronze_task_kind="trade",
        symbol_group="option_trade_symbols",
        default_timeframe="tick",
        market="option",
    ),
    "historical_volatility": DatasetSpec(
        cli_data_type="historical_volatility",
        dataset_type="historical_volatility",
        instrument_type="perp",
        bronze_task_kind="volatility",
        symbol_group="symbols",
        market="perp",
    ),
    "volatility_index_data": DatasetSpec(
        cli_data_type="volatility_index_data",
        dataset_type="volatility_index_data",
        instrument_type="perp",
        bronze_task_kind="volatility",
        symbol_group="symbols",
        market="perp",
    ),
}


def dataset_spec(cli_data_type: CliDataType) -> DatasetSpec:
    """Return the registered spec for one CLI dataset name."""

    return DATASET_REGISTRY[cli_data_type]


def dataset_specs(cli_data_types: list[CliDataType]) -> list[DatasetSpec]:
    """Return registered specs in the caller-provided order."""

    return [dataset_spec(item) for item in cli_data_types]


def dataset_names_for_task_kind(task_kind: BronzeTaskKind) -> set[CliDataType]:
    """Return CLI dataset names handled by one Bronze task kind."""

    return {name for name, spec in DATASET_REGISTRY.items() if spec.bronze_task_kind == task_kind}
