"""Checkpoint filtering helpers for bronze loader resume behavior."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeAlias

from ingestion.spot import Exchange, Market
from ingestion.trades import TradeMarket

CandleTask: TypeAlias = tuple[Exchange, Market, str, str]
OpenInterestTask: TypeAlias = tuple[Exchange, str, str]
FundingTask: TypeAlias = tuple[Exchange, str, str]
TradeTask: TypeAlias = tuple[Exchange, TradeMarket, str]


@dataclass(frozen=True)
class PendingTaskGroups:
    """Pending task groups after applying completed-checkpoint filtering."""

    candle_tasks: list[CandleTask]
    oi_tasks: list[OpenInterestTask]
    funding_tasks: list[FundingTask]
    historical_volatility_tasks: list[FundingTask]
    volatility_index_data_tasks: list[FundingTask]
    trade_tasks: list[TradeTask]


def apply_checkpoint_filter(
    *,
    candle_tasks: list[CandleTask],
    oi_tasks: list[OpenInterestTask],
    funding_tasks: list[FundingTask],
    historical_volatility_tasks: list[FundingTask],
    volatility_index_data_tasks: list[FundingTask],
    trade_tasks: list[TradeTask],
    completed: dict[str, set[str]],
    candle_key_serializer: Callable[[CandleTask], str],
    oi_key_serializer: Callable[[OpenInterestTask], str],
    funding_key_serializer: Callable[[FundingTask], str],
    historical_volatility_key_serializer: Callable[[FundingTask], str],
    volatility_index_data_key_serializer: Callable[[FundingTask], str],
    trade_key_serializer: Callable[[TradeTask], str],
) -> PendingTaskGroups:
    """Filter task groups against completed checkpoint sets."""

    pending_candles = [task for task in candle_tasks if candle_key_serializer(task) not in completed["candle"]]
    pending_oi = [task for task in oi_tasks if oi_key_serializer(task) not in completed["oi"]]
    pending_funding = [task for task in funding_tasks if funding_key_serializer(task) not in completed["funding"]]
    pending_historical_volatility = [
        task for task in historical_volatility_tasks if historical_volatility_key_serializer(task) not in completed["historical_volatility"]
    ]
    pending_volatility_index_data = [
        task for task in volatility_index_data_tasks if volatility_index_data_key_serializer(task) not in completed["volatility_index_data"]
    ]
    pending_trades = [task for task in trade_tasks if trade_key_serializer(task) not in completed["trade"]]
    return PendingTaskGroups(
        candle_tasks=pending_candles,
        oi_tasks=pending_oi,
        funding_tasks=pending_funding,
        historical_volatility_tasks=pending_historical_volatility,
        volatility_index_data_tasks=pending_volatility_index_data,
        trade_tasks=pending_trades,
    )


def has_checkpoint_state(completed: dict[str, set[str]]) -> bool:
    """Return whether any checkpoint category has completed entries."""

    return any(completed[name] for name in ("candle", "oi", "funding", "historical_volatility", "volatility_index_data", "trade"))
