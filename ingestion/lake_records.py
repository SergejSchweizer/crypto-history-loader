"""Bronze lake partition keys and source-shaped row mappers."""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime

from ingestion.funding import FundingPoint
from ingestion.lake_datasets import OI_DATASET_TYPE, ohlcv_dataset_type_for_market
from ingestion.lake_layout import PartitionKey
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick
from ingestion.volatility import VolatilityPoint


def utc_run_id() -> str:
    """Create a UTC run identifier for lake writes."""

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def candle_partition_key(candle: SpotCandle, market: str) -> PartitionKey:
    """Build partition key as exchange/instrument_type/symbol/timeframe/date."""

    return (
        candle.exchange,
        market,
        candle.symbol,
        candle.interval,
        candle.open_time.strftime("%Y-%m-%d"),
    )


def candle_record(candle: SpotCandle, market: str, run_id: str, ingested_at: datetime) -> dict[str, object]:
    """Convert a candle to bronze parquet row format with source-shaped fields."""

    return {
        "schema_version": "v1",
        "dataset_type": ohlcv_dataset_type_for_market(market),
        "exchange": candle.exchange,
        "symbol": candle.symbol,
        "instrument_type": market,
        "event_time": candle.open_time,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "source_endpoint": "public_market_data",
        "open_time": candle.open_time,
        "close_time": candle.close_time,
        "timeframe": candle.interval,
        "open_price": candle.open_price,
        "high_price": candle.high_price,
        "low_price": candle.low_price,
        "close_price": candle.close_price,
        "volume": candle.volume,
        "quote_volume": candle.quote_volume,
        "trade_count": candle.trade_count,
        "origin_payload": asdict(candle),
    }


def open_interest_partition_key(item: OpenInterestPoint, market: str) -> PartitionKey:
    """Build partition key for open-interest records."""

    return (
        item.exchange,
        market,
        item.symbol,
        item.interval,
        item.open_time.strftime("%Y-%m-%d"),
    )


def open_interest_record(
    item: OpenInterestPoint,
    market: str,
    run_id: str,
    ingested_at: datetime,
) -> dict[str, object]:
    """Convert open-interest point to bronze parquet row format."""

    return {
        "schema_version": "v1",
        "dataset_type": OI_DATASET_TYPE,
        "exchange": item.exchange,
        "symbol": item.symbol,
        "instrument_type": market,
        "event_time": item.open_time,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "source_endpoint": "public_open_interest",
        "open_time": item.open_time,
        "close_time": item.close_time,
        "timeframe": item.interval,
        "open_interest": item.open_interest,
        "open_interest_value": item.open_interest_value,
    }


def funding_partition_key(item: FundingPoint, market: str) -> PartitionKey:
    """Build partition key for funding records."""

    return (
        item.exchange,
        market,
        item.symbol,
        item.interval,
        item.open_time.strftime("%Y-%m-%d"),
    )


def funding_record(
    item: FundingPoint,
    market: str,
    run_id: str,
    ingested_at: datetime,
) -> dict[str, object]:
    """Convert funding point to bronze parquet row format."""

    return {
        "schema_version": "v1",
        "dataset_type": "funding",
        "exchange": item.exchange,
        "symbol": item.symbol,
        "instrument_type": market,
        "event_time": item.open_time,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "source_endpoint": "public_funding",
        "open_time": item.open_time,
        "close_time": item.close_time,
        "timeframe": item.interval,
        "funding_rate": item.funding_rate,
        "index_price": item.index_price,
        "mark_price": item.mark_price,
    }


def trade_partition_key(item: TradeTick | OptionTradeTick, market: TradeMarket) -> PartitionKey:
    """Build partition key for trade records."""

    return (
        item.exchange,
        market,
        item.symbol,
        "tick",
        item.trade_time.strftime("%Y-%m-%d"),
    )


def trade_record(
    item: TradeTick | OptionTradeTick,
    market: TradeMarket,
    run_id: str,
    ingested_at: datetime,
) -> dict[str, object]:
    """Convert trade tick to bronze parquet row format."""

    dataset_type = "option_trades" if market == "option" else "perps_trades"
    record: dict[str, object] = {
        "schema_version": "v1",
        "dataset_type": dataset_type,
        "exchange": item.exchange,
        "symbol": item.symbol,
        "instrument_type": market,
        "event_time": item.trade_time,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "source_endpoint": item.source_endpoint,
        "open_time": item.trade_time,
        "close_time": item.trade_time,
        "timeframe": "tick",
        "trade_id": item.trade_id,
        "price": item.price,
        "quantity": item.quantity,
        "side": item.side,
        "is_maker": item.is_maker,
    }
    if isinstance(item, OptionTradeTick):
        record["instrument_name"] = item.instrument_name
        record["expiry"] = item.expiry
        record["strike"] = item.strike
        record["option_type"] = item.option_type
    return record


def volatility_partition_key(item: VolatilityPoint, market: str) -> PartitionKey:
    """Build partition key for volatility records."""

    return (
        item.exchange,
        market,
        item.symbol,
        item.interval,
        item.open_time.strftime("%Y-%m-%d"),
    )


def volatility_record(
    item: VolatilityPoint,
    market: str,
    run_id: str,
    ingested_at: datetime,
) -> dict[str, object]:
    """Convert volatility point to bronze parquet row format."""

    return {
        "schema_version": "v1",
        "dataset_type": item.dataset_type,
        "exchange": item.exchange,
        "symbol": item.symbol,
        "instrument_type": market,
        "event_time": item.open_time,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "source_endpoint": item.source_endpoint,
        "open_time": item.open_time,
        "close_time": item.close_time,
        "timeframe": item.interval,
        "value": item.value,
    }
