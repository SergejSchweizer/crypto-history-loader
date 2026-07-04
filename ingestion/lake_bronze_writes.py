"""Bronze parquet lake save APIs for fetched market data."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

from ingestion.funding import FundingPoint
from ingestion.lake_datasets import OI_DATASET_TYPE, ohlcv_dataset_type_for_market
from ingestion.lake_layout import PartitionKey
from ingestion.lake_records import (
    candle_partition_key,
    candle_record,
    funding_partition_key,
    funding_record,
    open_interest_partition_key,
    open_interest_record,
    trade_partition_key,
    trade_record,
    utc_run_id,
    volatility_partition_key,
    volatility_record,
)
from ingestion.lake_writes import require_pyarrow, write_grouped_rows
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot_ohlcv import SpotCandle
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick
from ingestion.volatility import VolatilityPoint


def save_spot_ohlcv_candles_parquet_lake(
    candles_by_exchange: dict[str, dict[str, list[SpotCandle]]],
    market: str,
    lake_root: str,
) -> list[str]:
    """Save fetched candles to Bronze parquet lake partitions."""

    pa, pq = require_pyarrow()

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)
    dataset_type = ohlcv_dataset_type_for_market(market)

    grouped: defaultdict[PartitionKey, list[dict[str, object]]] = defaultdict(list)

    for symbol_map in candles_by_exchange.values():
        for candles in symbol_map.values():
            for candle in candles:
                key = candle_partition_key(candle=candle, market=market)
                grouped[key].append(candle_record(candle=candle, market=market, run_id=run_id, ingested_at=ingested_at))

    return write_grouped_rows(
        pa=pa,
        pq=pq,
        lake_root=lake_root,
        dataset_type=dataset_type,
        run_id=run_id,
        grouped=grouped,
    )


def save_open_interest_parquet_lake(
    open_interest_by_exchange: dict[str, dict[str, list[OpenInterestPoint]]],
    market: str,
    lake_root: str,
) -> list[str]:
    """Save fetched open-interest data to Bronze parquet lake partitions."""

    pa, pq = require_pyarrow()

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)
    dataset_type = OI_DATASET_TYPE

    grouped: defaultdict[PartitionKey, list[dict[str, object]]] = defaultdict(list)
    for symbol_map in open_interest_by_exchange.values():
        for items in symbol_map.values():
            for item in items:
                key = open_interest_partition_key(item=item, market=market)
                grouped[key].append(
                    open_interest_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at)
                )

    return write_grouped_rows(
        pa=pa,
        pq=pq,
        lake_root=lake_root,
        dataset_type=dataset_type,
        run_id=run_id,
        grouped=grouped,
    )


def save_funding_parquet_lake(
    funding_by_exchange: dict[str, dict[str, list[FundingPoint]]],
    market: str,
    lake_root: str,
) -> list[str]:
    """Save fetched funding rows to Bronze parquet lake partitions."""

    pa, pq = require_pyarrow()

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)
    dataset_type = "funding"

    grouped: defaultdict[PartitionKey, list[dict[str, object]]] = defaultdict(list)
    for symbol_map in funding_by_exchange.values():
        for items in symbol_map.values():
            for item in items:
                key = funding_partition_key(item=item, market=market)
                grouped[key].append(funding_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at))

    return write_grouped_rows(
        pa=pa,
        pq=pq,
        lake_root=lake_root,
        dataset_type=dataset_type,
        run_id=run_id,
        grouped=grouped,
    )


def save_volatility_parquet_lake(
    volatility_by_exchange: dict[str, dict[str, list[VolatilityPoint]]],
    market: str,
    dataset_type: str,
    lake_root: str,
) -> list[str]:
    """Save fetched volatility rows to Bronze parquet lake partitions."""

    pa, pq = require_pyarrow()

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)
    grouped: defaultdict[PartitionKey, list[dict[str, object]]] = defaultdict(list)
    for symbol_map in volatility_by_exchange.values():
        for items in symbol_map.values():
            for item in items:
                key = volatility_partition_key(item=item, market=market)
                record = volatility_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at)
                record["dataset_type"] = dataset_type
                grouped[key].append(record)

    return write_grouped_rows(
        pa=pa,
        pq=pq,
        lake_root=lake_root,
        dataset_type=dataset_type,
        run_id=run_id,
        grouped=grouped,
    )


def save_trades_parquet_lake(
    trades_by_exchange: dict[str, dict[str, list[TradeTick | OptionTradeTick]]],
    market: TradeMarket,
    lake_root: str,
) -> list[str]:
    """Save fetched trade rows to Bronze parquet lake partitions."""

    pa, pq = require_pyarrow()

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)
    dataset_type = "options_trades" if market == "option" else "perps_trades"

    grouped: defaultdict[PartitionKey, list[dict[str, object]]] = defaultdict(list)
    for symbol_map in trades_by_exchange.values():
        for items in symbol_map.values():
            for item in items:
                key = trade_partition_key(item=item, market=market)
                grouped[key].append(trade_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at))

    return write_grouped_rows(
        pa=pa,
        pq=pq,
        lake_root=lake_root,
        dataset_type=dataset_type,
        run_id=run_id,
        grouped=grouped,
    )
