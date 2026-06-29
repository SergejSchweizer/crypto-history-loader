"""Parquet lake writing utilities for fetched market data."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from ingestion import lake_dataframe as _lake_dataframe
from ingestion import lake_queries as _lake_queries
from ingestion import lake_writes as _lake_writes
from ingestion.funding import FundingPoint
from ingestion.lake_datasets import OI_DATASET_TYPE, ohlcv_dataset_type_for_market
from ingestion.lake_layout import (
    PartitionKey,
    partition_path,
)
from ingestion.lake_layout import (
    partition_data_files as _partition_data_files,
)
from ingestion.lake_sidecars import (
    ensure_bronze_sidecars,
)
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick
from ingestion.volatility import VolatilityPoint

__all__ = ["ensure_bronze_sidecars", "partition_path"]

NaturalKey = _lake_writes.NaturalKey
latest_open_time_in_lake = _lake_queries.latest_open_time_in_lake
latest_open_time_in_lake_by_dataset = _lake_queries.latest_open_time_in_lake_by_dataset
open_time_bounds_in_lake_by_dataset = _lake_queries.open_time_bounds_in_lake_by_dataset
open_times_in_lake = _lake_queries.open_times_in_lake
open_times_in_lake_by_dataset = _lake_queries.open_times_in_lake_by_dataset
partition_dates_in_lake_by_dataset = _lake_queries.partition_dates_in_lake_by_dataset
load_combined_dataframe_from_lake = _lake_dataframe.load_combined_dataframe_from_lake
record_natural_key = _lake_writes.record_natural_key
merge_and_deduplicate_rows = _lake_writes.merge_and_deduplicate_rows
_require_pyarrow = _lake_writes.require_pyarrow
_write_partition_file = _lake_writes.write_partition_file
_write_grouped_rows = _lake_writes.write_grouped_rows


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
    """Convert a candle to bronze parquet row format (source-shaped fields)."""

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
    """Convert open-interest point to bronze parquet row format (source-shaped fields)."""

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
    """Convert funding point to parquet-lake row format."""

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
    """Convert trade tick to parquet-lake row format."""

    dataset_type = "option_trades" if market == "option" else "perp_trades"
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
    """Convert volatility point to parquet-lake row format."""

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


def load_spot_candles_from_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[SpotCandle]:
    """Load all stored candles for one exchange/symbol/timeframe from parquet lake.

    Uses batched parquet reads to avoid materializing complete files in memory.
    """

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    partition_root = (
        Path(lake_root)
        / f"dataset_type={ohlcv_dataset_type_for_market(market)}"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not partition_root.exists():
        return []

    candles_by_open_time: dict[datetime, SpotCandle] = {}
    for data_file in _partition_data_files(partition_root):
        parquet_file = pq.ParquetFile(data_file)  # type: ignore[no-untyped-call]
        for batch in parquet_file.iter_batches(batch_size=10_000):  # type: ignore[no-untyped-call]
            for row in batch.to_pylist():
                open_time = row.get("open_time")
                close_time = row.get("close_time")
                if not isinstance(open_time, datetime) or not isinstance(close_time, datetime):
                    continue
                quote_volume_raw = row.get("quote_volume")
                candles_by_open_time[open_time] = SpotCandle(
                    exchange=str(row.get("exchange", exchange)),
                    symbol=str(row.get("symbol", symbol)),
                    interval=str(row.get("timeframe", timeframe)),
                    open_time=open_time,
                    close_time=close_time,
                    open_price=float(row.get("open_price", row.get("open", 0.0))),
                    high_price=float(row.get("high_price", row.get("high", 0.0))),
                    low_price=float(row.get("low_price", row.get("low", 0.0))),
                    close_price=float(row.get("close_price", row.get("close", 0.0))),
                    volume=float(row.get("volume", 0.0)),
                    quote_volume=None if quote_volume_raw is None else float(quote_volume_raw),
                    trade_count=int(row.get("trade_count", 0)),
                )
    return [candles_by_open_time[key] for key in sorted(candles_by_open_time)]


def load_open_interest_from_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[OpenInterestPoint]:
    """Load all stored open-interest rows for one exchange/symbol/timeframe from parquet lake."""

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    partition_root = (
        Path(lake_root)
        / f"dataset_type={OI_DATASET_TYPE}"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not partition_root.exists():
        return []

    items_by_open_time: dict[datetime, OpenInterestPoint] = {}
    for data_file in _partition_data_files(partition_root):
        parquet_file = pq.ParquetFile(data_file)  # type: ignore[no-untyped-call]
        for batch in parquet_file.iter_batches(batch_size=10_000):  # type: ignore[no-untyped-call]
            for row in batch.to_pylist():
                open_time = row.get("open_time")
                close_time = row.get("close_time")
                if not isinstance(open_time, datetime) or not isinstance(close_time, datetime):
                    continue
                items_by_open_time[open_time] = OpenInterestPoint(
                    exchange=str(row.get("exchange", exchange)),
                    symbol=str(row.get("symbol", symbol)),
                    interval=str(row.get("timeframe", timeframe)),
                    open_time=open_time,
                    close_time=close_time,
                    open_interest=float(row.get("open_interest", 0.0)),
                    open_interest_value=float(row.get("open_interest_value", 0.0)),
                )
    return [items_by_open_time[key] for key in sorted(items_by_open_time)]


def load_funding_from_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[FundingPoint]:
    """Load all stored funding rows for one exchange/symbol/timeframe from parquet lake."""

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    partition_root = (
        Path(lake_root)
        / "dataset_type=funding"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not partition_root.exists():
        return []

    items_by_open_time: dict[datetime, FundingPoint] = {}
    for data_file in _partition_data_files(partition_root):
        parquet_file = pq.ParquetFile(data_file)  # type: ignore[no-untyped-call]
        for batch in parquet_file.iter_batches(batch_size=10_000):  # type: ignore[no-untyped-call]
            for row in batch.to_pylist():
                open_time = row.get("open_time")
                close_time = row.get("close_time")
                if not isinstance(open_time, datetime) or not isinstance(close_time, datetime):
                    continue
                items_by_open_time[open_time] = FundingPoint(
                    exchange=str(row.get("exchange", exchange)),
                    symbol=str(row.get("symbol", symbol)),
                    interval=str(row.get("timeframe", timeframe)),
                    open_time=open_time,
                    close_time=close_time,
                    funding_rate=float(row.get("funding_rate", 0.0)),
                    index_price=float(row.get("index_price", 0.0)),
                    mark_price=float(row.get("mark_price", 0.0)),
                )
    return [items_by_open_time[key] for key in sorted(items_by_open_time)]


def save_spot_candles_parquet_lake(
    candles_by_exchange: dict[str, dict[str, list[SpotCandle]]],
    market: str,
    lake_root: str,
) -> list[str]:
    """Save fetched candles to parquet lake partitions.

    Args:
        candles_by_exchange: Nested mapping ``exchange -> symbol_key -> candles``.
        market: Market type (`spot` or `perp`).
        lake_root: Root directory of parquet lake.

    Returns:
        List of absolute file paths written.

    Raises:
        RuntimeError: If ``pyarrow`` is unavailable.
    """

    pa, pq = _require_pyarrow()

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)
    dataset_type = ohlcv_dataset_type_for_market(market)

    grouped: defaultdict[PartitionKey, list[dict[str, object]]] = defaultdict(list)

    for symbol_map in candles_by_exchange.values():
        for candles in symbol_map.values():
            for candle in candles:
                key = candle_partition_key(candle=candle, market=market)
                grouped[key].append(candle_record(candle=candle, market=market, run_id=run_id, ingested_at=ingested_at))

    return _write_grouped_rows(
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
    """Save fetched open-interest data to parquet lake partitions."""

    pa, pq = _require_pyarrow()

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

    return _write_grouped_rows(
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
    """Save fetched funding rows to parquet lake partitions."""

    pa, pq = _require_pyarrow()

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)
    dataset_type = "funding"

    grouped: defaultdict[PartitionKey, list[dict[str, object]]] = defaultdict(list)
    for symbol_map in funding_by_exchange.values():
        for items in symbol_map.values():
            for item in items:
                key = funding_partition_key(item=item, market=market)
                grouped[key].append(funding_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at))

    return _write_grouped_rows(
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
    """Save fetched volatility rows to parquet lake partitions."""

    pa, pq = _require_pyarrow()

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

    return _write_grouped_rows(
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
    """Save fetched trade rows to parquet lake partitions."""

    pa, pq = _require_pyarrow()

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)
    dataset_type = "option_trades" if market == "option" else "perp_trades"

    grouped: defaultdict[PartitionKey, list[dict[str, object]]] = defaultdict(list)
    for symbol_map in trades_by_exchange.values():
        for items in symbol_map.values():
            for item in items:
                key = trade_partition_key(item=item, market=market)
                grouped[key].append(trade_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at))

    return _write_grouped_rows(
        pa=pa,
        pq=pq,
        lake_root=lake_root,
        dataset_type=dataset_type,
        run_id=run_id,
        grouped=grouped,
    )
