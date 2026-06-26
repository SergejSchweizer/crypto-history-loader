"""Storage orchestration for loader outputs."""

from __future__ import annotations

from collections.abc import Callable

from application.dto import LoaderStorageDTO, PersistOptionsDTO, PersistResultDTO
from ingestion.funding import FundingPoint
from ingestion.lake import (
    save_funding_parquet_lake,
    save_open_interest_parquet_lake,
    save_spot_candles_parquet_lake,
    save_trades_parquet_lake,
    save_volatility_parquet_lake,
)
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import Market, SpotCandle
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick
from ingestion.volatility import VolatilityPoint


def persist_loader_outputs_dto(
    storage: LoaderStorageDTO,
    options: PersistOptionsDTO,
    save_spot_lake_fn: Callable[..., list[str]] = save_spot_candles_parquet_lake,
    save_oi_lake_fn: Callable[..., list[str]] = save_open_interest_parquet_lake,
    save_funding_lake_fn: Callable[..., list[str]] = save_funding_parquet_lake,
    save_trades_lake_fn: Callable[..., list[str]] = save_trades_parquet_lake,
    save_volatility_lake_fn: Callable[..., list[str]] = save_volatility_parquet_lake,
) -> PersistResultDTO:
    """Persist fetched datasets to parquet lake."""

    result = PersistResultDTO()
    if options.save_parquet_lake:
        for market_key, candles_by_exchange in storage.candles.items():
            result.parquet_files.extend(
                save_spot_lake_fn(
                    candles_by_exchange=candles_by_exchange,
                    market=market_key,
                    lake_root=options.lake_root,
                )
            )
        if options.oi_requested:
            for market_key, oi_by_exchange in storage.open_interest.items():
                result.parquet_files.extend(
                    save_oi_lake_fn(
                        open_interest_by_exchange=oi_by_exchange,
                        market=market_key,
                        lake_root=options.lake_root,
                    )
                )
        if options.funding_requested:
            for market_key, funding_by_exchange in storage.funding.items():
                result.parquet_files.extend(
                    save_funding_lake_fn(
                        funding_by_exchange=funding_by_exchange,
                        market=market_key,
                        lake_root=options.lake_root,
                    )
                )
        if options.volatility_index_data_requested:
            for market_key, volatility_by_exchange in storage.volatility_index_data.items():
                result.parquet_files.extend(
                    save_volatility_lake_fn(
                        volatility_by_exchange=volatility_by_exchange,
                        market=market_key,
                        dataset_type="volatility_index_data",
                        lake_root=options.lake_root,
                    )
                )
        if options.trades_requested:
            for trade_market_key, trades_by_exchange in storage.trades.items():
                result.parquet_files.extend(
                    save_trades_lake_fn(
                        trades_by_exchange=trades_by_exchange,
                        market=trade_market_key,
                        lake_root=options.lake_root,
                    )
                )

    return result


def persist_loader_outputs(
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
    open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
    save_parquet_lake: bool,
    lake_root: str,
    oi_requested: bool,
    funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] | None = None,
    volatility_index_data_for_storage: dict[Market, dict[str, dict[str, list[VolatilityPoint]]]] | None = None,
    save_spot_lake_fn: Callable[..., list[str]] = save_spot_candles_parquet_lake,
    save_oi_lake_fn: Callable[..., list[str]] = save_open_interest_parquet_lake,
    save_funding_lake_fn: Callable[..., list[str]] = save_funding_parquet_lake,
    trades_for_storage: dict[TradeMarket, dict[str, dict[str, list[TradeTick | OptionTradeTick]]]] | None = None,
    trades_requested: bool = False,
    save_trades_lake_fn: Callable[..., list[str]] = save_trades_parquet_lake,
) -> dict[str, object]:
    """Compatibility wrapper that returns dict output for existing callers."""

    dto = persist_loader_outputs_dto(
        storage=LoaderStorageDTO(
            candles=candles_for_storage,
            open_interest=open_interest_for_storage,
            funding=funding_for_storage or {},
            volatility_index_data=volatility_index_data_for_storage or {},
            trades=trades_for_storage or {},
        ),
        options=PersistOptionsDTO(
            save_parquet_lake=save_parquet_lake,
            lake_root=lake_root,
            oi_requested=oi_requested,
            funding_requested=bool(funding_for_storage),
            volatility_index_data_requested=bool(volatility_index_data_for_storage),
            trades_requested=trades_requested,
        ),
        save_spot_lake_fn=save_spot_lake_fn,
        save_oi_lake_fn=save_oi_lake_fn,
        save_funding_lake_fn=save_funding_lake_fn,
        save_trades_lake_fn=save_trades_lake_fn,
    )
    return dto.to_output_dict()
