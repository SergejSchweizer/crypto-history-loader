"""Output/persistence helpers for bronze loader orchestration."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol, cast

from application.dto import (
    FundingFetchTaskDTO,
    LoaderStorageDTO,
    OpenInterestFetchTaskDTO,
    PersistOptionsDTO,
    TradeFetchTaskDTO,
)
from application.services.storage_service import persist_loader_outputs_dto
from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import Exchange, Market, SpotCandle
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick


class _LakeArgs(Protocol):
    save_parquet_lake: bool
    lake_root: str


class _PersistResult(Protocol):
    parquet_files: list[str]

    def to_output_dict(self) -> dict[str, object]: ...


class _CandleTaskLike(Protocol):
    @property
    def exchange(self) -> Any: ...

    @property
    def market(self) -> Any: ...

    @property
    def symbol(self) -> str: ...

    @property
    def timeframe(self) -> str: ...


def _extract_date_partition(file_path: str) -> str | None:
    marker = "/date="
    if marker not in file_path:
        return None
    tail = file_path.split(marker, 1)[1]
    return tail.split("/", 1)[0] if tail else None


class IncrementalPersistor:
    """Handle incremental parquet persistence and checkpoint updates."""

    def __init__(
        self,
        *,
        lake_root: str,
        mark_checkpoint_complete: Callable[[str, tuple[object, ...]], None],
        persist_fn: Callable[..., object] = persist_loader_outputs_dto,
    ) -> None:
        self.lake_root = lake_root
        self.mark_checkpoint_complete = mark_checkpoint_complete
        self.persist_fn = persist_fn
        self.incremental_parquet_files: list[str] = []
        self.logged_daily_partitions: set[tuple[str, str, str, str, str, str]] = set()
        self.streamed_candle_tasks: set[tuple[Exchange, Market, str, str]] = set()
        self.streamed_oi_tasks: set[tuple[Exchange, str, str]] = set()
        self.streamed_funding_tasks: set[tuple[Exchange, str, str]] = set()
        self.streamed_trade_tasks: set[tuple[Exchange, TradeMarket, str]] = set()
        self._lock = threading.Lock()

    def _log_new_daily_partitions(
        self,
        *,
        logger: logging.Logger,
        data_type: str,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        parquet_files: list[str],
    ) -> None:
        days = sorted({day for day in (_extract_date_partition(path) for path in parquet_files) if day is not None})
        new_days = [
            day
            for day in days
            if (data_type, exchange, market, symbol.upper(), timeframe, day) not in self.logged_daily_partitions
        ]
        for day in new_days:
            self.logged_daily_partitions.add((data_type, exchange, market, symbol.upper(), timeframe, day))
            logger.info(
                "Parquet daily file saved type=%s exchange=%s market=%s symbol=%s timeframe=%s day=%s",
                data_type,
                exchange,
                market,
                symbol.upper(),
                timeframe,
                day,
            )

    def _persist_candle_task(self, task: _CandleTaskLike, rows: list[SpotCandle], logger: logging.Logger) -> None:
        if not rows:
            return
        storage_result = cast(
            _PersistResult,
            self.persist_fn(
                storage=LoaderStorageDTO(
                    candles={
                        cast(Market, task.market): {
                            cast(Exchange, task.exchange): {task.symbol.upper(): rows},
                        },
                    },
                ),
                options=PersistOptionsDTO(
                    save_parquet_lake=True,
                    lake_root=self.lake_root,
                    oi_requested=False,
                    funding_requested=False,
                    trades_requested=False,
                ),
            ),
        )
        self.incremental_parquet_files.extend(storage_result.parquet_files)
        self._log_new_daily_partitions(
            logger=logger,
            data_type="ohlcv",
            exchange=task.exchange,
            market=task.market,
            symbol=task.symbol,
            timeframe=task.timeframe,
            parquet_files=storage_result.parquet_files,
        )

    def _persist_oi_task(
        self,
        task: OpenInterestFetchTaskDTO,
        rows: list[OpenInterestPoint],
        logger: logging.Logger,
    ) -> None:
        if not rows:
            return
        storage_result = cast(
            _PersistResult,
            self.persist_fn(
                storage=LoaderStorageDTO(open_interest={"perp": {task.exchange: {task.symbol.upper(): rows}}}),
                options=PersistOptionsDTO(
                    save_parquet_lake=True,
                    lake_root=self.lake_root,
                    oi_requested=True,
                    funding_requested=False,
                    trades_requested=False,
                ),
            ),
        )
        self.incremental_parquet_files.extend(storage_result.parquet_files)
        self._log_new_daily_partitions(
            logger=logger,
            data_type="oi",
            exchange=task.exchange,
            market="perp",
            symbol=task.symbol,
            timeframe=task.timeframe,
            parquet_files=storage_result.parquet_files,
        )

    def _persist_funding_task(
        self,
        task: FundingFetchTaskDTO,
        rows: list[FundingPoint],
        logger: logging.Logger,
    ) -> None:
        if not rows:
            return
        storage_result = cast(
            _PersistResult,
            self.persist_fn(
                storage=LoaderStorageDTO(funding={"perp": {task.exchange: {task.symbol.upper(): rows}}}),
                options=PersistOptionsDTO(
                    save_parquet_lake=True,
                    lake_root=self.lake_root,
                    oi_requested=False,
                    funding_requested=True,
                    trades_requested=False,
                ),
            ),
        )
        self.incremental_parquet_files.extend(storage_result.parquet_files)
        self._log_new_daily_partitions(
            logger=logger,
            data_type="funding",
            exchange=task.exchange,
            market="perp",
            symbol=task.symbol,
            timeframe=task.timeframe,
            parquet_files=storage_result.parquet_files,
        )

    def _persist_trade_task(
        self,
        task: TradeFetchTaskDTO,
        rows: list[TradeTick | OptionTradeTick],
        logger: logging.Logger,
    ) -> None:
        if not rows:
            return
        storage_result = cast(
            _PersistResult,
            self.persist_fn(
                storage=LoaderStorageDTO(trades={task.market: {task.exchange: {task.symbol.upper(): rows}}}),
                options=PersistOptionsDTO(
                    save_parquet_lake=True,
                    lake_root=self.lake_root,
                    oi_requested=False,
                    funding_requested=False,
                    trades_requested=True,
                ),
            ),
        )
        self.incremental_parquet_files.extend(storage_result.parquet_files)
        self._log_new_daily_partitions(
            logger=logger,
            data_type="option_trades" if task.market == "option" else "perp_trades",
            exchange=task.exchange,
            market=task.market,
            symbol=task.symbol,
            timeframe="tick",
            parquet_files=storage_result.parquet_files,
        )

    def on_candle_task_complete(self, task: _CandleTaskLike, rows: list[SpotCandle], logger: logging.Logger) -> None:
        if (task.exchange, task.market, task.symbol, task.timeframe) in self.streamed_candle_tasks:
            return
        self._persist_candle_task(task, rows, logger)

    def on_oi_task_complete(
        self,
        task: OpenInterestFetchTaskDTO,
        rows: list[OpenInterestPoint],
        logger: logging.Logger,
    ) -> None:
        if (task.exchange, task.symbol, task.timeframe) in self.streamed_oi_tasks:
            return
        self._persist_oi_task(task, rows, logger)

    def on_funding_task_complete(
        self,
        task: FundingFetchTaskDTO,
        rows: list[FundingPoint],
        logger: logging.Logger,
    ) -> None:
        if (task.exchange, task.symbol, task.timeframe) in self.streamed_funding_tasks:
            return
        self._persist_funding_task(task, rows, logger)

    def on_trade_task_complete(
        self,
        task: TradeFetchTaskDTO,
        rows: list[TradeTick | OptionTradeTick],
        logger: logging.Logger,
    ) -> None:
        if (task.exchange, task.market, task.symbol) in self.streamed_trade_tasks:
            return
        self._persist_trade_task(task, rows, logger)

    def on_candle_task_chunk(self, task: _CandleTaskLike, rows: list[SpotCandle], logger: logging.Logger) -> None:
        if not rows:
            return
        self.streamed_candle_tasks.add((task.exchange, task.market, task.symbol, task.timeframe))
        self._persist_candle_task(task, rows, logger)
        self.mark_checkpoint_complete("candle", (task.exchange, task.market, task.symbol, task.timeframe))

    def on_oi_task_chunk(
        self,
        task: OpenInterestFetchTaskDTO,
        rows: list[OpenInterestPoint],
        logger: logging.Logger,
    ) -> None:
        if not rows:
            return
        self.streamed_oi_tasks.add((task.exchange, task.symbol, task.timeframe))
        self._persist_oi_task(task, rows, logger)
        self.mark_checkpoint_complete("oi", (task.exchange, task.symbol, task.timeframe))

    def on_funding_task_chunk(
        self,
        task: FundingFetchTaskDTO,
        rows: list[FundingPoint],
        logger: logging.Logger,
    ) -> None:
        if not rows:
            return
        self.streamed_funding_tasks.add((task.exchange, task.symbol, task.timeframe))
        self._persist_funding_task(task, rows, logger)
        self.mark_checkpoint_complete("funding", (task.exchange, task.symbol, task.timeframe))

    def on_trade_task_chunk(
        self,
        task: TradeFetchTaskDTO,
        rows: list[TradeTick | OptionTradeTick],
        logger: logging.Logger,
    ) -> None:
        if not rows:
            return
        with self._lock:
            self.streamed_trade_tasks.add((task.exchange, task.market, task.symbol))
            self._persist_trade_task(task, rows, logger)


def finalize_bronze_output(
    *,
    logger: logging.Logger,
    output: dict[str, object],
    tasks: list[tuple[Exchange, Market, str, str]],
    oi_tasks: list[tuple[Exchange, str, str]],
    funding_tasks: list[tuple[Exchange, str, str]],
    trade_tasks: list[tuple[Exchange, TradeMarket, str]],
    task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]],
    task_errors: dict[tuple[Exchange, Market, str, str], str],
    oi_results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]],
    oi_errors: dict[tuple[Exchange, str, str], str],
    funding_results: dict[tuple[Exchange, str, str], list[FundingPoint]],
    funding_errors: dict[tuple[Exchange, str, str], str],
    trade_results: dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]],
    trade_errors: dict[tuple[Exchange, TradeMarket, str], str],
    multi_market: bool,
    oi_requested: bool,
    funding_requested: bool,
    perp_trades_requested: bool,
    option_trades_requested: bool,
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
    open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
    funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]],
    trades_for_storage: dict[TradeMarket, dict[str, dict[str, list[TradeTick | OptionTradeTick]]]],
    ohlcv_markets: list[Market],
    args: _LakeArgs,
    incremental_parquet_on_fetch: bool,
    incremental_parquet_files: list[str],
    oi_dataset_type: str,
    sidecar_path_list_fn: Callable[[list[str], str], list[str]],
    ensure_bronze_sidecars_fn: Callable[..., list[str]],
    populate_ohlcv_output_fn: Callable[..., None],
    populate_oi_output_fn: Callable[..., None],
    populate_funding_output_fn: Callable[..., None],
    populate_trades_output_fn: Callable[..., None],
    symbol_progress_rows_fn: Callable[..., list[dict[str, object]]],
    fairness_rows: list[dict[str, object]] | None,
    trade_error_breakdown_fn: Callable[[dict[tuple[str, str, str], str]], dict[str, int]],
    candle_serializer: Callable[[SpotCandle], dict[str, object]],
    persist_fn: Callable[..., object] = persist_loader_outputs_dto,
) -> None:
    logger.info(
        "Fetch summary spot/perp: success=%s failed=%s | oi: success=%s failed=%s | "
        "funding: success=%s failed=%s | trades: success=%s failed=%s",
        len(task_results),
        len(task_errors),
        len(oi_results),
        len(oi_errors),
        len(funding_results),
        len(funding_errors),
        len(trade_results),
        len(trade_errors),
    )
    fairness = fairness_rows
    if fairness is None:
        fairness = symbol_progress_rows_fn(
            candle_tasks=cast(list[tuple[str, str, str, str]], tasks),
            oi_tasks=cast(list[tuple[str, str, str]], oi_tasks),
            funding_tasks=cast(list[tuple[str, str, str]], funding_tasks),
            trade_tasks=cast(list[tuple[str, str, str]], trade_tasks),
            candle_results=cast(dict[tuple[str, str, str, str], object], task_results),
            oi_results=cast(dict[tuple[str, str, str], object], oi_results),
            funding_results=cast(dict[tuple[str, str, str], object], funding_results),
            trade_results=cast(dict[tuple[str, str, str], object], trade_results),
        )
    if fairness:
        logger.info("Bronze per-symbol progress: %s", fairness)

    populate_ohlcv_output_fn(
        output=output,
        tasks=tasks,
        task_results=task_results,
        task_errors=task_errors,
        multi_market=multi_market,
        candle_serializer=candle_serializer,
        candles_for_storage=candles_for_storage,
    )
    if oi_requested:
        populate_oi_output_fn(
            output=output,
            tasks=oi_tasks,
            results=oi_results,
            errors=oi_errors,
            multi_market=multi_market,
            storage=open_interest_for_storage,
        )
    if funding_requested:
        populate_funding_output_fn(
            output=output,
            tasks=funding_tasks,
            results=funding_results,
            errors=funding_errors,
            multi_market=multi_market,
            storage=funding_for_storage,
        )
    if perp_trades_requested or option_trades_requested:
        populate_trades_output_fn(
            output=output,
            tasks=trade_tasks,
            results=trade_results,
            errors=trade_errors,
            multi_market=multi_market,
            storage=trades_for_storage,
        )

    if args.save_parquet_lake and not incremental_parquet_on_fetch:
        try:
            storage_result = cast(
                _PersistResult,
                persist_fn(
                    storage=LoaderStorageDTO(
                        candles=candles_for_storage,
                        open_interest=open_interest_for_storage,
                        funding=funding_for_storage,
                        trades=trades_for_storage,
                    ),
                    options=PersistOptionsDTO(
                        save_parquet_lake=True,
                        lake_root=args.lake_root,
                        oi_requested=oi_requested,
                        funding_requested=funding_requested,
                        trades_requested=perp_trades_requested or option_trades_requested,
                    ),
                ),
            )
            output.update(storage_result.to_output_dict())
        except Exception as exc:  # noqa: BLE001
            output["_parquet_error"] = str(exc)
            logger.exception("Parquet lake write failed")
    elif incremental_parquet_on_fetch:
        output["_parquet_files"] = incremental_parquet_files

    if args.save_parquet_lake:
        parquet_files = cast(list[str], output.get("_parquet_files", []))
        selected_dataset_types: set[str] = set()
        if any(market == "spot" for market in ohlcv_markets):
            selected_dataset_types.add("spot")
        if any(market == "perp" for market in ohlcv_markets):
            selected_dataset_types.add("perp")
        if oi_requested:
            selected_dataset_types.add(oi_dataset_type)
        if funding_requested:
            selected_dataset_types.add("funding")
        if perp_trades_requested:
            selected_dataset_types.add("perp_trades")
        if option_trades_requested:
            selected_dataset_types.add("option_trades")
        repaired_parquet_files = ensure_bronze_sidecars_fn(
            lake_root=args.lake_root,
            dataset_types=sorted(selected_dataset_types),
            log_fn=logger.info,
        )
        if repaired_parquet_files:
            parquet_files = sorted(set(parquet_files).union(repaired_parquet_files))
            output["_parquet_files"] = parquet_files
        output["_manifest_files"] = sidecar_path_list_fn(parquet_files, ".json")
        output["_plot_files"] = sidecar_path_list_fn(parquet_files, ".png")

    if perp_trades_requested or option_trades_requested:
        breakdown = trade_error_breakdown_fn(cast(dict[tuple[str, str, str], str], trade_errors))
        output["_trade_error_breakdown"] = breakdown
        trade_parquet_files = sorted(
            {
                str(Path(path).resolve())
                for path in cast(list[str], output.get("_parquet_files", []))
                if ("dataset_type=perp_trades" in path or "dataset_type=option_trades" in path)
                and path.endswith(".parquet")
            }
        )
        logger.info(
            "Trades bronze summary tasks_total=%s tasks_success=%s tasks_failed=%s "
            "failed_net_unreachable=%s failed_net_timeout=%s failed_other=%s rows_total=%s "
            "parquet_files_written=%s lake_root=%s",
            len(trade_tasks),
            len(trade_tasks) - breakdown["total"],
            breakdown["total"],
            breakdown["net_unreachable"],
            breakdown["net_timeout"],
            breakdown["other"],
            sum(len(rows) for rows in trade_results.values()),
            len(trade_parquet_files),
            args.lake_root,
        )
        if trade_parquet_files:
            logger.info("Trades bronze parquet files: %s", trade_parquet_files)
        if trade_errors:
            logger.error("Trades bronze task errors: %s", trade_errors)
