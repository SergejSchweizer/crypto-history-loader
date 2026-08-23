"""Bronze loader workflow coordinator."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from api.commands.loader_output import BronzeRunState, IncrementalPersistor, finalize_bronze_output
from application.dto import BronzeExecutionPolicyDTO, BronzeFetchPlanDTO
from application.services.bronze_runtime_service import (
    BronzeRuntimeBoundsContext,
    CheckpointDataset,
    add_completed_checkpoint_key,
    apply_checkpoint_filter_with_key_maps,
    bronze_checkpoint_key_maps,
    checkpoint_task_keys,
    has_checkpoint_state,
)


@dataclass(frozen=True)
class BronzeWorkflowDependencies:
    """Patchable side-effect and compatibility hooks used by the Bronze workflow."""

    configure_bronze_start_bounds: Callable[[argparse.Namespace, logging.Logger], None]
    current_runtime_bounds_context: Callable[[], BronzeRuntimeBoundsContext]
    single_instance_lock: Callable[[str], AbstractContextManager[object]]
    single_instance_error: type[Exception]
    build_bronze_fetch_plan: Callable[..., BronzeFetchPlanDTO]
    build_bronze_execution_policy: Callable[[], BronzeExecutionPolicyDTO]
    bronze_checkpoint_path: Callable[[], Path]
    bronze_checkpoint_fingerprint: Callable[..., str]
    load_bronze_checkpoint: Callable[..., dict[str, set[str]]]
    hydrate_checkpoint_aliases: Callable[..., None]
    write_bronze_checkpoint: Callable[..., None]
    fetch_all_task_groups: Callable[..., object]
    persist_loader_outputs: Callable[..., object]
    sidecar_path_list: Callable[..., list[str]]
    ensure_bronze_sidecars: Callable[..., list[str]]
    populate_ohlcv_output: Callable[..., None]
    populate_open_interest_output: Callable[..., None]
    populate_funding_output: Callable[..., None]
    populate_volatility_output: Callable[..., None]
    populate_trades_output: Callable[..., None]
    symbol_progress_rows: Callable[..., list[dict[str, object]]]
    symbol_progress_rows_from_dataset_tasks: Callable[..., list[dict[str, object]]]
    trade_error_breakdown: Callable[..., dict[str, int]]
    candle_serializer: Callable[..., dict[str, object]]
    open_interest_dataset_type: str


def run_bronze_build(
    *,
    args: argparse.Namespace,
    logger: logging.Logger,
    dependencies: BronzeWorkflowDependencies,
) -> None:
    """Run the Bronze build workflow using injected command dependencies."""

    dependencies.configure_bronze_start_bounds(args, logger)
    if dependencies.current_runtime_bounds_context().tail_delta_only:
        today_start = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        logger.info(
            "Bronze default tail-mode cap enabled delta_scope=today today_start_utc=%s",
            today_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

    try:
        with dependencies.single_instance_lock(".run/crypto-loader.lock"):
            plan = dependencies.build_bronze_fetch_plan(args=args, logger=logger)
            ohlcv_markets = plan.ohlcv_markets
            data_types = plan.data_types
            open_interest_requested = "open_interest" in data_types
            funding_requested = "funding" in data_types
            volatility_index_data_requested = "volatility_index_data" in data_types
            perps_trades_requested = "perps_trades" in data_types
            options_trades_requested = "options_trades" in data_types
            multi_market = len(data_types) > 1
            state = BronzeRunState.from_plan(plan)
            logger.info(
                "Deterministic schedule markets=%s symbols=%s perp_trade_symbols=%s options_trade_symbols=%s",
                data_types,
                plan.symbols,
                plan.perp_trade_symbols,
                plan.options_trade_symbols,
            )
            key_maps = bronze_checkpoint_key_maps(plan)
            candle_key_map = key_maps.candle
            open_interest_key_map = key_maps.open_interest
            funding_key_map = key_maps.funding
            volatility_key_map = key_maps.volatility_index_data
            trade_key_map = key_maps.trade
            checkpoint_path = dependencies.bronze_checkpoint_path()
            checkpoint_enabled = bool(args.save_parquet_lake) or checkpoint_path.exists()
            checkpoint_fingerprint = dependencies.bronze_checkpoint_fingerprint(args=args, plan=plan)
            empty_checkpoint: dict[str, set[str]] = {
                "candle": set(),
                "open_interest": set(),
                "funding": set(),
                "volatility_index_data": set(),
                "trade": set(),
            }
            if checkpoint_enabled and bool(args.tail_delta_only):
                checkpoint_completed = dependencies.load_bronze_checkpoint(
                    path=checkpoint_path,
                    fingerprint=checkpoint_fingerprint,
                    logger=logger,
                )
            else:
                checkpoint_completed = empty_checkpoint
                if checkpoint_path.exists() and not bool(args.tail_delta_only):
                    logger.info(
                        "Ignoring Bronze checkpoint '%s' for full-gap-fill run; lake gaps will be rescanned",
                        checkpoint_path,
                    )
            dependencies.hydrate_checkpoint_aliases(
                completed=checkpoint_completed,
                candle_tasks=state.candle_tasks,
                open_interest_tasks=state.open_interest_tasks,
                funding_tasks=state.funding_tasks,
                volatility_index_data_tasks=state.volatility_index_data_tasks,
                trade_tasks=state.trade_tasks,
                candle_key_map=candle_key_map,
                open_interest_key_map=open_interest_key_map,
                funding_key_map=funding_key_map,
                volatility_key_map=volatility_key_map,
                trade_key_map=trade_key_map,
            )

            pending_tasks = apply_checkpoint_filter_with_key_maps(
                candle_tasks=state.candle_tasks,
                open_interest_tasks=state.open_interest_tasks,
                funding_tasks=state.funding_tasks,
                volatility_index_data_tasks=state.volatility_index_data_tasks,
                trade_tasks=state.trade_tasks,
                completed=checkpoint_completed,
                key_maps=key_maps,
            )
            state.candle_tasks = pending_tasks.candle_tasks
            state.open_interest_tasks = pending_tasks.open_interest_tasks
            state.funding_tasks = pending_tasks.funding_tasks
            state.volatility_index_data_tasks = pending_tasks.volatility_index_data_tasks
            state.trade_tasks = pending_tasks.trade_tasks
            if has_checkpoint_state(checkpoint_completed):
                logger.info(
                    (
                        "Resuming from Bronze checkpoint '%s' pending_tasks "
                        "candle=%s open_interest=%s funding=%s volatility_index_data=%s trade=%s"
                    ),
                    checkpoint_path,
                    len(state.candle_tasks),
                    len(state.open_interest_tasks),
                    len(state.funding_tasks),
                    len(state.volatility_index_data_tasks),
                    len(state.trade_tasks),
                )

            policy = dependencies.build_bronze_execution_policy()
            candle_concurrency = policy.candle_concurrency
            open_interest_concurrency = policy.open_interest_concurrency
            funding_concurrency = policy.funding_concurrency
            volatility_concurrency = policy.funding_concurrency
            trade_concurrency = policy.trade_concurrency
            incremental_parquet_on_fetch = bool(args.save_parquet_lake)
            logger.info(
                (
                    "Fetch mode enabled for spot_ohlcv/perp, open_interest, funding, volatility_index_data, and trades "
                    "with concurrency=%s (configured=%s)"
                ),
                policy.effective_concurrency,
                policy.configured_concurrency,
            )
            if incremental_parquet_on_fetch:
                logger.info("Incremental parquet flush enabled during fetch execution")

            def _mark_checkpoint_complete(dataset: str, key: tuple[object, ...]) -> None:
                add_completed_checkpoint_key(
                    completed=checkpoint_completed,
                    dataset=cast(CheckpointDataset, dataset),
                    key=key,
                    key_maps=key_maps,
                )
                if checkpoint_enabled:
                    dependencies.write_bronze_checkpoint(
                        checkpoint_path,
                        fingerprint=checkpoint_fingerprint,
                        completed=checkpoint_completed,
                    )

            incremental_persistor = IncrementalPersistor(
                lake_root=cast(str, args.lake_root),
                mark_checkpoint_complete=_mark_checkpoint_complete,
                persist_fn=dependencies.persist_loader_outputs,
            )

            fetch_results = cast(
                Any,
                dependencies.fetch_all_task_groups(
                    candle_tasks=state.candle_tasks,
                    open_interest_tasks=state.open_interest_tasks,
                    funding_tasks=state.funding_tasks,
                    volatility_index_data_tasks=state.volatility_index_data_tasks,
                    trade_tasks=state.trade_tasks,
                    lake_root=cast(str, args.lake_root),
                    candle_concurrency=candle_concurrency,
                    open_interest_concurrency=open_interest_concurrency,
                    funding_concurrency=funding_concurrency,
                    volatility_concurrency=volatility_concurrency,
                    trade_concurrency=trade_concurrency,
                    logger=logger,
                    on_candle_task_complete=(
                        lambda task, rows: incremental_persistor.on_candle_task_complete(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_open_interest_task_complete=(
                        lambda task, rows: incremental_persistor.on_open_interest_task_complete(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_funding_task_complete=(
                        lambda task, rows: incremental_persistor.on_funding_task_complete(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_candle_task_chunk=(
                        lambda task, rows: incremental_persistor.on_candle_task_chunk(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_open_interest_task_chunk=(
                        lambda task, rows: incremental_persistor.on_open_interest_task_chunk(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_funding_task_chunk=(
                        lambda task, rows: incremental_persistor.on_funding_task_chunk(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_volatility_index_data_task_chunk=(
                        lambda task, rows: incremental_persistor.on_volatility_index_data_task_chunk(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_trade_task_complete=(
                        lambda task, rows: incremental_persistor.on_trade_task_complete(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_trade_task_chunk=(
                        lambda task, rows: incremental_persistor.on_trade_task_chunk(task, rows, logger)
                    )
                    if incremental_parquet_on_fetch
                    else None,
                ),
            )
            task_results = fetch_results.candle_results
            task_errors = fetch_results.candle_errors
            open_interest_results = fetch_results.open_interest_results
            open_interest_errors = fetch_results.open_interest_errors
            funding_results = fetch_results.funding_results
            funding_errors = fetch_results.funding_errors
            volatility_index_data_results = fetch_results.volatility_results
            volatility_index_data_errors = fetch_results.volatility_errors
            trade_results = fetch_results.trade_results
            trade_errors = fetch_results.trade_errors
            for key in task_results:
                _mark_checkpoint_complete("candle", key)
            for open_interest_key in open_interest_results:
                _mark_checkpoint_complete("open_interest", open_interest_key)
            for funding_key in funding_results:
                _mark_checkpoint_complete("funding", funding_key)
            for volatility_key in volatility_index_data_results:
                _mark_checkpoint_complete("volatility_index_data", volatility_key)
            for trade_key in trade_results:
                _mark_checkpoint_complete("trade", trade_key)
            pending_task_keys = checkpoint_task_keys(
                candle_tasks=state.candle_tasks,
                open_interest_tasks=state.open_interest_tasks,
                funding_tasks=state.funding_tasks,
                volatility_index_data_tasks=state.volatility_index_data_tasks,
                trade_tasks=state.trade_tasks,
                key_maps=key_maps,
            )
            success_task_keys = checkpoint_task_keys(
                candle_tasks=task_results,
                open_interest_tasks=open_interest_results,
                funding_tasks=funding_results,
                volatility_index_data_tasks=volatility_index_data_results,
                trade_tasks=trade_results,
                key_maps=key_maps,
            )
            fairness_rows = dependencies.symbol_progress_rows_from_dataset_tasks(
                dataset_tasks=[task for task in plan.dataset_tasks if task.checkpoint_key() in pending_task_keys],
                success_keys=success_task_keys,
            )
            finalize_bronze_output(
                logger=logger,
                output=state.output,
                tasks=state.candle_tasks,
                open_interest_tasks=state.open_interest_tasks,
                funding_tasks=state.funding_tasks,
                volatility_index_data_tasks=state.volatility_index_data_tasks,
                trade_tasks=state.trade_tasks,
                task_results=task_results,
                task_errors=task_errors,
                open_interest_results=open_interest_results,
                open_interest_errors=open_interest_errors,
                funding_results=funding_results,
                funding_errors=funding_errors,
                volatility_index_data_results=volatility_index_data_results,
                volatility_index_data_errors=volatility_index_data_errors,
                trade_results=trade_results,
                trade_errors=trade_errors,
                multi_market=multi_market,
                open_interest_requested=open_interest_requested,
                funding_requested=funding_requested,
                volatility_index_data_requested=volatility_index_data_requested,
                perps_trades_requested=perps_trades_requested,
                options_trades_requested=options_trades_requested,
                candles_for_storage=state.candles_for_storage,
                open_interest_for_storage=state.open_interest_for_storage,
                funding_for_storage=state.funding_for_storage,
                volatility_index_data_for_storage=state.volatility_index_data_for_storage,
                trades_for_storage=state.trades_for_storage,
                ohlcv_markets=ohlcv_markets,
                args=cast(Any, args),
                incremental_parquet_on_fetch=incremental_parquet_on_fetch,
                incremental_parquet_files=incremental_persistor.incremental_parquet_files,
                open_interest_dataset_type=dependencies.open_interest_dataset_type,
                sidecar_path_list_fn=dependencies.sidecar_path_list,
                ensure_bronze_sidecars_fn=dependencies.ensure_bronze_sidecars,
                populate_ohlcv_output_fn=dependencies.populate_ohlcv_output,
                populate_open_interest_output_fn=dependencies.populate_open_interest_output,
                populate_funding_output_fn=dependencies.populate_funding_output,
                populate_volatility_output_fn=dependencies.populate_volatility_output,
                populate_trades_output_fn=dependencies.populate_trades_output,
                symbol_progress_rows_fn=dependencies.symbol_progress_rows,
                fairness_rows=fairness_rows,
                trade_error_breakdown_fn=dependencies.trade_error_breakdown,
                candle_serializer=dependencies.candle_serializer,
                persist_fn=dependencies.persist_loader_outputs,
            )

            if not args.no_json_output:
                print(json.dumps(state.output, indent=2))
            if checkpoint_enabled and not (
                task_errors or open_interest_errors or funding_errors or volatility_index_data_errors or trade_errors
            ):
                checkpoint_path.unlink(missing_ok=True)
                logger.info("Cleared Bronze checkpoint '%s' after successful run", checkpoint_path)
            elif checkpoint_enabled:
                logger.warning(
                    "Retaining Bronze checkpoint '%s' for resume; failures remain",
                    checkpoint_path,
                )
            logger.info("Command complete: bronze-build")
    except dependencies.single_instance_error as exc:
        logger.warning("Single-instance lock active")
        raise SystemExit(str(exc)) from exc
