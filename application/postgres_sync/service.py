"""Application orchestration for deterministic Gold-to-PostgreSQL reconciliation."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import polars as pl

from application.postgres_sync.contracts import (
    GoldReconcileDecision,
    GoldReconcilePlanner,
    GoldRowDigest,
    GoldSourceSnapshot,
    GoldSyncLineageResult,
    GoldSyncRepository,
    GoldSyncResult,
    GoldSyncState,
    expected_target_summary,
    state_matches_snapshot,
)
from application.postgres_sync.delta import plan_gold_delta
from application.postgres_sync.inventory import discover_current_gold_lineages, discover_declared_gold_lineages
from application.postgres_sync.schema import PostgresTableSchema, build_postgres_table_schema

Clock = Callable[[], datetime]


class GoldSyncServiceError(RuntimeError):
    """Typed application failure identifying the first lineage that could not converge."""

    def __init__(self, snapshot: GoldSourceSnapshot, category: str, message: str) -> None:
        lineage = snapshot.lineage
        super().__init__(f"{category}: {lineage.dataset_id}/{lineage.exchange}/{lineage.symbol}: {message}")
        self.lineage = snapshot.lineage
        self.category = category


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _validate_schema_timestamp_contract(schema: dict[str, pl.DataType]) -> None:
    for column_name, dtype in schema.items():
        if str(dtype).startswith("Datetime") and dtype != pl.Datetime("us", "UTC"):
            raise TypeError(f"Gold datetime column {column_name!r} must be Datetime(us, UTC)")
    if schema.get("timestamp_m1") != pl.Datetime("us", "UTC"):
        raise TypeError("Gold timestamp_m1 must be Datetime(us, UTC)")


def _mapped_schema(snapshot: GoldSourceSnapshot) -> PostgresTableSchema:
    schema_obj = pl.read_parquet_schema(snapshot.artifact_path)
    schema = dict(schema_obj)
    _validate_schema_timestamp_contract(schema)
    return build_postgres_table_schema(snapshot.lineage.dataset_id, schema)


def _validated_source_rows(snapshot: GoldSourceSnapshot) -> list[dict[str, object]]:
    frame = pl.read_parquet(snapshot.artifact_path)
    schema = dict(frame.schema)
    _validate_schema_timestamp_contract(schema)
    if frame.height != snapshot.row_count:
        raise ValueError("Gold source row count differs from current manifest snapshot")
    if frame.height == 0:
        if snapshot.min_timestamp is not None or snapshot.max_timestamp is not None:
            raise ValueError("empty Gold source has non-empty timestamp bounds")
        return []

    timestamps = frame.get_column("timestamp_m1")
    if timestamps.null_count() != 0:
        raise ValueError("Gold timestamp_m1 cannot contain null values")
    if bool(timestamps.is_duplicated().any()):
        raise ValueError("Gold timestamp_m1 contains duplicate values within the lineage")
    min_timestamp = timestamps.min()
    max_timestamp = timestamps.max()
    if not isinstance(min_timestamp, datetime) or not isinstance(max_timestamp, datetime):
        raise TypeError("Gold timestamp bounds must be datetimes")
    if min_timestamp.tzinfo is None or max_timestamp.tzinfo is None:
        raise ValueError("Gold timestamp bounds must be timezone-aware")
    if min_timestamp.utcoffset() != timedelta(0) or max_timestamp.utcoffset() != timedelta(0):
        raise ValueError("Gold timestamp bounds must use UTC")
    normalized_min = min_timestamp.astimezone(UTC)
    normalized_max = max_timestamp.astimezone(UTC)
    if normalized_min != snapshot.min_timestamp or normalized_max != snapshot.max_timestamp:
        raise ValueError("Gold source timestamp bounds differ from current manifest snapshot")

    exchange_values = frame.get_column("exchange").unique().to_list()
    symbol_values = frame.get_column("symbol").unique().to_list()
    if exchange_values != [snapshot.lineage.exchange] or symbol_values != [snapshot.lineage.symbol]:
        raise ValueError("Gold source row identity differs from selected lineage")

    raw_rows: list[dict[str, Any]] = frame.to_dicts()
    return [cast(dict[str, object], row) for row in raw_rows]


def _snapshot_for_target(snapshot: GoldSourceSnapshot, table_schema: PostgresTableSchema) -> GoldSourceSnapshot:
    return replace(snapshot, schema_signature=table_schema.signature)


def _state_for_snapshot(snapshot: GoldSourceSnapshot, *, clock: Clock) -> GoldSyncState:
    synced_at = clock()
    if synced_at.tzinfo is None or synced_at.utcoffset() != timedelta(0):
        raise ValueError("Gold sync clock must return an aware UTC datetime")
    return GoldSyncState(
        lineage=snapshot.lineage,
        source_fingerprint=snapshot.source_fingerprint,
        schema_signature=snapshot.schema_signature,
        row_count=snapshot.row_count,
        min_timestamp=snapshot.min_timestamp,
        max_timestamp=snapshot.max_timestamp,
        synced_at_utc=synced_at.astimezone(UTC),
        source_version=snapshot.source_version,
        build_id=snapshot.build_id,
    )


def _reconcile_planner(snapshot: GoldSourceSnapshot, *, clock: Clock) -> GoldReconcilePlanner:
    def plan(state: GoldSyncState | None, target_digests: tuple[GoldRowDigest, ...]) -> GoldReconcileDecision:
        expected_summary = expected_target_summary(snapshot)
        if state is not None and state_matches_snapshot(state, snapshot):
            return GoldReconcileDecision(None, None, expected_summary)

        source_rows = _validated_source_rows(snapshot)
        delta = plan_gold_delta(source_rows, target_digests, state_exists=state is not None)
        return GoldReconcileDecision(delta, _state_for_snapshot(snapshot, clock=clock), expected_summary)

    return plan


def reconcile_snapshots(
    snapshots: Sequence[GoldSourceSnapshot],
    repository: GoldSyncRepository,
    *,
    clock: Clock = _utc_now,
) -> GoldSyncResult:
    """Reconcile selected current Gold snapshots sequentially in stable lineage order."""

    results: list[GoldSyncLineageResult] = []
    for raw_snapshot in sorted(snapshots, key=lambda item: item.lineage):
        try:
            table_schema = _mapped_schema(raw_snapshot)
            snapshot = _snapshot_for_target(raw_snapshot, table_schema)
            decision = repository.reconcile_lineage(
                snapshot,
                table_schema.ddl,
                table_schema.signature,
                _reconcile_planner(snapshot, clock=clock),
            )

            if decision.plan is None:
                results.append(
                    GoldSyncLineageResult(
                        lineage=snapshot.lineage,
                        source_fingerprint=snapshot.source_fingerprint,
                        inserted=0,
                        updated=0,
                        deleted=0,
                        unchanged=snapshot.row_count,
                        status="unchanged",
                    )
                )
                continue

            plan = decision.plan
            results.append(
                GoldSyncLineageResult(
                    lineage=snapshot.lineage,
                    source_fingerprint=snapshot.source_fingerprint,
                    inserted=plan.inserted_count,
                    updated=plan.updated_count,
                    deleted=plan.deleted_count,
                    unchanged=plan.unchanged_count,
                    status="synchronized",
                )
            )
        except GoldSyncServiceError:
            raise
        except (ValueError, TypeError) as exc:
            raise GoldSyncServiceError(raw_snapshot, "compatibility", str(exc)) from exc
        except Exception as exc:
            raise GoldSyncServiceError(raw_snapshot, "repository", type(exc).__name__) from exc
    return GoldSyncResult(tuple(results))


def synchronize_gold_root(
    gold_root: str | Path,
    repository: GoldSyncRepository,
    *,
    publication_result: str | Path | None = None,
    clock: Clock = _utc_now,
) -> GoldSyncResult:
    """Discover and reconcile either declared-run or backward-compatible current Gold."""

    snapshots = (
        discover_current_gold_lineages(gold_root)
        if publication_result is None
        else discover_declared_gold_lineages(gold_root, publication_result)
    )
    return reconcile_snapshots(snapshots, repository, clock=clock)
