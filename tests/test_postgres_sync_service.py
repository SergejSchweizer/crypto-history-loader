from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from application.postgres_sync import (
    GoldDeltaPlan,
    GoldLineage,
    GoldRowDigest,
    GoldRowKey,
    GoldSourceSnapshot,
    GoldSyncState,
    GoldTargetSummary,
)
from application.postgres_sync.delta import canonical_row_hash
from application.postgres_sync.service import GoldSyncServiceError, reconcile_snapshots


class FakeRepository:
    def __init__(self) -> None:
        self.states: dict[GoldLineage, GoldSyncState] = {}
        self.digests: dict[GoldLineage, tuple[GoldRowDigest, ...]] = {}
        self.summaries: dict[GoldLineage, GoldTargetSummary] = {}
        self.applied: list[tuple[GoldLineage, GoldDeltaPlan, GoldSyncState]] = []
        self.ensure_calls: list[GoldLineage] = []
        self.fail_lineage: GoldLineage | None = None

    def ensure_lineage(self, snapshot: GoldSourceSnapshot, ddl: str, schema_signature: str) -> None:
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert schema_signature == snapshot.schema_signature
        self.ensure_calls.append(snapshot.lineage)

    def read_state(self, lineage: GoldLineage) -> GoldSyncState | None:
        return self.states.get(lineage)

    def read_digests(self, lineage: GoldLineage) -> tuple[GoldRowDigest, ...]:
        return self.digests.get(lineage, ())

    def summary(self, lineage: GoldLineage) -> GoldTargetSummary:
        return self.summaries.get(lineage, GoldTargetSummary(0, None, None))

    def apply_delta(self, lineage: GoldLineage, plan: GoldDeltaPlan, state: GoldSyncState) -> None:
        if self.fail_lineage == lineage:
            raise RuntimeError("injected repository failure")
        self.applied.append((lineage, plan, state))
        self.states[lineage] = state
        self.digests[lineage] = plan.source_digests
        self.summaries[lineage] = GoldTargetSummary(state.row_count, state.min_timestamp, state.max_timestamp)


def _write_snapshot(
    root: Path,
    *,
    dataset_id: str = "gold.history.full.m1",
    symbol: str = "BTC",
    rows: tuple[tuple[datetime, float], ...],
    fingerprint: str = "fingerprint",
) -> GoldSourceSnapshot:
    lineage = GoldLineage(dataset_id, "deribit", symbol)
    path = root / f"{symbol}_{dataset_id.replace('.', '_')}.parquet"
    frame = pl.DataFrame(
        {
            "exchange": ["deribit" for _ in rows],
            "symbol": [symbol for _ in rows],
            "timestamp_m1": [timestamp for timestamp, _ in rows],
            "value": [value for _, value in rows],
        },
        schema_overrides={"timestamp_m1": pl.Datetime("us", "UTC")},
    )
    frame.write_parquet(path)
    minimum = min((timestamp for timestamp, _ in rows), default=None)
    maximum = max((timestamp for timestamp, _ in rows), default=None)
    return GoldSourceSnapshot(
        lineage=lineage,
        artifact_path=path,
        source_fingerprint=fingerprint,
        schema_signature="inventory-signature",
        row_count=len(rows),
        min_timestamp=minimum,
        max_timestamp=maximum,
        source_version="v1.0.0",
    )


def _fixed_clock() -> datetime:
    return datetime(2026, 8, 22, 18, 45, 0, 999999, tzinfo=UTC)


def test_bootstrap_inserts_complete_current_lineage(tmp_path: Path) -> None:
    timestamps = (
        datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC),
        datetime(2026, 1, 2, 0, 0, 0, 654321, tzinfo=UTC),
    )
    snapshot = _write_snapshot(tmp_path, rows=((timestamps[0], 1.0), (timestamps[1], 2.0)))
    repository = FakeRepository()

    result = reconcile_snapshots((snapshot,), repository, clock=_fixed_clock)

    assert result.inserted == 2
    assert result.updated == 0
    assert result.deleted == 0
    assert result.unchanged == 0
    assert len(repository.applied) == 1
    _, plan, state = repository.applied[0]
    assert plan.inserted_count == 2
    assert state.synced_at_utc.microsecond == 999999
    assert plan.inserts[0].key.timestamp_m1.microsecond == 123456


def test_unchanged_snapshot_performs_zero_mutation_but_verifies_summary(tmp_path: Path) -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    snapshot = _write_snapshot(tmp_path, rows=((timestamp, 1.0),))
    repository = FakeRepository()

    first = reconcile_snapshots((snapshot,), repository, clock=_fixed_clock)
    assert first.inserted == 1
    repository.applied.clear()

    second = reconcile_snapshots((snapshot,), repository, clock=_fixed_clock)
    assert second.inserted == second.updated == second.deleted == 0
    assert second.unchanged == 1
    assert repository.applied == []


def test_changed_source_writes_only_exact_delta(tmp_path: Path) -> None:
    t1 = datetime(2026, 1, 1, tzinfo=UTC)
    t2 = datetime(2026, 1, 2, tzinfo=UTC)
    t3 = datetime(2026, 1, 3, tzinfo=UTC)
    t4 = datetime(2026, 1, 4, tzinfo=UTC)
    original = _write_snapshot(tmp_path, rows=((t1, 1.0), (t2, 2.0), (t3, 3.0)), fingerprint="old")
    repository = FakeRepository()
    reconcile_snapshots((original,), repository, clock=_fixed_clock)
    repository.applied.clear()

    changed = _write_snapshot(tmp_path, rows=((t1, 1.0), (t2, 20.0), (t4, 4.0)), fingerprint="new")
    result = reconcile_snapshots((changed,), repository, clock=_fixed_clock)

    assert (result.inserted, result.updated, result.deleted, result.unchanged) == (1, 1, 1, 1)
    _, plan, _ = repository.applied[0]
    assert [item.key.timestamp_m1 for item in plan.inserts] == [t4]
    assert [item.key.timestamp_m1 for item in plan.updates] == [t2]
    assert list(plan.deletes) == [GoldRowKey("deribit", "BTC", t3)]


def test_historical_revision_after_missed_runs_is_detected(tmp_path: Path) -> None:
    old = datetime(2020, 1, 1, tzinfo=UTC)
    recent1 = datetime(2026, 7, 1, tzinfo=UTC)
    recent2 = datetime(2026, 7, 8, tzinfo=UTC)
    recent3 = datetime(2026, 7, 15, tzinfo=UTC)
    baseline = _write_snapshot(tmp_path, rows=((old, 1.0),), fingerprint="baseline")
    repository = FakeRepository()
    reconcile_snapshots((baseline,), repository, clock=_fixed_clock)
    repository.applied.clear()

    current = _write_snapshot(
        tmp_path,
        rows=((old, 9.0), (recent1, 2.0), (recent2, 3.0), (recent3, 4.0)),
        fingerprint="after-missed-runs",
    )
    result = reconcile_snapshots((current,), repository, clock=_fixed_clock)
    assert result.updated == 1
    assert result.inserted == 3


def test_summary_mismatch_fails_without_false_success(tmp_path: Path) -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    snapshot = _write_snapshot(tmp_path, rows=((timestamp, 1.0),))

    class BadSummaryRepository(FakeRepository):
        def summary(self, lineage: GoldLineage) -> GoldTargetSummary:
            return GoldTargetSummary(999, timestamp, timestamp)

    repository = BadSummaryRepository()
    with pytest.raises(GoldSyncServiceError, match="compatibility"):
        reconcile_snapshots((snapshot,), repository, clock=_fixed_clock)


def test_processing_is_stable_and_stops_on_first_failure(tmp_path: Path) -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    btc = _write_snapshot(tmp_path, symbol="BTC", rows=((timestamp, 1.0),), fingerprint="btc")
    eth = _write_snapshot(tmp_path, symbol="ETH", rows=((timestamp, 2.0),), fingerprint="eth")
    sol = _write_snapshot(tmp_path, symbol="SOL", rows=((timestamp, 3.0),), fingerprint="sol")
    repository = FakeRepository()
    repository.fail_lineage = eth.lineage

    with pytest.raises(GoldSyncServiceError) as exc_info:
        reconcile_snapshots((sol, eth, btc), repository, clock=_fixed_clock)

    assert exc_info.value.lineage == eth.lineage
    assert [lineage.symbol for lineage in repository.ensure_calls] == ["BTC", "ETH"]
    assert btc.lineage in repository.states
    assert sol.lineage not in repository.states


def test_preexisting_digests_without_state_fail_closed(tmp_path: Path) -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    snapshot = _write_snapshot(tmp_path, rows=((timestamp, 1.0),))
    repository = FakeRepository()
    row = {
        "exchange": "deribit",
        "symbol": "BTC",
        "timestamp_m1": timestamp,
        "value": 1.0,
    }
    repository.digests[snapshot.lineage] = (
        GoldRowDigest(GoldRowKey("deribit", "BTC", timestamp), canonical_row_hash(row)),
    )
    with pytest.raises(GoldSyncServiceError, match="compatibility"):
        reconcile_snapshots((snapshot,), repository, clock=_fixed_clock)
