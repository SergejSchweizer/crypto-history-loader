"""Focused operator-safety tests for guarded PostgreSQL reconstruction."""

from __future__ import annotations

import json
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import pytest

from application.services.postgres_reconstruction import ReconstructionError, run_postgres_reconstruction


def _report(status: str, checks: list[dict[str, object]]) -> dict[str, object]:
    return {
        "status": status,
        "endpoint": {"host": "10.10.1.3", "port": 54321, "database": "market_data", "password": "secret"},
        "checks": checks,
    }


def _passing_report() -> dict[str, object]:
    return _report("PASS", [{"name": "owned-catalog", "category": "catalog", "passed": True}])


class _Adapter:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def disable_scheduler(self) -> None:
        self.calls.append("disable")

    def restore_scheduler(self) -> None:
        self.calls.append("restore")

    def acquire_host_pipeline_lock(self) -> Any:
        self.calls.append("host-lock")
        return nullcontext()

    def acquire_postgres_reconstruction_lock(self) -> Any:
        self.calls.append("postgres-lock")
        return nullcontext()

    def validate_endpoint(self, *, host: str, port: int, database: str) -> None:
        self.calls.append(f"endpoint:{host}:{port}:{database}")

    def create_validated_backup(self) -> None:
        self.calls.append("backup")

    def reconstruct_owned_schemas(self) -> None:
        self.calls.append("schemas")

    def validate_runtime_dml_role(self) -> None:
        self.calls.append("dml-role")

    def bootstrap_certified_gold(self) -> None:
        self.calls.append("bootstrap")

    def zero_mutation_replay(self) -> None:
        self.calls.append("replay")


def test_passing_report_selects_no_op_without_creating_adapter(tmp_path: Path) -> None:
    """A passing PR-101 report certifies without destructive adapter calls."""

    def forbidden_factory() -> _Adapter:
        raise AssertionError("no-op must not instantiate adapter")

    evidence_path = tmp_path / "evidence.json"
    mode = run_postgres_reconstruction(
        current_report=_passing_report(),
        evidence_path=evidence_path,
        verify_independently=_passing_report,
        adapter_factory=forbidden_factory,
    )

    payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert mode == "no-op-certification"
    assert payload["status"] == "PASS"
    assert "secret" not in json.dumps(payload)
    assert "detail" not in json.dumps(payload)


def test_correctable_drift_reconstructs_in_required_order(tmp_path: Path) -> None:
    """Owned catalog/data drift runs the guarded reconstruction sequence then restores scheduling."""

    calls: list[str] = []
    adapter = _Adapter(calls)
    mode = run_postgres_reconstruction(
        current_report=_report(
            "FAIL",
            [
                {"name": "owned-catalog", "category": "catalog", "passed": False},
                {"name": "lineage:gold.x:binance:BTC", "category": "data", "passed": False},
            ],
        ),
        evidence_path=tmp_path / "evidence.json",
        verify_independently=_passing_report,
        adapter_factory=lambda: adapter,
    )

    assert mode == "reconstruction"
    assert calls == [
        "disable",
        "host-lock",
        "postgres-lock",
        "endpoint:10.10.1.3:54321:market_data",
        "backup",
        "schemas",
        "dml-role",
        "bootstrap",
        "replay",
        "restore",
    ]


def test_non_owned_failure_hard_stops_without_adapter(tmp_path: Path) -> None:
    """Permission and other non-owned failures cannot authorize destructive recovery."""

    with pytest.raises(ReconstructionError, match="not reconstruction-correctable"):
        run_postgres_reconstruction(
            current_report=_report("FAIL", [{"name": "runtime-contract", "category": "role", "passed": False}]),
            evidence_path=tmp_path / "evidence.json",
            verify_independently=_passing_report,
            adapter_factory=lambda: (_ for _ in ()).throw(AssertionError("must not create adapter")),
        )

    assert json.loads((tmp_path / "evidence.json").read_text(encoding="utf-8"))["mode"] == "hard-stop"


def test_failed_independent_verification_keeps_scheduler_disabled(tmp_path: Path) -> None:
    """A failed reconstruction verification blocks replay and scheduler restoration."""

    calls: list[str] = []
    with pytest.raises(ReconstructionError, match="did not pass"):
        run_postgres_reconstruction(
            current_report=_report("FAIL", [{"name": "owned-catalog", "category": "catalog", "passed": False}]),
            evidence_path=tmp_path / "evidence.json",
            verify_independently=lambda: _report(
                "FAIL", [{"name": "owned-catalog", "category": "catalog", "passed": False}]
            ),
            adapter_factory=lambda: _Adapter(calls),
        )

    assert calls == [
        "disable",
        "host-lock",
        "postgres-lock",
        "endpoint:10.10.1.3:54321:market_data",
        "backup",
        "schemas",
        "dml-role",
        "bootstrap",
    ]
