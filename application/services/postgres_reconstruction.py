"""Guarded orchestration for PostgreSQL serving-plane reconstruction."""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal, Protocol

ReconstructionMode = Literal["no-op-certification", "reconstruction"]
_OWNED_RECONSTRUCTION_CHECKS = frozenset({"owned-catalog"})


class ReconstructionError(RuntimeError):
    """Raised when production reconstruction cannot complete safely."""


class ReconstructionAdapter(Protocol):
    """Operator-provided destructive operations for the two owned schemas only."""

    def disable_scheduler(self) -> None: ...

    def restore_scheduler(self) -> None: ...

    def acquire_host_pipeline_lock(self) -> AbstractContextManager[None]: ...

    def acquire_postgres_reconstruction_lock(self) -> AbstractContextManager[None]: ...

    def validate_endpoint(self, *, host: str, port: int, database: str) -> None: ...

    def create_validated_backup(self) -> None: ...

    def reconstruct_owned_schemas(self) -> None: ...

    def validate_runtime_dml_role(self) -> None: ...

    def bootstrap_certified_gold(self) -> None: ...

    def zero_mutation_replay(self) -> None: ...


@dataclass(frozen=True, slots=True)
class SanitizedCheck:
    """Secret-free subset of one PR-101 conformance check."""

    name: str
    category: str
    passed: bool


@dataclass(frozen=True, slots=True)
class SanitizedConformanceReport:
    """Validated secret-free data consumed from a PR-101 report."""

    status: str
    host: str
    port: int
    database: str
    checks: tuple[SanitizedCheck, ...]


Verifier = Callable[[], Mapping[str, object]]
AdapterFactory = Callable[[], ReconstructionAdapter]


def parse_sanitized_conformance_report(payload: Mapping[str, object]) -> SanitizedConformanceReport:
    """Validate and sanitize the only PR-101 fields that control reconstruction."""

    status = payload.get("status")
    endpoint = payload.get("endpoint")
    checks = payload.get("checks")
    if (
        not isinstance(status, str)
        or status not in {"PASS", "FAIL"}
        or not isinstance(endpoint, Mapping)
        or not isinstance(checks, list)
    ):
        raise ReconstructionError("invalid PR-101 conformance report")
    host = endpoint.get("host")
    port = endpoint.get("port")
    database = endpoint.get("database")
    if (
        not isinstance(host, str)
        or not isinstance(port, int)
        or isinstance(port, bool)
        or not isinstance(database, str)
    ):
        raise ReconstructionError("invalid PR-101 conformance endpoint")
    parsed_checks: list[SanitizedCheck] = []
    for check in checks:
        if not isinstance(check, Mapping):
            raise ReconstructionError("invalid PR-101 conformance check")
        name, category, passed = check.get("name"), check.get("category"), check.get("passed")
        if not isinstance(name, str) or not isinstance(category, str) or not isinstance(passed, bool):
            raise ReconstructionError("invalid PR-101 conformance check")
        parsed_checks.append(SanitizedCheck(name=name, category=category, passed=passed))
    return SanitizedConformanceReport(status, host, port, database, tuple(parsed_checks))


def _select_mode(report: SanitizedConformanceReport) -> ReconstructionMode:
    if report.status == "PASS":
        if not all(check.passed for check in report.checks):
            raise ReconstructionError("inconsistent PR-101 PASS report")
        return "no-op-certification"
    failed_checks = tuple(check for check in report.checks if not check.passed)
    if not failed_checks or any(not _is_owned_reconstruction_drift(check) for check in failed_checks):
        raise ReconstructionError("PR-101 failures are not reconstruction-correctable")
    return "reconstruction"


def _is_owned_reconstruction_drift(check: SanitizedCheck) -> bool:
    return (check.category == "catalog" and check.name in _OWNED_RECONSTRUCTION_CHECKS) or (
        check.category == "data" and check.name.startswith("lineage:")
    )


def _write_evidence_atomically(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temp_path = Path(handle.name)
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    try:
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)


def run_postgres_reconstruction(
    *,
    current_report: Mapping[str, object],
    evidence_path: Path,
    verify_independently: Verifier,
    adapter_factory: AdapterFactory | None = None,
) -> ReconstructionMode:
    """Certify a passing target or reconstruct only explicitly owned, correctable drift."""

    mode: ReconstructionMode | None = None
    post_report: SanitizedConformanceReport | None = None
    try:
        report = parse_sanitized_conformance_report(current_report)
        mode = _select_mode(report)
        if mode == "no-op-certification":
            post_report = parse_sanitized_conformance_report(verify_independently())
            if post_report.status != "PASS" or not all(check.passed for check in post_report.checks):
                raise ReconstructionError("independent PR-101 verification did not pass")
        else:
            if adapter_factory is None:
                raise ReconstructionError("reconstruction requires an operator adapter factory")
            adapter = adapter_factory()
            adapter.disable_scheduler()
            with adapter.acquire_host_pipeline_lock(), adapter.acquire_postgres_reconstruction_lock():
                adapter.validate_endpoint(host=report.host, port=report.port, database=report.database)
                adapter.create_validated_backup()
                adapter.reconstruct_owned_schemas()
                adapter.validate_runtime_dml_role()
                adapter.bootstrap_certified_gold()
                post_report = parse_sanitized_conformance_report(verify_independently())
                if post_report.status != "PASS" or not all(check.passed for check in post_report.checks):
                    raise ReconstructionError("independent PR-101 verification did not pass")
                adapter.zero_mutation_replay()
            adapter.restore_scheduler()
        _write_evidence_atomically(
            evidence_path,
            {
                "contract": "pg-temporal-v1",
                "mode": mode,
                "status": "PASS",
                "generated_at_utc": datetime.now(UTC).isoformat(),
                "initial_checks": [asdict(check) for check in report.checks],
                "post_checks": [asdict(check) for check in post_report.checks] if post_report else [],
            },
        )
        return mode
    except Exception as exc:
        _write_evidence_atomically(
            evidence_path,
            {
                "contract": "pg-temporal-v1",
                "mode": mode or "hard-stop",
                "status": "FAIL",
                "generated_at_utc": datetime.now(UTC).isoformat(),
            },
        )
        if isinstance(exc, ReconstructionError):
            raise
        raise ReconstructionError("PostgreSQL reconstruction did not complete") from exc
