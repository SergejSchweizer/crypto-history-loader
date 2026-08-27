"""Guarded orchestration and sanitized evidence for historical reconciliation."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, Protocol, cast

from application.data_quality_report import (
    AuditStatus,
    CompletenessInterval,
    EvidenceSource,
    GapCategory,
    HistoricalBronzeLineage,
    HistoricalCompletenessReport,
)

RecoveryStatus = Literal["PASS", "FAIL"]
RecoveryMode = Literal["targeted_reload", "no_op_certification"]
CheckStatus = Literal["PASS", "FAIL", "NOT_RUN"]
GoldPublicationMode = Literal["rebuilt", "certification_only", "current"]


@dataclass(frozen=True, order=True)
class GoldCertification:
    """Describe sanitized certification state for one serving Gold lineage.

    Args:
        lineage: Stable serving lineage identifier.
        publication_mode: Whether data was rebuilt, republished only for attestation, or already current.
        certified: Whether PR-89 output attestation and PR-90 inventory validation are available.
    """

    lineage: str
    publication_mode: GoldPublicationMode
    certified: bool


@dataclass(frozen=True)
class SilverRebuild:
    """Describe the dependency-scoped Silver rebuild result.

    Args:
        partitions: Sanitized identifiers of changed dependency-reachable partitions.
        lookback_applied: Whether every required feature lookback was propagated.
    """

    partitions: tuple[str, ...]
    lookback_applied: bool


@dataclass(frozen=True)
class ReconciliationState:
    """Persist the current fail-closed orchestration phase.

    Args:
        phase: Stable phase name, excluding local paths and provider payloads.
        mode: Source reconciliation mode selected solely from the PR-99 report.
        target_intervals: Exact PR-99 non-PASS intervals.
        status: RUNNING until a terminal PASS or FAIL is persisted.
    """

    phase: str
    mode: RecoveryMode
    target_intervals: tuple[CompletenessInterval, ...]
    status: Literal["RUNNING", "PASS", "FAIL"] = "RUNNING"
    schema_version: str = "historical-reconciliation-state-v1"

    def to_json(self) -> str:
        """Render deterministic sanitized state JSON.

        Returns:
            Newline-terminated state without filesystem paths or provider payloads.
        """

        payload = {
            "mode": self.mode,
            "phase": self.phase,
            "schema_version": self.schema_version,
            "status": self.status,
            "target_intervals": [interval.to_dict() for interval in self.target_intervals],
        }
        return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n"


@dataclass(frozen=True)
class RecoveryReport:
    """Contain sanitized terminal PR-100 recovery evidence.

    Args:
        mode: Selected source reconciliation mode.
        input_audit_status: Aggregate status from the consumed PR-99 report.
        target_intervals: Exact PR-99 non-PASS intervals.
        source_mutated: Whether provider reload was performed.
        silver: Dependency-scoped Silver result, when reached.
        gold: Serving-eligible Gold certification results.
        checks: Final PR-99, freshness, and inventory statuses.
        blockers: Stable unresolved reason codes. Any blocker forces FAIL.
    """

    mode: RecoveryMode
    input_audit_status: str
    target_intervals: tuple[CompletenessInterval, ...]
    source_mutated: bool
    silver: SilverRebuild | None
    gold: tuple[GoldCertification, ...]
    checks: tuple[tuple[str, CheckStatus], ...]
    blockers: tuple[str, ...]
    schema_version: str = "historical-reconciliation-report-v1"

    @property
    def status(self) -> RecoveryStatus:
        """Return PASS only when no unresolved blocker remains.

        Returns:
            PASS for certified terminal state; otherwise FAIL.
        """

        return "FAIL" if self.blockers else "PASS"

    def to_json(self) -> str:
        """Render deterministic sanitized recovery JSON.

        Returns:
            Newline-terminated report without paths, credentials, or market payloads.
        """

        payload = {
            "blockers": list(self.blockers),
            "checks": {name: status for name, status in self.checks},
            "downstream_blocked": self.status != "PASS",
            "gold_certifications": [
                {
                    "certified": item.certified,
                    "lineage": item.lineage,
                    "publication_mode": item.publication_mode,
                }
                for item in sorted(self.gold)
            ],
            "input_audit_status": self.input_audit_status,
            "mode": self.mode,
            "schema_version": self.schema_version,
            "silver_partitions": [] if self.silver is None else sorted(self.silver.partitions),
            "source_mutated": self.source_mutated,
            "status": self.status,
            "target_intervals": [interval.to_dict() for interval in self.target_intervals],
        }
        return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n"


class ReconciliationAdapter(Protocol):
    """Define all externally owned reconciliation side effects."""

    def snapshot_unaffected_bronze(self, intervals: tuple[CompletenessInterval, ...]) -> object:
        """Capture byte evidence for Bronze content outside target intervals."""

        ...

    def backup_affected_bronze(self, intervals: tuple[CompletenessInterval, ...]) -> None:
        """Create and verify a backup of every affected Bronze partition and manifest."""

        ...

    def reload_bronze_interval(self, interval: CompletenessInterval) -> None:
        """Reload exactly one PR-99 non-PASS interval through strict source validators."""

        ...

    def validate_reloaded_bronze(self, interval: CompletenessInterval) -> None:
        """Validate one recovered interval against PR-93 through PR-98 contracts."""

        ...

    def verify_unaffected_bronze(self, snapshot: object, intervals: tuple[CompletenessInterval, ...]) -> None:
        """Prove Bronze bytes outside target intervals are unchanged."""

        ...

    def rebuild_silver(self, intervals: tuple[CompletenessInterval, ...]) -> SilverRebuild:
        """Rebuild only dependency-reachable Silver partitions with required lookback."""

        ...

    def certify_serving_gold(
        self, intervals: tuple[CompletenessInterval, ...], *, source_changed: bool
    ) -> tuple[GoldCertification, ...]:
        """Rebuild affected Gold and certification-only republish current legacy Gold."""

        ...

    def rerun_historical_audit(self) -> HistoricalCompletenessReport:
        """Rerun the PR-99 historical completeness audit."""

        ...

    def verify_gold_freshness(self) -> bool:
        """Verify current Gold input fingerprints and freshness."""

        ...

    def verify_gold_inventory(self) -> bool:
        """Verify PR-90 certified-artifact inventory for all serving Gold."""

        ...


class EvidenceSink(Protocol):
    """Persist sanitized state and terminal report evidence."""

    def write_state(self, state: ReconciliationState) -> None:
        """Persist one state transition."""

        ...

    def write_report(self, report: RecoveryReport) -> None:
        """Persist one terminal report."""

        ...


class JsonEvidenceSink:
    """Atomically persist reconciliation state and report JSON files."""

    def __init__(self, *, state_path: Path, report_path: Path) -> None:
        self._state_path = state_path
        self._report_path = report_path

    def write_state(self, state: ReconciliationState) -> None:
        """Atomically persist state.

        Args:
            state: Sanitized state transition.

        Side Effects:
            Creates the state parent directory and replaces the state file.
        """

        _atomic_write(self._state_path, state.to_json())

    def write_report(self, report: RecoveryReport) -> None:
        """Atomically persist the terminal report.

        Args:
            report: Sanitized terminal recovery evidence.

        Side Effects:
            Creates the report parent directory and replaces the report file.
        """

        _atomic_write(self._report_path, report.to_json())


def load_historical_report(path: Path) -> HistoricalCompletenessReport:
    """Load and strictly validate a PR-99 report.

    Args:
        path: Existing PR-99 JSON report.

    Returns:
        Typed report whose aggregate status matches its intervals.

    Raises:
        ValueError: If schema, fields, interval status, or aggregate status is invalid.

    Side Effects:
        Reads one local JSON file without mutation.
    """

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("invalid PR-99 historical completeness report") from exc
    if not isinstance(raw, dict) or raw.get("schema_version") != "historical-lake-completeness-v1":
        raise ValueError("unsupported PR-99 historical completeness report schema")
    raw_intervals = raw.get("intervals")
    if not isinstance(raw_intervals, list):
        raise ValueError("PR-99 report intervals must be a list")
    intervals = tuple(_parse_interval(item) for item in raw_intervals)
    report = HistoricalCompletenessReport(intervals=intervals)
    if raw.get("status") != report.status:
        raise ValueError("PR-99 aggregate status does not match interval evidence")
    return report


def reconcile_historical_intervals(
    *,
    input_report: HistoricalCompletenessReport,
    adapter: ReconciliationAdapter,
    sink: EvidenceSink,
) -> RecoveryReport:
    """Execute fail-closed PR-100 orchestration from exact PR-99 evidence.

    Args:
        input_report: Validated PR-99 report controlling source mutation.
        adapter: Explicit implementation of all backup, mutation, rebuild, and audit side effects.
        sink: State and report persistence boundary.

    Returns:
        Sanitized PASS or FAIL report. Adapter failures become blocking FAIL evidence.

    Side Effects:
        Delegates guarded lake operations and atomically persists state/report through injected interfaces.
    """

    targets = tuple(sorted(interval for interval in input_report.intervals if interval.status != "PASS"))
    mode: RecoveryMode = "targeted_reload" if targets else "no_op_certification"
    state = ReconciliationState(phase="planned", mode=mode, target_intervals=targets)
    sink.write_state(state)
    source_mutated = False
    silver: SilverRebuild | None = None
    gold: tuple[GoldCertification, ...] = ()
    checks: dict[str, CheckStatus] = {
        "historical_completeness": "NOT_RUN",
        "gold_freshness": "NOT_RUN",
        "gold_inventory": "NOT_RUN",
    }
    blockers: list[str] = []

    try:
        if targets:
            unaffected_snapshot = adapter.snapshot_unaffected_bronze(targets)
            adapter.backup_affected_bronze(targets)
            state = replace(state, phase="bronze_backed_up")
            sink.write_state(state)
            for interval in targets:
                source_mutated = True
                adapter.reload_bronze_interval(interval)
                adapter.validate_reloaded_bronze(interval)
            adapter.verify_unaffected_bronze(unaffected_snapshot, targets)
            state = replace(state, phase="bronze_reconciled")
            sink.write_state(state)

        silver = adapter.rebuild_silver(targets) if targets else SilverRebuild((), True)
        if not silver.lookback_applied:
            blockers.append("silver_lookback_unverified")
        state = replace(state, phase="silver_rebuilt")
        sink.write_state(state)

        gold = adapter.certify_serving_gold(targets, source_changed=source_mutated)
        if any(not item.certified for item in gold):
            blockers.append("uncertified_serving_gold")
        state = replace(state, phase="gold_certified")
        sink.write_state(state)

        checks["historical_completeness"] = "PASS" if adapter.rerun_historical_audit().status == "PASS" else "FAIL"
        checks["gold_freshness"] = "PASS" if adapter.verify_gold_freshness() else "FAIL"
        checks["gold_inventory"] = "PASS" if adapter.verify_gold_inventory() else "FAIL"
        blockers.extend(name for name, status in checks.items() if status != "PASS")
    except Exception:
        blockers.append("reconciliation_operation_failed")

    report = RecoveryReport(
        mode=mode,
        input_audit_status=input_report.status,
        target_intervals=targets,
        source_mutated=source_mutated,
        silver=silver,
        gold=gold,
        checks=tuple(checks.items()),
        blockers=tuple(dict.fromkeys(blockers)),
    )
    sink.write_state(replace(state, phase="complete" if report.status == "PASS" else "blocked", status=report.status))
    sink.write_report(report)
    return report


def _parse_interval(raw: object) -> CompletenessInterval:
    if not isinstance(raw, dict):
        raise ValueError("PR-99 interval must be an object")
    parsed = cast(dict[str, object], raw)
    required = ("lineage", "start", "end", "gap_category", "evidence_source", "status")
    if any(not isinstance(parsed.get(field), str) for field in required):
        raise ValueError("PR-99 interval fields must be strings")
    lineage_parts = cast(str, parsed["lineage"]).split("|")
    if len(lineage_parts) != 5 or any(not part for part in lineage_parts):
        raise ValueError("PR-99 interval lineage must contain five non-empty fields")
    category = parsed["gap_category"]
    evidence = parsed["evidence_source"]
    status = parsed["status"]
    if category not in {"observed_rows", "confirmed_empty", "expected_provider_gap", "unverified_acquisition"}:
        raise ValueError("unsupported PR-99 gap category")
    if evidence not in {
        "bronze_data:open_time",
        "bronze_sidecar:confirmed_empty",
        "config:expected_provider_gap",
        "missing:acquisition_evidence",
    }:
        raise ValueError("unsupported PR-99 evidence source")
    if status not in {"PASS", "RECONCILE_REQUIRED"}:
        raise ValueError("unsupported PR-99 interval status")
    return CompletenessInterval(
        lineage=HistoricalBronzeLineage(*lineage_parts),
        start=cast(str, parsed["start"]),
        end=cast(str, parsed["end"]),
        category=cast(GapCategory, category),
        evidence=cast(EvidenceSource, evidence),
        status=cast(AuditStatus, status),
    )


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)
