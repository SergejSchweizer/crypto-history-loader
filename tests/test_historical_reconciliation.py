"""Focused operator tests for guarded PR-100 historical reconciliation."""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from api import cli
from api.commands import historical_reconciliation as reconciliation_command
from application.data_quality_report import (
    CompletenessInterval,
    HistoricalBronzeLineage,
    HistoricalCompletenessReport,
)
from application.services.historical_reconciliation import (
    GoldCertification,
    JsonEvidenceSink,
    ReconciliationState,
    RecoveryReport,
    SilverRebuild,
    load_historical_report,
    reconcile_historical_intervals,
)


def _interval(*, status: str, minute: int) -> CompletenessInterval:
    return CompletenessInterval(
        lineage=HistoricalBronzeLineage("perps_trades", "deribit", "perp", "BTC-PERPETUAL", "tick"),
        start=f"2026-01-01T00:{minute:02d}:00Z",
        end=f"2026-01-01T00:{minute:02d}:00Z",
        category="observed_rows" if status == "PASS" else "unverified_acquisition",
        evidence="bronze_data:open_time" if status == "PASS" else "missing:acquisition_evidence",
        status=status,  # type: ignore[arg-type]  # Test helper deliberately varies the report status fixture.
    )


@dataclass
class RecordingSink:
    states: list[ReconciliationState] = field(default_factory=list)
    reports: list[RecoveryReport] = field(default_factory=list)

    def write_state(self, state: ReconciliationState) -> None:
        self.states.append(state)

    def write_report(self, report: RecoveryReport) -> None:
        self.reports.append(report)


@dataclass
class RecordingAdapter:
    events: list[str] = field(default_factory=list)
    gold: tuple[GoldCertification, ...] = (GoldCertification("gold.history.full.m1|deribit|BTC", "current", True),)
    final_audit_passes: bool = True
    inventory_passes: bool = True

    def snapshot_unaffected_bronze(self, intervals: tuple[CompletenessInterval, ...]) -> object:
        self.events.append(f"snapshot:{len(intervals)}")
        return "opaque-byte-snapshot"

    def backup_affected_bronze(self, intervals: tuple[CompletenessInterval, ...]) -> None:
        self.events.append(f"backup:{len(intervals)}")

    def reload_bronze_interval(self, interval: CompletenessInterval) -> None:
        self.events.append(f"reload:{interval.start}")

    def validate_reloaded_bronze(self, interval: CompletenessInterval) -> None:
        self.events.append(f"validate:{interval.start}")

    def verify_unaffected_bronze(self, snapshot: object, intervals: tuple[CompletenessInterval, ...]) -> None:
        assert snapshot == "opaque-byte-snapshot"
        self.events.append(f"unchanged:{len(intervals)}")

    def rebuild_silver(self, intervals: tuple[CompletenessInterval, ...]) -> SilverRebuild:
        self.events.append(f"silver:{len(intervals)}")
        return SilverRebuild(("silver.features|deribit|BTC|2026-01-01",), True)

    def certify_serving_gold(
        self, intervals: tuple[CompletenessInterval, ...], *, source_changed: bool
    ) -> tuple[GoldCertification, ...]:
        self.events.append(f"gold:{len(intervals)}:{source_changed}")
        return self.gold

    def rerun_historical_audit(self) -> HistoricalCompletenessReport:
        self.events.append("audit")
        return HistoricalCompletenessReport(
            intervals=(_interval(status="PASS" if self.final_audit_passes else "RECONCILE_REQUIRED", minute=1),)
        )

    def verify_gold_freshness(self) -> bool:
        self.events.append("freshness")
        return True

    def verify_gold_inventory(self) -> bool:
        self.events.append("inventory")
        return self.inventory_passes


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"schema_version": "unknown", "intervals": [], "status": "PASS"}, "unsupported"),
        ({"schema_version": "historical-lake-completeness-v1", "intervals": {}, "status": "PASS"}, "list"),
        (
            {
                "schema_version": "historical-lake-completeness-v1",
                "intervals": [
                    {
                        "lineage": "perps_trades|deribit|perp|BTC-PERPETUAL",
                        "start": "2026-01-01T00:00:00Z",
                        "end": "2026-01-01T00:00:00Z",
                        "gap_category": "observed_rows",
                        "evidence_source": "bronze_data:open_time",
                        "status": "PASS",
                    }
                ],
                "status": "PASS",
            },
            "five non-empty fields",
        ),
        (
            {
                "schema_version": "historical-lake-completeness-v1",
                "intervals": [
                    {
                        "lineage": "perps_trades|deribit|perp|BTC-PERPETUAL|tick",
                        "start": "2026-01-01T00:00:00Z",
                        "end": "2026-01-01T00:00:00Z",
                        "gap_category": "observed_rows",
                        "evidence_source": "bronze_data:open_time",
                        "status": "INVALID",
                    }
                ],
                "status": "PASS",
            },
            "unsupported PR-99 interval status",
        ),
    ],
)
def test_load_historical_report_rejects_invalid_schema_and_malformed_intervals(
    tmp_path: Path, payload: dict[str, object], message: str
) -> None:
    report_path = tmp_path / "pr99.json"
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_historical_report(report_path)


def test_load_historical_report_rejects_aggregate_status_mismatch(tmp_path: Path) -> None:
    report_path = tmp_path / "pr99.json"
    payload = json.loads(HistoricalCompletenessReport((_interval(status="RECONCILE_REQUIRED", minute=1),)).to_json())
    payload["status"] = "PASS"
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="aggregate status"):
        load_historical_report(report_path)


def test_reloads_exact_non_pass_intervals_after_backup_and_preserves_other_bytes() -> None:
    passed = _interval(status="PASS", minute=0)
    failed_one = _interval(status="RECONCILE_REQUIRED", minute=1)
    failed_two = _interval(status="RECONCILE_REQUIRED", minute=2)
    adapter = RecordingAdapter()
    sink = RecordingSink()

    report = reconcile_historical_intervals(
        input_report=HistoricalCompletenessReport((failed_two, passed, failed_one)),
        adapter=adapter,
        sink=sink,
    )

    assert report.status == "PASS"
    assert report.target_intervals == (failed_one, failed_two)
    assert report.source_mutated is True
    assert adapter.events == [
        "snapshot:2",
        "backup:2",
        f"reload:{failed_one.start}",
        f"validate:{failed_one.start}",
        f"reload:{failed_two.start}",
        f"validate:{failed_two.start}",
        "unchanged:2",
        "silver:2",
        "gold:2:True",
        "audit",
        "freshness",
        "inventory",
    ]
    assert sink.states[-1].status == "PASS"


def test_pass_input_never_mutates_source_but_certification_only_gold_is_allowed() -> None:
    adapter = RecordingAdapter(
        gold=(GoldCertification("gold.history.full.m1|deribit|BTC", "certification_only", True),)
    )
    sink = RecordingSink()

    report = reconcile_historical_intervals(
        input_report=HistoricalCompletenessReport((_interval(status="PASS", minute=0),)),
        adapter=adapter,
        sink=sink,
    )

    assert report.mode == "no_op_certification"
    assert report.source_mutated is False
    assert report.target_intervals == ()
    assert adapter.events == ["gold:0:False", "audit", "freshness", "inventory"]
    assert report.gold[0].publication_mode == "certification_only"


def test_unresolved_audit_inventory_or_gold_certification_blocks_downstream() -> None:
    adapter = RecordingAdapter(
        gold=(GoldCertification("gold.history.full.m1|deribit|BTC", "rebuilt", False),),
        final_audit_passes=False,
        inventory_passes=False,
    )
    sink = RecordingSink()

    report = reconcile_historical_intervals(
        input_report=HistoricalCompletenessReport((_interval(status="PASS", minute=0),)),
        adapter=adapter,
        sink=sink,
    )

    assert report.status == "FAIL"
    assert report.blockers == (
        "uncertified_serving_gold",
        "historical_completeness",
        "gold_inventory",
    )
    assert json.loads(report.to_json())["downstream_blocked"] is True
    assert sink.states[-1].phase == "blocked"


def test_unverified_silver_lookback_blocks_otherwise_passing_reconciliation() -> None:
    class MissingLookbackAdapter(RecordingAdapter):
        def rebuild_silver(self, intervals: tuple[CompletenessInterval, ...]) -> SilverRebuild:
            self.events.append(f"silver:{len(intervals)}")
            return SilverRebuild(("silver.features|deribit|BTC|2026-01-01",), False)

    adapter = MissingLookbackAdapter()
    sink = RecordingSink()

    report = reconcile_historical_intervals(
        input_report=HistoricalCompletenessReport((_interval(status="RECONCILE_REQUIRED", minute=0),)),
        adapter=adapter,
        sink=sink,
    )

    assert report.status == "FAIL"
    assert report.blockers == ("silver_lookback_unverified",)
    assert adapter.events == [
        "snapshot:1",
        "backup:1",
        "reload:2026-01-01T00:00:00Z",
        "validate:2026-01-01T00:00:00Z",
        "unchanged:1",
        "silver:1",
        "gold:1:True",
        "audit",
        "freshness",
        "inventory",
    ]
    assert sink.states[-1].phase == "blocked"


def test_uncertified_gold_blocks_otherwise_passing_reconciliation() -> None:
    adapter = RecordingAdapter(gold=(GoldCertification("gold.history.full.m1|deribit|BTC", "current", False),))
    sink = RecordingSink()

    report = reconcile_historical_intervals(
        input_report=HistoricalCompletenessReport((_interval(status="PASS", minute=0),)),
        adapter=adapter,
        sink=sink,
    )

    assert report.status == "FAIL"
    assert report.blockers == ("uncertified_serving_gold",)
    assert adapter.events == ["gold:0:False", "audit", "freshness", "inventory"]
    assert sink.states[-1].phase == "blocked"


def test_adapter_exception_during_silver_rebuild_stops_downstream_and_fails_closed() -> None:
    class FailingSilverAdapter(RecordingAdapter):
        def rebuild_silver(self, intervals: tuple[CompletenessInterval, ...]) -> SilverRebuild:
            self.events.append("silver-failed")
            raise RuntimeError("secret=/private/silver-input")

    adapter = FailingSilverAdapter()
    sink = RecordingSink()

    report = reconcile_historical_intervals(
        input_report=HistoricalCompletenessReport((_interval(status="RECONCILE_REQUIRED", minute=0),)),
        adapter=adapter,
        sink=sink,
    )

    assert report.status == "FAIL"
    assert report.blockers == ("reconciliation_operation_failed",)
    assert adapter.events == [
        "snapshot:1",
        "backup:1",
        "reload:2026-01-01T00:00:00Z",
        "validate:2026-01-01T00:00:00Z",
        "unchanged:1",
        "silver-failed",
    ]
    assert "secret" not in report.to_json()
    assert sink.states[-1].phase == "blocked"


def test_verified_backup_failure_prevents_reload_and_emits_sanitized_fail() -> None:
    class FailingBackupAdapter(RecordingAdapter):
        def backup_affected_bronze(self, intervals: tuple[CompletenessInterval, ...]) -> None:
            self.events.append("backup-failed")
            raise RuntimeError("secret=/private/provider-payload")

    adapter = FailingBackupAdapter()
    sink = RecordingSink()

    report = reconcile_historical_intervals(
        input_report=HistoricalCompletenessReport((_interval(status="RECONCILE_REQUIRED", minute=1),)),
        adapter=adapter,
        sink=sink,
    )

    assert adapter.events == ["snapshot:1", "backup-failed"]
    assert report.status == "FAIL"
    assert report.source_mutated is False
    assert "secret" not in report.to_json()
    assert sink.reports == [report]


def test_pr99_report_roundtrip_and_atomic_evidence_files(tmp_path: Path) -> None:
    source = tmp_path / "pr99.json"
    original = HistoricalCompletenessReport((_interval(status="RECONCILE_REQUIRED", minute=1),))
    source.write_text(original.to_json(), encoding="utf-8")
    loaded = load_historical_report(source)
    state_path = tmp_path / "evidence" / "state.json"
    report_path = tmp_path / "evidence" / "report.json"
    sink = JsonEvidenceSink(state_path=state_path, report_path=report_path)

    report = reconcile_historical_intervals(input_report=loaded, adapter=RecordingAdapter(), sink=sink)

    assert report.status == "PASS"
    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == "PASS"
    assert json.loads(report_path.read_text(encoding="utf-8"))["status"] == "PASS"


def test_cli_parser_exposes_guard_paths_adapter_and_global_debug() -> None:
    args = cli.build_parser().parse_args(
        [
            "--debug",
            "historical-reconcile",
            "--pr99-report",
            "pr99.json",
            "--state-file",
            "state.json",
            "--report-file",
            "report.json",
            "--adapter-factory",
            "operator_adapter:build",
        ]
    )

    assert args.command == "historical-reconcile"
    assert args.debug is True
    assert args.adapter_factory == "operator_adapter:build"


def test_cli_dispatch_uses_repository_debug_logging_and_blocks_on_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """env: {}
export-descriptive-stats:
    lake_root: lake/bronze
    output_csv: out.csv
    start_time: '2026-01-01T00:00:00Z'
    end_time: '2026-01-01T00:00:00Z'
    exchanges: [deribit]
    symbols: [BTC]
    timeframes: [1m]
    instrument_types: [perp]
""",
        encoding="utf-8",
    )
    config_path.chmod(0o644)
    captured: dict[str, object] = {}
    failed_report = RecoveryReport(
        mode="no_op_certification",
        input_audit_status="PASS",
        target_intervals=(),
        source_mutated=False,
        silver=None,
        gold=(),
        checks=(("historical_completeness", "NOT_RUN"),),
        blockers=("reconciliation_operation_failed",),
    )

    def _configure_logging(*, module_name: str, debug: bool) -> logging.Logger:
        captured["logging"] = (module_name, debug)
        return logging.getLogger("historical-reconciliation-cli-test")

    monkeypatch.setattr(cli, "configure_logging", _configure_logging)
    monkeypatch.setattr(
        reconciliation_command,
        "run_historical_reconciliation",
        lambda **_kwargs: failed_report,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "--config",
            str(config_path),
            "--debug",
            "historical-reconcile",
            "--pr99-report",
            "pr99.json",
            "--state-file",
            "state.json",
            "--report-file",
            "report.json",
            "--adapter-factory",
            "operator_adapter:build",
        ],
    )

    with pytest.raises(SystemExit, match="1"):
        cli.main()

    assert captured["logging"] == ("historical-reconcile", True)
