"""Tests for read-only Medallion benchmark telemetry."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from api.commands.benchmark import run_benchmark_build
from application.services import benchmark_service
from application.services.benchmark_service import BenchmarkTelemetryEvent, benchmark_stage, write_benchmark_report

pl = pytest.importorskip("polars")


def _sha256(path: Path) -> str:
    """Return an artifact hash used to verify read-only benchmark behavior."""

    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_benchmark_stage_reads_artifact_without_changing_it(tmp_path: Path) -> None:
    """A benchmark emits planned, built, and published events without mutating its input."""

    artifact = tmp_path / "spot_ohlcv" / "symbol=BTC" / "month=2026-01.parquet"
    artifact.parent.mkdir(parents=True)
    pl.DataFrame({"timestamp": [1, 2], "close": [10.0, 11.0]}).write_parquet(artifact)
    original_hash = _sha256(artifact)

    events = benchmark_stage(stage="bronze", root=tmp_path)

    assert [event.event_type for event in events] == ["planned", "built", "published"]
    assert {event.rows_in for event in events} == {2}
    assert {event.bytes_read for event in events} == {artifact.stat().st_size}
    assert {event.worker_count for event in events} == {1}
    assert {event.polars_thread_count for event in events} == {4}
    assert _sha256(artifact) == original_hash


def test_benchmark_stage_reports_skipped_and_failed_work(tmp_path: Path) -> None:
    """Empty and malformed inputs remain visible as skipped and failed telemetry."""

    assert [event.event_type for event in benchmark_stage(stage="silver", root=tmp_path / "missing")] == ["skipped"]

    invalid_artifact = tmp_path / "dataset" / "symbol=ETH" / "month=2026-02.parquet"
    invalid_artifact.parent.mkdir(parents=True)
    invalid_artifact.write_text("not parquet", encoding="utf-8")

    events = benchmark_stage(stage="gold", root=tmp_path)

    assert [event.event_type for event in events] == ["failed"]
    assert events[0].symbol == "ETH"


def test_write_benchmark_report_contains_all_measurement_fields(tmp_path: Path) -> None:
    """Reports retain every telemetry field needed for baseline comparisons."""

    source = tmp_path / "source" / "dataset" / "symbol=BTC" / "month=2026-01.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame({"value": [1]}).write_parquet(source)
    output = tmp_path / "reports" / "benchmark.json"

    write_benchmark_report(events=benchmark_stage(stage="bronze", root=tmp_path / "source"), output=output)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert set(payload["events"][0]) == {
        "event_type",
        "stage",
        "dataset",
        "symbol",
        "partition",
        "rows_in",
        "rows_out",
        "bytes_read",
        "bytes_written",
        "elapsed_seconds",
        "worker_count",
        "polars_thread_count",
        "timestamp",
    }


def test_fixture_benchmark_runs_twice_without_production_lake_paths(tmp_path: Path) -> None:
    """Fixture-only runs are repeatable and publish only the requested reports."""

    logger = logging.getLogger("benchmark-test")
    report_paths = [tmp_path / "first.json", tmp_path / "second.json"]
    for report_path in report_paths:
        run_benchmark_build(
            SimpleNamespace(
                maxprocesses=1,
                output_report=str(report_path),
                fixture_only=True,
                no_json_output=True,
                bronze_root=None,
                silver_root=None,
                gold_root=None,
            ),
            logger,
        )

    reports = [json.loads(report_path.read_text(encoding="utf-8")) for report_path in report_paths]
    assert [report["event_count"] for report in reports] == [9, 9]
    assert [[event["stage"] for event in report["events"]] for report in reports] == [
        ["bronze"] * 3 + ["silver"] * 3 + ["gold"] * 3,
        ["bronze"] * 3 + ["silver"] * 3 + ["gold"] * 3,
    ]


def test_benchmark_logs_every_required_work_event(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Structured logs expose planned, skipped, built, published, and failed work with context."""

    valid_root = tmp_path / "valid"
    artifact = valid_root / "dataset" / "symbol=BTC" / "month=2026-01.parquet"
    artifact.parent.mkdir(parents=True)
    pl.DataFrame({"value": [1]}).write_parquet(artifact)
    invalid_root = tmp_path / "invalid"
    invalid_artifact = invalid_root / "dataset" / "symbol=ETH" / "month=2026-01.parquet"
    invalid_artifact.parent.mkdir(parents=True)
    invalid_artifact.write_text("invalid", encoding="utf-8")
    logger = logging.getLogger("benchmark-log-test")

    with caplog.at_level(logging.INFO, logger=logger.name):
        run_benchmark_build(
            SimpleNamespace(
                maxprocesses=1,
                output_report=str(tmp_path / "report.json"),
                fixture_only=False,
                no_json_output=True,
                bronze_root=str(valid_root),
                silver_root=str(tmp_path / "missing"),
                gold_root=str(invalid_root),
            ),
            logger,
        )

    payloads = [
        json.loads(record.message.removeprefix("benchmark_telemetry="))
        for record in caplog.records
        if record.message.startswith("benchmark_telemetry=")
    ]
    assert {payload["event_type"] for payload in payloads} == {"planned", "skipped", "built", "published", "failed"}
    for payload in payloads:
        assert {"stage", "dataset", "rows_in", "rows_out", "worker_count", "polars_thread_count"} <= set(payload)


def test_benchmark_event_to_dict_is_json_compatible() -> None:
    event = BenchmarkTelemetryEvent(
        event_type="skipped",
        stage="gold",
        dataset="none",
        symbol=None,
        partition=None,
        rows_in=0,
        rows_out=0,
        bytes_read=0,
        bytes_written=0,
        elapsed_seconds=0.0,
        worker_count=1,
        polars_thread_count=4,
        timestamp="2026-01-01T00:00:00+00:00",
    )

    payload = event.to_dict()
    assert payload["event_type"] == "skipped"
    assert json.loads(json.dumps(payload)) == payload


@pytest.mark.parametrize("worker_count", [0, 5])
def test_benchmark_stage_rejects_invalid_worker_count(tmp_path: Path, worker_count: int) -> None:
    with pytest.raises(ValueError, match="worker_count"):
        benchmark_stage(stage="bronze", root=tmp_path, worker_count=worker_count)


@pytest.mark.parametrize("thread_count", [0, 5])
def test_benchmark_stage_rejects_invalid_polars_thread_count(tmp_path: Path, thread_count: int) -> None:
    with pytest.raises(ValueError, match="polars_thread_count"):
        benchmark_stage(stage="bronze", root=tmp_path, polars_thread_count=thread_count)


def test_benchmark_stage_uses_stem_when_path_has_no_partition_labels(tmp_path: Path) -> None:
    artifact = tmp_path / "dataset" / "artifact.parquet"
    artifact.parent.mkdir(parents=True)
    pl.DataFrame({"value": [1]}).write_parquet(artifact)

    events = benchmark_stage(stage="silver", root=tmp_path)

    assert events[0].dataset == "dataset"
    assert events[0].symbol is None
    assert events[0].partition == "artifact"


def test_benchmark_stage_marks_row_count_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    artifact = tmp_path / "dataset" / "artifact.parquet"
    artifact.parent.mkdir(parents=True)
    pl.DataFrame({"value": [1]}).write_parquet(artifact)
    monkeypatch.setattr(benchmark_service, "_parquet_rows", lambda _path: (_ for _ in ()).throw(OSError("broken")))

    events = benchmark_stage(stage="gold", root=tmp_path)

    assert events[0].event_type == "failed"
    assert events[0].dataset == "dataset"
