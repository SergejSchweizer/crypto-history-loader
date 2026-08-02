"""Tests for the read-only benchmark CLI command."""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from api.commands import benchmark as benchmark_cmd

pl = pytest.importorskip("polars")


def _args(tmp_path: Path, **overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "maxprocesses": 1,
        "output_report": str(tmp_path / "report.json"),
        "fixture_only": False,
        "no_json_output": True,
        "bronze_root": None,
        "silver_root": None,
        "gold_root": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_write_fixture_creates_all_stages(tmp_path: Path) -> None:
    roots = benchmark_cmd._write_fixture(tmp_path)

    assert set(roots) == {"bronze", "silver", "gold"}
    for root in roots.values():
        assert list(root.rglob("*.parquet"))


def test_root_arguments_requires_at_least_one_root(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Provide one or more"):
        benchmark_cmd._root_arguments(_args(tmp_path))


def test_root_arguments_resolves_only_configured_roots(tmp_path: Path) -> None:
    args = _args(tmp_path, bronze_root=str(tmp_path / "bronze"), gold_root=str(tmp_path / "gold"))

    roots = benchmark_cmd._root_arguments(args)

    assert set(roots) == {"bronze", "gold"}
    assert roots["bronze"] == (tmp_path / "bronze").resolve()


def test_validate_output_path_rejects_report_inside_lake(tmp_path: Path) -> None:
    root = (tmp_path / "bronze").resolve()
    with pytest.raises(ValueError, match="outside every measured"):
        benchmark_cmd._validate_output_path(output=root / "report.json", roots={"bronze": root})


def test_validate_output_path_allows_report_outside_lake(tmp_path: Path) -> None:
    root = (tmp_path / "bronze").resolve()

    benchmark_cmd._validate_output_path(output=tmp_path / "report.json", roots={"bronze": root})


def test_benchmark_roots_preserves_stage_order(tmp_path: Path) -> None:
    calls: list[str] = []

    def _fake_benchmark_stage(*, stage: str, root: Path, worker_count: int) -> list[object]:
        calls.append(stage)
        return []

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(benchmark_cmd, "benchmark_stage", _fake_benchmark_stage)
    try:
        benchmark_cmd._benchmark_roots(roots={"gold": tmp_path / "gold", "bronze": tmp_path / "bronze"}, worker_count=1)
    finally:
        monkeypatch.undo()

    assert calls == ["bronze", "gold"]


@pytest.mark.parametrize("worker_count", [0, 5])
def test_run_benchmark_build_rejects_worker_bounds(tmp_path: Path, worker_count: int) -> None:
    with pytest.raises(ValueError, match="maxprocesses"):
        benchmark_cmd.run_benchmark_build(
            _args(tmp_path, maxprocesses=worker_count), logging.getLogger("benchmark-test")
        )


def test_run_benchmark_build_with_explicit_roots_writes_report(tmp_path: Path) -> None:
    root = tmp_path / "bronze"
    artifact = root / "dataset" / "month=2026-01.parquet"
    artifact.parent.mkdir(parents=True)
    pl.DataFrame({"value": [1]}).write_parquet(artifact)
    output = tmp_path / "report.json"

    benchmark_cmd.run_benchmark_build(
        _args(tmp_path, bronze_root=str(root), output_report=str(output)),
        logging.getLogger("benchmark-test"),
    )

    assert output.exists()


def test_run_benchmark_build_prints_json_when_enabled(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    benchmark_cmd.run_benchmark_build(
        _args(tmp_path, fixture_only=True, no_json_output=False), logging.getLogger("benchmark-test")
    )

    assert '"report"' in capsys.readouterr().out
