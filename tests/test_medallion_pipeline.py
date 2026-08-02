"""Tests for Medallion runner failure observability."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts import run_medallion_pipeline


def test_run_pipeline_logs_failed_step_and_signal(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A killed builder must leave an actionable step-level log marker."""

    def fail(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        raise subprocess.CalledProcessError(returncode=-9, cmd=["python", "main.py"])

    monkeypatch.setattr(run_medallion_pipeline.subprocess, "run", fail)

    with pytest.raises(subprocess.CalledProcessError):
        run_medallion_pipeline._run_pipeline(
            python_bin="python",
            steps=[run_medallion_pipeline.PipelineStep(name="gold", args=[str(Path("main.py"))])],
            repo_root=Path("."),
            env={},
        )

    assert "FAILED gold returncode=-9 signal=9" in capsys.readouterr().out


def test_run_pipeline_logs_successful_step_exit(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Successful builders should expose an explicit exit marker before DONE."""

    monkeypatch.setattr(
        run_medallion_pipeline.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args, returncode=0),
    )

    run_medallion_pipeline._run_pipeline(
        python_bin="python",
        steps=[run_medallion_pipeline.PipelineStep(name="gold", args=[str(Path("main.py"))])],
        repo_root=Path("."),
        env={},
    )

    output = capsys.readouterr().out
    assert "EXIT gold returncode=0" in output
    assert "DONE gold" in output
