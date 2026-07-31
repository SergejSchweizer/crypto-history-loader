"""Tests for repository-wide Python startup bootstrap."""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest


def test_sitecustomize_caps_polars_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Importing ``sitecustomize`` should clamp Polars parallelism."""

    monkeypatch.setenv("POLARS_MAX_THREADS", "16")
    sys.modules.pop("sitecustomize", None)

    importlib.import_module("sitecustomize")

    assert os.getenv("POLARS_MAX_THREADS") == "4"


def test_cli_import_caps_effective_polars_thread_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI startup must cap the effective Polars pool before command imports use it."""

    environment = os.environ.copy()
    environment["POLARS_MAX_THREADS"] = "16"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import os; import api.cli; import polars as pl; "
                "print(os.getenv('POLARS_MAX_THREADS')); print(pl.thread_pool_size())"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
        cwd=Path(__file__).resolve().parents[1],
    )

    assert result.stdout.splitlines() == ["4", "4"]
