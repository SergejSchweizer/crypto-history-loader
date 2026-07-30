"""Tests for repository-wide Python startup bootstrap."""

from __future__ import annotations

import importlib
import os
import sys

import pytest


def test_sitecustomize_caps_polars_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Importing ``sitecustomize`` should clamp Polars parallelism."""

    monkeypatch.setenv("POLARS_MAX_THREADS", "16")
    sys.modules.pop("sitecustomize", None)

    importlib.import_module("sitecustomize")

    assert os.getenv("POLARS_MAX_THREADS") == "4"
