"""Shared pytest fixtures for deterministic process-level test state."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def isolate_depth_environment() -> Iterator[None]:
    """Prevent repository runtime config loaded in one test from leaking into another."""

    original_depth_env = {key: value for key, value in os.environ.items() if key.startswith("DEPTH_")}
    for key in list(os.environ):
        if key.startswith("DEPTH_"):
            del os.environ[key]
    try:
        yield
    finally:
        for key in list(os.environ):
            if key.startswith("DEPTH_"):
                del os.environ[key]
        os.environ.update(original_depth_env)
