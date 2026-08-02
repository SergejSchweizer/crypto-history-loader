"""Tests for bounded batch-local Silver partition cache behavior."""

from __future__ import annotations

import pytest

from application.services.silver_partition_cache import SilverPartitionCache


def test_cache_reuses_one_partition_load_and_evicts_at_capacity() -> None:
    """Avoid duplicate scans while retaining at most the configured partition count."""

    cache = SilverPartitionCache(max_entries=1)
    loads: list[str] = []

    assert cache.get_or_load("2026-01", lambda: loads.append("first") or {"month": "2026-01"}) == {"month": "2026-01"}
    assert cache.get_or_load("2026-01", lambda: loads.append("duplicate") or {"month": "2026-01"}) == {
        "month": "2026-01"
    }
    assert cache.get_or_load("2026-02", lambda: loads.append("second") or {"month": "2026-02"}) == {"month": "2026-02"}
    assert loads == ["first", "second"]


def test_cache_clear_and_limit_validation() -> None:
    """Make batch-boundary release and invalid capacity handling explicit."""

    with pytest.raises(ValueError, match="at least one"):
        SilverPartitionCache(max_entries=0)

    cache = SilverPartitionCache()
    cache.get_or_load("2026-01", lambda: "first")
    cache.clear()
    assert cache.get_or_load("2026-01", lambda: "reloaded") == "reloaded"
