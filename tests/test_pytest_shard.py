"""Tests for deterministic GitHub Actions pytest sharding."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.pytest_shard import select_shard


def test_select_shard_partitions_files_without_overlap() -> None:
    """Every file should be assigned to exactly one deterministic shard."""

    files = [Path(f"tests/test_{index}.py") for index in range(10)]

    shards = [select_shard(files, shard_index=index, shard_count=4) for index in range(1, 5)]
    flattened = [path for shard in shards for path in shard]

    assert sorted(flattened) == files
    assert len(flattened) == len(set(flattened))
    assert shards[0] == [files[0], files[4], files[8]]
    assert shards[3] == [files[3], files[7]]


def test_select_shard_rejects_invalid_indexes() -> None:
    """Shard indexes are 1-based and bounded by the shard count."""

    files = [Path("tests/test_example.py")]

    with pytest.raises(ValueError, match="shard_index"):
        select_shard(files, shard_index=0, shard_count=2)
    with pytest.raises(ValueError, match="shard_index"):
        select_shard(files, shard_index=3, shard_count=2)
    with pytest.raises(ValueError, match="shard_count"):
        select_shard(files, shard_index=1, shard_count=0)
