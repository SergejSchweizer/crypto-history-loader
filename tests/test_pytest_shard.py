"""Tests for deterministic GitHub Actions pytest sharding."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.pytest_shard import classify_test_suite, filter_suite, select_shard


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


def test_filter_suite_splits_unit_and_integration_files() -> None:
    """Unit and integration suites should be deterministic and non-overlapping."""

    files = [
        Path("tests/test_feature_metadata.py"),
        Path("tests/test_gold_service.py"),
        Path("tests/test_cli_parser_args.py"),
        Path("tests/test_open_interest.py"),
    ]

    assert classify_test_suite(files[0]) == "unit"
    assert classify_test_suite(files[1]) == "integration"
    assert filter_suite(files, suite="unit") == [files[0], files[3]]
    assert filter_suite(files, suite="integration") == [files[1], files[2]]
    assert filter_suite(files, suite="all") == files


def test_real_postgres_integration_file_is_not_routed_to_unit_ci() -> None:
    """The CI database contract test requires the integration PostgreSQL service."""

    postgres_test = Path("tests/test_postgres_real_integration.py")

    assert classify_test_suite(postgres_test) == "integration"
    assert filter_suite([postgres_test], suite="unit") == []


def test_filter_suite_rejects_unknown_suite() -> None:
    """Suite names should stay explicit."""

    with pytest.raises(ValueError, match="suite"):
        filter_suite([Path("tests/test_example.py")], suite="unknown")
