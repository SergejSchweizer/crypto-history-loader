"""Tests for Gold dataset versioning helpers."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from application.services.gold_versioning import (
    bump_semver,
    contract_bump_level,
    latest_manifest_for_dataset,
    parse_semver,
    prune_gold_artifacts,
)


def test_parse_and_bump_semver() -> None:
    assert parse_semver("v1.2.3") == (1, 2, 3)
    assert bump_semver("v1.2.3", "major") == "v2.0.0"
    assert bump_semver("v1.2.3", "minor") == "v1.3.0"
    assert bump_semver("v1.2.3", "patch") == "v1.2.4"
    assert bump_semver("v1.2.3", "none") == "v1.2.3"
    with pytest.raises(ValueError, match="Invalid semantic version"):
        parse_semver("1.2.3")
    with pytest.raises(ValueError, match="Unsupported semver bump level"):
        bump_semver("v1.2.3", "unsupported")


def test_contract_bump_level_falls_back_to_legacy_manifest_shape() -> None:
    current = {
        "columns": ["a", "b"],
        "join_policy": "full_outer_coalesce",
        "source_dataset_keys": ["spot_1m"],
    }
    previous = {
        "columns": ["a", "b"],
        "source_silver_datasets": {"spot_1m": {"rows": 2}},
    }

    assert contract_bump_level(
        previous,
        current,
        previous_source_data_hash="old",
        current_source_data_hash="new",
    ) == ("patch", "source_data_changed")


def test_latest_manifest_for_dataset_uses_newest_matching_manifest(tmp_path: Path) -> None:
    root = tmp_path / "gold"
    old_manifest = (
        root
        / "dataset_id=gold.market.core.m1"
        / "exchange=deribit"
        / "symbol=BTC"
        / "dataset_type=gold_symbol_dataset"
        / "feature_set_version=v1.0.0"
        / "exchange=deribit"
        / "symbol=BTC"
        / "old.json"
    )
    new_manifest = old_manifest.with_name("new.json")
    other_manifest = old_manifest.with_name("other.json")
    old_manifest.parent.mkdir(parents=True)
    old_manifest.write_text(json.dumps({"dataset_id": "gold.market.core.m1", "dataset_version": "v1.0.0"}))
    new_manifest.write_text(json.dumps({"dataset_id": "gold.market.core.m1", "dataset_version": "v1.0.1"}))
    other_manifest.write_text(json.dumps({"dataset_id": "gold.market.full.m1", "dataset_version": "v9.0.0"}))
    os.utime(old_manifest, (1, 1))
    os.utime(new_manifest, (2, 2))
    os.utime(other_manifest, (3, 3))

    payload = latest_manifest_for_dataset(root, "deribit", "BTC", "gold.market.core.m1")

    assert payload is not None
    assert payload["dataset_version"] == "v1.0.1"


def test_prune_gold_artifacts_keeps_latest_stem_groups(tmp_path: Path) -> None:
    artifact_dir = (
        tmp_path
        / "dataset_id=gold.market.core.m1"
        / "dataset_type=gold_symbol_dataset"
        / "feature_set_version=v1.0.0"
        / "exchange=deribit"
        / "symbol=BTC"
    )
    artifact_dir.mkdir(parents=True)
    for index in range(4):
        stem = artifact_dir / f"BTC_GOLD_{index}"
        for suffix in (".parquet", ".json", ".png"):
            path = stem.with_suffix(suffix)
            path.write_text("x", encoding="utf-8")

    prune_gold_artifacts(
        gold_root=tmp_path,
        dataset_id="gold.market.core.m1",
        exchange="deribit",
        symbol="BTC",
        keep_last_versions=2,
    )

    assert len(list(artifact_dir.glob("*.parquet"))) == 2
    assert len(list(artifact_dir.glob("*.json"))) == 2
    assert len(list(artifact_dir.glob("*.png"))) == 2
