"""Tests for Gold dataset versioning helpers."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from application.services.gold_versioning import (
    bump_semver,
    contract_bump_level,
    extract_feature_set_version,
    format_semver,
    latest_manifest_for_dataset,
    parse_semver,
    prune_gold_artifacts,
    prune_gold_versions,
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
        "source_dataset_keys": ["spot_ohlcv_1m"],
    }
    previous = {
        "columns": ["a", "b"],
        "source_silver_datasets": {"spot_ohlcv_1m": {"rows": 2}},
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


def test_prune_gold_versions_enforces_dataset_wide_latest_three(tmp_path: Path) -> None:
    dataset_base = tmp_path / "dataset_id=gold.market.core.m1" / "dataset_type=gold_symbol_dataset"
    for version, symbol in (
        ("v1.0.0", "ETH"),
        ("v1.0.1", "BTC"),
        ("v1.0.2", "BTC"),
        ("v1.0.3", "BTC"),
    ):
        symbol_dir = dataset_base / f"feature_set_version={version}" / "exchange=deribit" / f"symbol={symbol}"
        symbol_dir.mkdir(parents=True)
        (symbol_dir / f"{symbol}_GOLD.parquet").write_text("x", encoding="utf-8")

    prune_gold_versions(
        gold_root=tmp_path,
        dataset_id="gold.market.core.m1",
        exchange="deribit",
        symbol="BTC",
        keep_last_versions=3,
    )

    kept_versions = sorted(path.name.split("=", 1)[1] for path in dataset_base.glob("feature_set_version=*"))
    assert kept_versions == ["v1.0.1", "v1.0.2", "v1.0.3"]


def test_versioning_handles_invalid_and_missing_artifacts(tmp_path: Path) -> None:
    """Versioning helpers should reject invalid retention and ignore malformed manifests."""

    assert format_semver(1, 2, 3) == "v1.2.3"
    assert extract_feature_set_version(Path("feature_set_version=v2.0.0")) == "v2.0.0"
    assert extract_feature_set_version(Path("version=v2.0.0")) is None
    assert extract_feature_set_version(Path("feature_set_version=")) is None
    assert latest_manifest_for_dataset(tmp_path, "deribit", "BTC", "gold.missing") is None
    with pytest.raises(ValueError, match="keep_last_versions"):
        prune_gold_versions(
            gold_root=tmp_path,
            dataset_id="gold.market.core.m1",
            exchange="deribit",
            symbol="BTC",
            keep_last_versions=0,
        )
    with pytest.raises(ValueError, match="keep_last_versions"):
        prune_gold_artifacts(
            gold_root=tmp_path,
            dataset_id="gold.market.core.m1",
            exchange="deribit",
            symbol="BTC",
            keep_last_versions=0,
        )


def test_contract_bump_level_detects_added_and_invalid_source_keys() -> None:
    """Source additions are minor while malformed source-key contracts are major."""

    base = {"columns": ["a"], "join_policy": "join", "source_dataset_keys": ["spot"]}
    assert contract_bump_level(
        {"contract_signature": base},
        {**base, "source_dataset_keys": ["spot", "funding"]},
        previous_source_data_hash="x",
        current_source_data_hash="x",
    ) == ("minor", "source_dataset_added")
    assert contract_bump_level(
        {"contract_signature": {**base, "source_dataset_keys": "bad"}},
        base,
        previous_source_data_hash="x",
        current_source_data_hash="x",
    ) == ("major", "invalid_source_dataset_keys")
