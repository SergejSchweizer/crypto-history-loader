from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from application.postgres_sync.inventory import discover_current_gold_lineages

UTC_MICROSECOND_TIMESTAMP = pl.Datetime("us", "UTC")


def _write_candidate(
    root: Path,
    *,
    dataset_id: str = "gold.history.full.m1",
    version: str = "v1.0.0",
    value: float = 1.0,
    timestamp_dtype: pl.DataType = UTC_MICROSECOND_TIMESTAMP,
    build_date_utc: str = "2026-01-01T00:00:00Z",
) -> Path:
    artifact_dir = (
        root
        / f"dataset_id={dataset_id}"
        / "dataset_type=gold_symbol_dataset"
        / f"feature_set_version={version}"
        / "exchange=deribit"
        / "symbol=BTC"
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "timestamp_m1": [datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)],
            "exchange": ["deribit"],
            "symbol": ["BTC"],
            "value": [value],
        },
        schema_overrides={"timestamp_m1": timestamp_dtype},
    )
    parquet = artifact_dir / "BTC_GOLD_fixture.parquet"
    frame.write_parquet(parquet)
    manifest = {
        "dataset_id": dataset_id,
        "exchange": "deribit",
        "symbol": "BTC",
        "dataset_version": version,
        "rows_out": 1,
        "min_timestamp": "2026-01-01T00:00:00.123456Z",
        "max_timestamp": "2026-01-01T00:00:00.123456Z",
        "input_fingerprint": f"fingerprint-{version}-{value}",
        "build_id": f"build-{version}",
        "build_date_utc": build_date_utc,
    }
    parquet.with_suffix(".json").write_text(json.dumps(manifest), encoding="utf-8")
    return parquet


def test_selects_highest_semver_without_using_mtime(tmp_path: Path) -> None:
    old = _write_candidate(tmp_path, version="v1.0.0", value=1.0)
    new = _write_candidate(tmp_path, version="v1.0.1", value=2.0)
    # Make the older semantic version newer by mtime; selection must not change.
    old.touch()
    snapshots = discover_current_gold_lineages(tmp_path)
    assert len(snapshots) == 1
    assert snapshots[0].source_version == "v1.0.1"
    assert snapshots[0].artifact_path == new.resolve()
    assert snapshots[0].min_timestamp is not None
    assert snapshots[0].min_timestamp.microsecond == 123456


def test_selects_newest_build_when_semver_is_unchanged(tmp_path: Path) -> None:
    old = _write_candidate(tmp_path, value=1.0, build_date_utc="2026-01-01T00:00:00Z")
    new = old.with_name("BTC_GOLD_newer.parquet")
    pl.read_parquet(old).with_columns(pl.lit(2.0).alias("value")).write_parquet(new)
    payload = json.loads(old.with_suffix(".json").read_text(encoding="utf-8"))
    payload["build_date_utc"] = "2026-01-02T00:00:00Z"
    payload["input_fingerprint"] = "fingerprint-newer"
    payload["build_id"] = "build-newer"
    new.with_suffix(".json").write_text(json.dumps(payload), encoding="utf-8")

    snapshots = discover_current_gold_lineages(tmp_path)

    assert len(snapshots) == 1
    assert snapshots[0].artifact_path == new.resolve()


def test_unregistered_gold_artifacts_are_not_publishable(tmp_path: Path) -> None:
    _write_candidate(tmp_path, dataset_id="gold.unregistered.m1")
    assert discover_current_gold_lineages(tmp_path) == ()


def test_duplicate_current_version_fails(tmp_path: Path) -> None:
    first = _write_candidate(tmp_path, version="v1.0.0")
    duplicate = first.with_name("BTC_GOLD_duplicate.parquet")
    pl.read_parquet(first).write_parquet(duplicate)
    duplicate.with_suffix(".json").write_text(first.with_suffix(".json").read_text(encoding="utf-8"), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate current"):
        discover_current_gold_lineages(tmp_path)


def test_wrong_timestamp_unit_or_timezone_fails(tmp_path: Path) -> None:
    _write_candidate(tmp_path, timestamp_dtype=pl.Datetime("ms", "UTC"))
    with pytest.raises(TypeError, match=r"Datetime\(us, UTC\)"):
        discover_current_gold_lineages(tmp_path)


def test_corrupt_or_incomplete_manifest_fails(tmp_path: Path) -> None:
    parquet = _write_candidate(tmp_path)
    manifest_path = parquet.with_suffix(".json")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload.pop("input_fingerprint")
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="fingerprint"):
        discover_current_gold_lineages(tmp_path)
