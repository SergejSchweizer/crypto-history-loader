from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import cast

import polars as pl
import pytest

from application.postgres_sync.contracts import GoldSyncRepository
from application.postgres_sync.inventory import discover_current_gold_lineages, discover_declared_gold_lineages
from application.postgres_sync.service import synchronize_gold_root
from application.services.gold_publication import write_gold_publication_result

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
    schema = pl.read_parquet_schema(parquet)
    canonical_schema = json.dumps([(name, str(dtype)) for name, dtype in schema.items()], separators=(",", ":"))
    manifest["manifest_version"] = 2
    manifest["output_sha256"] = sha256(parquet.read_bytes()).hexdigest()
    manifest["schema_signature"] = sha256(canonical_schema.encode("utf-8")).hexdigest()
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
    assert snapshots[0].output_sha256 == sha256(new.read_bytes()).hexdigest()


def test_selects_newest_build_when_semver_is_unchanged(tmp_path: Path) -> None:
    old = _write_candidate(tmp_path, value=1.0, build_date_utc="2026-01-01T00:00:00Z")
    new = old.with_name("BTC_GOLD_newer.parquet")
    pl.read_parquet(old).with_columns(pl.lit(2.0).alias("value")).write_parquet(new)
    payload = json.loads(old.with_suffix(".json").read_text(encoding="utf-8"))
    payload["build_date_utc"] = "2026-01-02T00:00:00Z"
    payload["input_fingerprint"] = "fingerprint-newer"
    payload["build_id"] = "build-newer"
    payload["output_sha256"] = sha256(new.read_bytes()).hexdigest()
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


def test_invalid_candidate_manifest_fails(tmp_path: Path) -> None:
    parquet = _write_candidate(tmp_path)
    parquet.with_suffix(".json").write_text("not-json", encoding="utf-8")

    with pytest.raises(ValueError, match="invalid Gold manifest"):
        discover_current_gold_lineages(tmp_path)


def test_candidate_dataset_must_match_its_registered_path(tmp_path: Path) -> None:
    parquet = _write_candidate(tmp_path)
    manifest_path = parquet.with_suffix(".json")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["dataset_id"] = "gold.history.extended.m1"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="dataset/path mismatch"):
        discover_current_gold_lineages(tmp_path)


def test_current_inventory_handles_absent_root_and_manifest_edge_cases(tmp_path: Path) -> None:
    assert discover_current_gold_lineages(tmp_path / "missing") == ()

    parquet = _write_candidate(tmp_path)
    parquet.unlink()
    with pytest.raises(ValueError, match="no matching parquet"):
        discover_current_gold_lineages(tmp_path)


@pytest.mark.parametrize(
    ("field_name", "field_value", "message"),
    [
        ("build_date_utc", 1, "ISO UTC timestamp"),
        ("build_date_utc", "2026-01-01T01:00:00+01:00", "must use UTC"),
        ("schema_signature", None, "schema signature"),
        ("input_fingerprint", "", "source fingerprint"),
    ],
)
def test_current_inventory_rejects_invalid_manifest_fields(
    tmp_path: Path, field_name: str, field_value: object, message: str
) -> None:
    parquet = _write_candidate(tmp_path)
    manifest_path = parquet.with_suffix(".json")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload[field_name] = field_value
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        discover_current_gold_lineages(tmp_path)


def test_current_inventory_accepts_legacy_source_data_hash(tmp_path: Path) -> None:
    parquet = _write_candidate(tmp_path)
    manifest_path = parquet.with_suffix(".json")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload.pop("input_fingerprint")
    payload["source_data_hash"] = "legacy-fingerprint"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    snapshots = discover_current_gold_lineages(tmp_path)

    assert snapshots[0].source_fingerprint == "legacy-fingerprint"


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


def test_legacy_or_tampered_artifact_fails_before_snapshot(tmp_path: Path) -> None:
    parquet = _write_candidate(tmp_path)
    manifest_path = parquet.with_suffix(".json")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload.pop("manifest_version")
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="certified manifest version"):
        discover_current_gold_lineages(tmp_path)

    parquet = _write_candidate(tmp_path)
    parquet.write_bytes(parquet.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="SHA-256"):
        discover_current_gold_lineages(tmp_path)


@pytest.mark.parametrize(
    ("field_name", "field_value", "message"),
    [
        ("rows_out", 2, "row count"),
        ("schema_signature", "0" * 64, "schema signature"),
        ("symbol", "ETH", "lineage"),
        ("max_timestamp", "2026-01-01T00:01:00.123456Z", "timestamp bounds"),
    ],
)
def test_manifest_metadata_must_match_parquet(
    tmp_path: Path, field_name: str, field_value: object, message: str
) -> None:
    parquet = _write_candidate(tmp_path)
    manifest_path = parquet.with_suffix(".json")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload[field_name] = field_value
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        discover_current_gold_lineages(tmp_path)


def _write_publication_result(path: Path, artifacts: list[Path]) -> None:
    reports = []
    for artifact in artifacts:
        manifest = json.loads(artifact.with_suffix(".json").read_text(encoding="utf-8"))
        reports.append(
            {
                "dataset_id": manifest["dataset_id"],
                "exchange": manifest["exchange"],
                "symbol": manifest["symbol"],
                "parquet_path": str(artifact.resolve()),
                "manifest_path": str(artifact.with_suffix(".json").resolve()),
            }
        )
    write_gold_publication_result(path, reports)


def test_declared_inventory_excludes_retained_stale_extended_lineage(tmp_path: Path) -> None:
    current = _write_candidate(tmp_path, dataset_id="gold.history.full.m1")
    _write_candidate(tmp_path, dataset_id="gold.history.extended.m1")
    result_path = tmp_path / "publication-result.json"
    _write_publication_result(result_path, [current])

    snapshots = discover_declared_gold_lineages(tmp_path, result_path)

    assert [(item.lineage.dataset_id, item.lineage.exchange, item.lineage.symbol) for item in snapshots] == [
        ("gold.history.full.m1", "deribit", "BTC")
    ]


def test_declared_inventory_rejects_duplicate_lineages(tmp_path: Path) -> None:
    current = _write_candidate(tmp_path)
    result_path = tmp_path / "publication-result.json"
    _write_publication_result(result_path, [current])
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    payload["artifacts"].append(payload["artifacts"][0])
    result_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate lineages"):
        discover_declared_gold_lineages(tmp_path, result_path)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (None, "invalid Gold publication result"),
        ({}, "not a certified success"),
        ({"result_version": 1, "status": "success", "serving_deprecations": ["old"]}, "deprecation policy"),
        (
            {"result_version": 1, "status": "success", "serving_deprecations": [], "artifacts": {}},
            "artifacts must be a list",
        ),
        (
            {"result_version": 1, "status": "success", "serving_deprecations": [], "artifacts": ["bad"]},
            "artifact must contain an object",
        ),
    ],
)
def test_declared_inventory_rejects_invalid_publication_result(tmp_path: Path, payload: object, message: str) -> None:
    result_path = tmp_path / "publication-result.json"
    if payload is None:
        result_path.write_text("not-json", encoding="utf-8")
    else:
        result_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        discover_declared_gold_lineages(tmp_path, result_path)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (("dataset_id", "gold.unregistered.m1"), "unregistered dataset"),
        (("parquet_path", 1), "paths must be strings"),
        (("parquet_path", "/tmp/outside.parquet"), "outside the Gold root"),
        (("manifest_path", "other.json"), "outside the Gold root"),
    ],
)
def test_declared_inventory_rejects_invalid_artifact_locations(
    tmp_path: Path, change: tuple[str, object], message: str
) -> None:
    current = _write_candidate(tmp_path)
    result_path = tmp_path / "publication-result.json"
    _write_publication_result(result_path, [current])
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    payload["artifacts"][0][change[0]] = change[1]
    result_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        discover_declared_gold_lineages(tmp_path, result_path)


def test_declared_inventory_requires_matching_artifact_pair_and_manifest_lineage(tmp_path: Path) -> None:
    current = _write_candidate(tmp_path)
    result_path = tmp_path / "publication-result.json"
    _write_publication_result(result_path, [current])
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    payload["artifacts"][0]["manifest_path"] = str((tmp_path / "other.json").resolve())
    result_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="artifact pair does not match"):
        discover_declared_gold_lineages(tmp_path, result_path)

    _write_publication_result(result_path, [current])
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    payload["artifacts"][0]["symbol"] = "ETH"
    result_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="lineage does not match its manifest"):
        discover_declared_gold_lineages(tmp_path, result_path)


def test_all_declared_artifacts_are_certified_before_repository_mutation(tmp_path: Path) -> None:
    first = _write_candidate(tmp_path, dataset_id="gold.history.full.m1")
    tampered = _write_candidate(tmp_path, dataset_id="gold.history.extended.m1")
    result_path = tmp_path / "publication-result.json"
    _write_publication_result(result_path, [first, tampered])
    tampered.write_bytes(tampered.read_bytes() + b"tampered")
    repository_calls: list[str] = []

    class MutationSpy:
        def __getattr__(self, name: str) -> object:
            repository_calls.append(name)
            raise AssertionError(f"repository called before certification: {name}")

    with pytest.raises(ValueError, match="SHA-256"):
        synchronize_gold_root(tmp_path, cast(GoldSyncRepository, MutationSpy()), publication_result=result_path)

    assert repository_calls == []
