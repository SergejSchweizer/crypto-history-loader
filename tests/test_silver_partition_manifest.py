"""Tests for deterministic Silver partition manifests and publication."""

from __future__ import annotations

from pathlib import Path

import pytest

from application.services import silver_partition_manifest as manifests

pl = pytest.importorskip("polars")


def _fingerprint(source_root: Path, source_file: Path) -> str:
    return manifests.source_fingerprint(
        bronze_root=source_root,
        source_files=[str(source_file)],
        source_schema={"open_time": "Datetime", "close_price": "Float64"},
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        builder_contract_version="test/v1",
    )


def test_source_fingerprint_is_repeatable_and_tracks_file_content(tmp_path: Path) -> None:
    """Use content identity, not process-local state, for unchanged detection."""

    source_root = tmp_path / "bronze"
    source_file = source_root / "input.parquet"
    source_file.parent.mkdir(parents=True)
    pl.DataFrame({"open_time": [1], "close_price": [100.0]}).write_parquet(source_file)

    first = _fingerprint(source_root, source_file)
    assert _fingerprint(source_root, source_file) == first

    pl.DataFrame({"open_time": [1], "close_price": [101.0]}).write_parquet(source_file)
    assert _fingerprint(source_root, source_file) != first


def test_publish_partition_atomically_supports_no_op_manifest_lookup(tmp_path: Path) -> None:
    """Publish a complete manifest and accept it only while artifact bytes match."""

    parquet_path = tmp_path / "silver" / "BTC-2026-05.parquet"
    source_schema = {"open_time": "Int64", "close_price": "Float64"}
    fingerprint = "input-fingerprint"
    frame = pl.DataFrame({"open_time": [1, 2], "close_price": [100.0, 101.0]})

    published = manifests.publish_partition_atomically(
        frame=frame,
        parquet_path=parquet_path,
        input_fingerprint=fingerprint,
        source_schema=source_schema,
        sort_keys=("open_time",),
        deduplication_keys=("open_time",),
        builder_contract_version="test/v1",
    )

    assert published.row_count == 2
    assert (
        manifests.load_current_manifest(
            parquet_path=parquet_path,
            expected_input_fingerprint=fingerprint,
            expected_builder_contract_version="test/v1",
        )
        == published
    )

    parquet_path.write_bytes(b"not a parquet file")
    assert (
        manifests.load_current_manifest(
            parquet_path=parquet_path,
            expected_input_fingerprint=fingerprint,
            expected_builder_contract_version="test/v1",
        )
        is None
    )


def test_unknown_manifest_status_is_a_cache_miss(tmp_path: Path) -> None:
    """Reject status values outside the versioned publication contract."""

    parquet_path = tmp_path / "silver" / "BTC-2026-05.parquet"
    manifests.publish_partition_atomically(
        frame=pl.DataFrame({"open_time": [1], "close_price": [100.0]}),
        parquet_path=parquet_path,
        input_fingerprint="input-fingerprint",
        source_schema={"open_time": "Int64", "close_price": "Float64"},
        sort_keys=("open_time",),
        deduplication_keys=("open_time",),
        builder_contract_version="test/v1",
    )
    manifest_path = manifests.performance_manifest_path(parquet_path)
    manifest_path.write_text(
        manifest_path.read_text(encoding="utf-8").replace('"published"', '"unknown"'), encoding="utf-8"
    )

    assert (
        manifests.load_current_manifest(
            parquet_path=parquet_path,
            expected_input_fingerprint="input-fingerprint",
            expected_builder_contract_version="test/v1",
        )
        is None
    )


def test_manifest_publication_failure_restores_previous_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the last valid parquet and manifest readable when publication fails."""

    parquet_path = tmp_path / "silver" / "BTC-2026-05.parquet"
    kwargs = {
        "parquet_path": parquet_path,
        "input_fingerprint": "input-fingerprint",
        "source_schema": {"open_time": "Int64", "close_price": "Float64"},
        "sort_keys": ("open_time",),
        "deduplication_keys": ("open_time",),
        "builder_contract_version": "test/v1",
    }
    manifests.publish_partition_atomically(frame=pl.DataFrame({"open_time": [1], "close_price": [100.0]}), **kwargs)
    previous_parquet = parquet_path.read_bytes()
    previous_manifest = manifests.performance_manifest_path(parquet_path).read_bytes()

    real_replace = manifests.os.replace

    def fail_manifest_publish(source: Path | str, destination: Path | str) -> None:
        if Path(destination) == manifests.performance_manifest_path(parquet_path):
            raise OSError("injected manifest publication failure")
        real_replace(source, destination)

    monkeypatch.setattr(manifests.os, "replace", fail_manifest_publish)
    with pytest.raises(OSError, match="injected"):
        manifests.publish_partition_atomically(frame=pl.DataFrame({"open_time": [1], "close_price": [101.0]}), **kwargs)

    assert parquet_path.read_bytes() == previous_parquet
    assert manifests.performance_manifest_path(parquet_path).read_bytes() == previous_manifest


def test_parquet_staging_failure_preserves_previous_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave the prior readable artifact intact when staging cannot be validated."""

    parquet_path = tmp_path / "silver" / "BTC-2026-05.parquet"
    kwargs = {
        "parquet_path": parquet_path,
        "input_fingerprint": "input-fingerprint",
        "source_schema": {"open_time": "Int64", "close_price": "Float64"},
        "sort_keys": ("open_time",),
        "deduplication_keys": ("open_time",),
        "builder_contract_version": "test/v1",
    }
    manifests.publish_partition_atomically(frame=pl.DataFrame({"open_time": [1], "close_price": [100.0]}), **kwargs)
    previous_parquet = parquet_path.read_bytes()
    previous_manifest = manifests.performance_manifest_path(parquet_path).read_bytes()

    monkeypatch.setattr(
        manifests, "_validate_parquet", lambda _path: (_ for _ in ()).throw(OSError("injected parquet failure"))
    )
    with pytest.raises(OSError, match="injected parquet failure"):
        manifests.publish_partition_atomically(frame=pl.DataFrame({"open_time": [1], "close_price": [101.0]}), **kwargs)

    assert parquet_path.read_bytes() == previous_parquet
    assert manifests.performance_manifest_path(parquet_path).read_bytes() == previous_manifest
