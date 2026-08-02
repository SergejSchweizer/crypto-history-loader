"""Tests for exact Gold input artifact identities."""

from __future__ import annotations

from pathlib import Path

import pytest

from application.services.gold_input_fingerprint import gold_input_artifact_fingerprints, gold_input_fingerprint


def _write(root: Path, name: str, content: str, manifest: str | None = None) -> Path:
    """Create a deterministic fixture artifact and optional adjacent manifest."""

    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    if manifest is not None:
        path.with_suffix(".json").write_text(manifest, encoding="utf-8")
    return path


def test_fingerprint_is_content_sensitive_and_tracks_missing_optionals(tmp_path: Path) -> None:
    """Represent optional availability explicitly without requiring nullable input rows."""

    required = _write(tmp_path, "silver/required.parquet", "v1", '{"version":"v1"}')
    optional = _write(tmp_path, "silver/optional.parquet", "optional")
    kwargs = {
        "root": tmp_path,
        "required_files": {"spot_ohlcv": [required]},
        "optional_files": {"historical_volatility": []},
        "dataset_id": "gold.history.full.m1",
        "contract_version": "gold-input/v1",
    }
    missing = gold_input_fingerprint(**kwargs)
    assert missing == gold_input_fingerprint(**kwargs)

    available = gold_input_fingerprint(**{**kwargs, "optional_files": {"historical_volatility": [optional]}})
    assert available != missing
    required.write_text("v2", encoding="utf-8")
    assert gold_input_fingerprint(**kwargs) != missing


def test_fingerprint_rejects_missing_required_artifacts(tmp_path: Path) -> None:
    """Fail planning before a Gold build can silently omit a contract source."""

    with pytest.raises(ValueError, match="spot_ohlcv"):
        gold_input_fingerprint(
            root=tmp_path,
            required_files={"spot_ohlcv": []},
            optional_files={},
            dataset_id="gold.history.full.m1",
            contract_version="gold-input/v1",
        )


def test_artifact_fingerprints_are_individual_and_content_sensitive(tmp_path: Path) -> None:
    """Incremental planning can pinpoint the changed Silver artifact without absolute paths."""

    first = _write(tmp_path, "silver/month=2026-01/first.parquet", "one")
    second = _write(tmp_path, "silver/month=2026-02/second.parquet", "two")
    fingerprints = gold_input_artifact_fingerprints(
        root=tmp_path,
        required_files={"spot_ohlcv": [first, second]},
        optional_files={},
    )
    assert sorted(fingerprints) == [
        "spot_ohlcv:silver/month=2026-01/first.parquet",
        "spot_ohlcv:silver/month=2026-02/second.parquet",
    ]
    second.write_text("changed", encoding="utf-8")
    changed = gold_input_artifact_fingerprints(
        root=tmp_path,
        required_files={"spot_ohlcv": [first, second]},
        optional_files={},
    )
    assert (
        changed[next(key for key in changed if "2026-02" in key)]
        != fingerprints[next(key for key in fingerprints if "2026-02" in key)]
    )
