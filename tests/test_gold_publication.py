"""Tests for atomic Gold artifact publication."""

from __future__ import annotations

from pathlib import Path

import pytest

from application.services import gold_publication

pl = pytest.importorskip("polars")


def test_publish_gold_artifact_atomically_writes_valid_pair(tmp_path: Path) -> None:
    """A successful publication exposes a readable parquet and matching manifest together."""

    parquet_path = tmp_path / "artifact.parquet"
    manifest_path = tmp_path / "artifact.json"
    gold_publication.publish_gold_artifact_atomically(
        frame=pl.DataFrame({"timestamp_m1": [1], "value": [2.0]}),
        parquet_path=parquet_path,
        manifest_path=manifest_path,
        manifest_payload={"dataset_id": "gold.history.full.m1", "rows_out": 1},
    )

    assert pl.read_parquet(parquet_path).to_dict(as_series=False) == {"timestamp_m1": [1], "value": [2.0]}
    assert '"dataset_id": "gold.history.full.m1"' in manifest_path.read_text(encoding="utf-8")


def test_publish_gold_artifact_atomically_preserves_previous_pair_on_validation_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed staged validation leaves the last valid parquet and manifest untouched."""

    parquet_path = tmp_path / "artifact.parquet"
    manifest_path = tmp_path / "artifact.json"
    old_frame = pl.DataFrame({"value": [1.0]})
    gold_publication.publish_gold_artifact_atomically(
        frame=old_frame,
        parquet_path=parquet_path,
        manifest_path=manifest_path,
        manifest_payload={"dataset_id": "gold.history.full.m1"},
    )
    old_parquet = parquet_path.read_bytes()
    old_manifest = manifest_path.read_text(encoding="utf-8")
    monkeypatch.setattr(gold_publication, "_validate_parquet", lambda _path: (_ for _ in ()).throw(ValueError("bad")))

    with pytest.raises(ValueError, match="bad"):
        gold_publication.publish_gold_artifact_atomically(
            frame=pl.DataFrame({"value": [2.0]}),
            parquet_path=parquet_path,
            manifest_path=manifest_path,
            manifest_payload={"dataset_id": "gold.history.full.m1"},
        )

    assert parquet_path.read_bytes() == old_parquet
    assert manifest_path.read_text(encoding="utf-8") == old_manifest


def test_publish_gold_artifacts_atomically_publishes_or_restores_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A sibling transaction never exposes a partially updated timeframe set."""

    requests = [
        gold_publication.GoldArtifactPublishRequest(
            frame=pl.DataFrame({"value": [1.0]}),
            parquet_path=tmp_path / "m5.parquet",
            manifest_path=tmp_path / "m5.json",
            manifest_payload={"dataset_id": "gold.history.full.m5"},
        ),
        gold_publication.GoldArtifactPublishRequest(
            frame=pl.DataFrame({"value": [2.0]}),
            parquet_path=tmp_path / "m30.parquet",
            manifest_path=tmp_path / "m30.json",
            manifest_payload={"dataset_id": "gold.history.full.m30"},
        ),
    ]
    gold_publication.publish_gold_artifacts_atomically(requests=requests)
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    monkeypatch.setattr(gold_publication, "_validate_parquet", lambda _path: (_ for _ in ()).throw(ValueError("bad")))

    with pytest.raises(ValueError, match="bad"):
        gold_publication.publish_gold_artifacts_atomically(
            requests=[
                gold_publication.GoldArtifactPublishRequest(
                    frame=pl.DataFrame({"value": [3.0]}),
                    parquet_path=tmp_path / "m5.parquet",
                    manifest_path=tmp_path / "m5.json",
                    manifest_payload={"dataset_id": "gold.history.full.m5"},
                ),
                requests[1],
            ]
        )

    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before
