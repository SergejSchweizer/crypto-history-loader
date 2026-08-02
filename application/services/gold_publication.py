"""Atomic publication of validated Gold data artifacts and manifests."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Protocol
from uuid import uuid4


class ParquetFrame(Protocol):
    """Minimal frame contract required by the Gold publication adapter."""

    def write_parquet(self, file: Path) -> None:
        """Write the frame to the supplied parquet path."""


@dataclass(frozen=True)
class GoldArtifactPublishRequest:
    """One parquet and manifest pair participating in a Gold publication transaction."""

    frame: ParquetFrame
    parquet_path: Path
    manifest_path: Path
    manifest_payload: dict[str, object]


def publish_gold_artifact_atomically(
    *,
    frame: ParquetFrame,
    parquet_path: Path,
    manifest_path: Path,
    manifest_payload: dict[str, object],
) -> None:
    """Publish a validated Gold parquet and manifest while retaining the prior pair on failure.

    The two files are staged on the destination filesystem before either becomes visible.  A
    manifest is the lineage authority, so a failed publication always restores the previously
    published pair rather than leaving a new parquet referenced by stale metadata.
    """

    if parquet_path.parent != manifest_path.parent:
        raise ValueError("Gold parquet and manifest must share one artifact directory")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    token = uuid4().hex
    staged_parquet = parquet_path.with_name(f".{parquet_path.name}.{token}.tmp")
    staged_manifest = manifest_path.with_name(f".{manifest_path.name}.{token}.tmp")
    previous_parquet = parquet_path.with_name(f".{parquet_path.name}.{token}.previous")
    previous_manifest = manifest_path.with_name(f".{manifest_path.name}.{token}.previous")
    had_parquet = parquet_path.exists()
    had_manifest = manifest_path.exists()

    try:
        frame.write_parquet(staged_parquet)
        _validate_parquet(staged_parquet)
        _write_json_fsync(staged_manifest, manifest_payload)
        _validate_manifest(staged_manifest, expected_dataset_id=manifest_payload.get("dataset_id"))
        if had_parquet:
            os.replace(parquet_path, previous_parquet)
        if had_manifest:
            os.replace(manifest_path, previous_manifest)
        os.replace(staged_parquet, parquet_path)
        os.replace(staged_manifest, manifest_path)
    except Exception:
        if previous_parquet.exists():
            os.replace(previous_parquet, parquet_path)
        elif not had_parquet:
            parquet_path.unlink(missing_ok=True)
        if previous_manifest.exists():
            os.replace(previous_manifest, manifest_path)
        elif not had_manifest:
            manifest_path.unlink(missing_ok=True)
        raise
    finally:
        staged_parquet.unlink(missing_ok=True)
        staged_manifest.unlink(missing_ok=True)
        previous_parquet.unlink(missing_ok=True)
        previous_manifest.unlink(missing_ok=True)


def publish_gold_artifacts_atomically(*, requests: list[GoldArtifactPublishRequest]) -> None:
    """Publish a complete sibling artifact set or restore every previous pair on failure.

    Args:
        requests: Non-empty Gold artifact pairs sharing a single publication transaction.

    Raises:
        ValueError: If requests are empty or contain duplicate target paths.
        Exception: Any staging or publication error after restoring the prior artifacts.
    """

    if not requests:
        raise ValueError("Gold publication transaction requires at least one artifact")
    all_paths = [path for request in requests for path in (request.parquet_path, request.manifest_path)]
    if len(all_paths) != len(set(all_paths)):
        raise ValueError("Gold publication transaction contains duplicate artifact paths")
    token = uuid4().hex
    staged: list[tuple[GoldArtifactPublishRequest, Path, Path, Path, Path, bool, bool]] = []
    temporary_paths: list[Path] = []
    try:
        for request in requests:
            if request.parquet_path.parent != request.manifest_path.parent:
                raise ValueError("Gold parquet and manifest must share one artifact directory")
            request.parquet_path.parent.mkdir(parents=True, exist_ok=True)
            staged_parquet = request.parquet_path.with_name(f".{request.parquet_path.name}.{token}.tmp")
            staged_manifest = request.manifest_path.with_name(f".{request.manifest_path.name}.{token}.tmp")
            previous_parquet = request.parquet_path.with_name(f".{request.parquet_path.name}.{token}.previous")
            previous_manifest = request.manifest_path.with_name(f".{request.manifest_path.name}.{token}.previous")
            temporary_paths.extend([staged_parquet, staged_manifest, previous_parquet, previous_manifest])
            request.frame.write_parquet(staged_parquet)
            _validate_parquet(staged_parquet)
            _write_json_fsync(staged_manifest, request.manifest_payload)
            _validate_manifest(staged_manifest, expected_dataset_id=request.manifest_payload.get("dataset_id"))
            staged.append(
                (
                    request,
                    staged_parquet,
                    staged_manifest,
                    previous_parquet,
                    previous_manifest,
                    request.parquet_path.exists(),
                    request.manifest_path.exists(),
                )
            )
        for (
            request,
            staged_parquet,
            staged_manifest,
            previous_parquet,
            previous_manifest,
            had_parquet,
            had_manifest,
        ) in staged:
            if had_parquet:
                os.replace(request.parquet_path, previous_parquet)
            if had_manifest:
                os.replace(request.manifest_path, previous_manifest)
            os.replace(staged_parquet, request.parquet_path)
            os.replace(staged_manifest, request.manifest_path)
    except Exception:
        for (
            request,
            _staged_parquet,
            _staged_manifest,
            previous_parquet,
            previous_manifest,
            had_parquet,
            had_manifest,
        ) in staged:
            if previous_parquet.exists():
                os.replace(previous_parquet, request.parquet_path)
            elif not had_parquet:
                request.parquet_path.unlink(missing_ok=True)
            if previous_manifest.exists():
                os.replace(previous_manifest, request.manifest_path)
            elif not had_manifest:
                request.manifest_path.unlink(missing_ok=True)
        raise
    finally:
        for path in temporary_paths:
            path.unlink(missing_ok=True)


def _validate_parquet(path: Path) -> None:
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for Gold parquet publication.") from exc
    pl.read_parquet(path, n_rows=1)


def _write_json_fsync(path: Path, payload: dict[str, object]) -> None:
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temporary_path = Path(handle.name)
        try:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
            os.replace(temporary_path, path)
        finally:
            temporary_path.unlink(missing_ok=True)


def _validate_manifest(path: Path, *, expected_dataset_id: object) -> None:
    payload: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("dataset_id") != expected_dataset_id:
        raise ValueError("Invalid Gold manifest")
