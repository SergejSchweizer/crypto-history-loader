"""Deterministic artifact identities for incremental Gold input planning."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


def gold_input_fingerprint(
    *,
    root: Path,
    required_files: dict[str, list[Path]],
    optional_files: dict[str, list[Path]],
    dataset_id: str,
    contract_version: str,
    feature_configuration: dict[str, object] | None = None,
) -> str:
    """Hash exact required and optional Silver artifact identity for one Gold build.

    Args:
        root: Common root used to make source paths machine-independent.
        required_files: Required dataset keys and their parquet artifacts.
        optional_files: Optional dataset keys and any available parquet artifacts.
        dataset_id: Gold output dataset identifier.
        contract_version: Versioned Gold input contract.
        feature_configuration: Effective versioned configuration of Gold feature families.

    Returns:
        Stable SHA-256 fingerprint that represents source contents and availability.

    Raises:
        ValueError: If a required dataset has no artifacts or a path is outside root.
    """

    payload = {
        "dataset_id": dataset_id,
        "contract_version": contract_version,
        "feature_configuration": feature_configuration or {},
        "required": _dataset_entries(root, required_files, required=True),
        "optional": _dataset_entries(root, optional_files, required=False),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def gold_input_artifact_fingerprints(
    *,
    root: Path,
    required_files: dict[str, list[Path]],
    optional_files: dict[str, list[Path]],
) -> dict[str, str]:
    """Return deterministic per-artifact identities used for Gold incremental planning.

    Keys retain the source dataset name and a root-relative parquet path.  This makes
    manifest comparisons independent of the machine-specific lake root while allowing
    the planner to recover the owning Silver month partition.

    Args:
        root: Common root used to make source paths machine-independent.
        required_files: Required dataset keys and their parquet artifacts.
        optional_files: Optional dataset keys and any available parquet artifacts.

    Returns:
        Stable artifact-key to fingerprint mapping.

    Raises:
        ValueError: If a required dataset has no artifacts or a path is outside root.
    """

    entries: dict[str, str] = {}
    for required, datasets in ((True, required_files), (False, optional_files)):
        for dataset, files in sorted(datasets.items()):
            if required and not files:
                raise ValueError(f"Required Gold input dataset has no artifacts: {dataset}")
            for path in sorted(files):
                artifact = _artifact_entry(root, path)
                key = f"{dataset}:{artifact['path']}"
                entries[key] = hashlib.sha256(
                    json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest()
    return entries


def _dataset_entries(root: Path, datasets: dict[str, list[Path]], *, required: bool) -> dict[str, object]:
    entries: dict[str, object] = {}
    for dataset, files in sorted(datasets.items()):
        if required and not files:
            raise ValueError(f"Required Gold input dataset has no artifacts: {dataset}")
        entries[dataset] = {
            "available": bool(files),
            "artifacts": [_artifact_entry(root, path) for path in sorted(files)],
        }
    return entries


def _artifact_entry(root: Path, path: Path) -> dict[str, object]:
    manifest_path = path.with_suffix(".json")
    return {
        "path": path.resolve().relative_to(root.resolve()).as_posix(),
        "sha256": _digest_file(path),
        "manifest": json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else None,
    }


def _digest_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()
