"""Atomic publication of validated Gold data artifacts and manifests."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Protocol
from uuid import uuid4

GOLD_MANIFEST_VERSION = 2
GOLD_PUBLICATION_RESULT_VERSION = 1


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


@dataclass(frozen=True, slots=True, order=True)
class GoldPublishedArtifact:
    """Certified artifact identity approved by one successful Gold command."""

    dataset_id: str
    exchange: str
    symbol: str
    parquet_path: str
    manifest_path: str

    @classmethod
    def from_report(cls, report: dict[str, object]) -> GoldPublishedArtifact:
        """Build a publication entry from a successful Gold build report."""

        required = ("dataset_id", "exchange", "symbol", "parquet_path", "manifest_path")
        values = {name: report.get(name) for name in required}
        if any(not isinstance(value, str) or not value for value in values.values()):
            raise ValueError("Gold build report is missing publication identity")
        return cls(
            dataset_id=str(values["dataset_id"]),
            exchange=str(values["exchange"]),
            symbol=str(values["symbol"]),
            parquet_path=str(values["parquet_path"]),
            manifest_path=str(values["manifest_path"]),
        )

    def to_dict(self) -> dict[str, str]:
        """Return a deterministic JSON-safe publication entry."""

        return {
            "dataset_id": self.dataset_id,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "parquet_path": self.parquet_path,
            "manifest_path": self.manifest_path,
        }


def write_gold_publication_result(path: Path, reports: list[dict[str, object]]) -> None:
    """Atomically declare the exact artifact set approved by a successful Gold command."""

    artifacts = sorted(GoldPublishedArtifact.from_report(report) for report in reports)
    lineages = [(artifact.dataset_id, artifact.exchange, artifact.symbol) for artifact in artifacts]
    if len(lineages) != len(set(lineages)):
        raise ValueError("Gold publication result contains duplicate lineages")
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_json_fsync(
        path,
        {
            "result_version": GOLD_PUBLICATION_RESULT_VERSION,
            "status": "success",
            "artifacts": [artifact.to_dict() for artifact in artifacts],
            "serving_deprecations": [],
        },
    )


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
        attested_payload = _attested_manifest_payload(manifest_payload, staged_parquet)
        _write_json_fsync(staged_manifest, attested_payload)
        _validate_manifest(staged_manifest, expected_dataset_id=manifest_payload.get("dataset_id"))
        if had_parquet:
            os.replace(parquet_path, previous_parquet)
        if had_manifest:
            os.replace(manifest_path, previous_manifest)
        os.replace(staged_parquet, parquet_path)
        os.replace(staged_manifest, manifest_path)
        validate_gold_artifact_attestation(parquet_path, manifest_path)
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
            attested_payload = _attested_manifest_payload(request.manifest_payload, staged_parquet)
            _write_json_fsync(staged_manifest, attested_payload)
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
            validate_gold_artifact_attestation(request.parquet_path, request.manifest_path)
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


def _sha256_file(path: Path) -> str:
    """Return the SHA-256 of exact artifact bytes."""

    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _attested_manifest_payload(payload: dict[str, object], parquet_path: Path) -> dict[str, object]:
    """Return a manifest-v2 copy attesting its paired Parquet bytes."""

    attested = dict(payload)
    attested["manifest_version"] = GOLD_MANIFEST_VERSION
    attested["output_sha256"] = _sha256_file(parquet_path)
    attested["schema_signature"] = _parquet_schema_signature(parquet_path)
    return attested


def _parquet_schema_signature(path: Path) -> str:
    """Return the canonical signature of the ordered Parquet schema."""

    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for Gold parquet publication.") from exc
    schema = pl.read_parquet_schema(path)
    canonical = json.dumps([(name, str(dtype)) for name, dtype in schema.items()], separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()


def validate_gold_artifact_attestation(parquet_path: Path, manifest_path: Path) -> None:
    """Fail unless a manifest-v2 hash matches the exact paired Parquet bytes."""

    payload: Any = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("manifest_version") != GOLD_MANIFEST_VERSION:
        raise ValueError("Gold manifest does not use the certified manifest version")
    expected_hash = payload.get("output_sha256")
    if not isinstance(expected_hash, str) or len(expected_hash) != 64:
        raise ValueError("Gold manifest output SHA-256 is missing or invalid")
    if _sha256_file(parquet_path) != expected_hash:
        raise ValueError("Gold Parquet output SHA-256 does not match its manifest")


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
