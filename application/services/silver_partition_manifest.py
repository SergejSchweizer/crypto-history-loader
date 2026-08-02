"""Deterministic Silver partition fingerprints and atomic publication."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal, Protocol
from uuid import uuid4

MANIFEST_VERSION = "silver_partition_manifest/v1"
BUILD_STATUS = Literal["published"]


class ParquetFrame(Protocol):
    """Minimal Polars frame protocol used by the publication adapter."""

    @property
    def columns(self) -> list[str]: ...

    @property
    def height(self) -> int: ...

    def write_parquet(self, file: Path) -> None: ...


@dataclass(frozen=True)
class SilverPartitionManifest:
    """Versioned cache contract for one Silver parquet partition."""

    version: str
    status: BUILD_STATUS
    input_fingerprint: str
    output_fingerprint: str
    source_schema_signature: str
    output_schema_signature: str
    row_count: int
    sort_keys: tuple[str, ...]
    deduplication_keys: tuple[str, ...]
    builder_contract_version: str

    def to_dict(self) -> dict[str, object]:
        """Return a stable JSON-compatible manifest payload."""

        return asdict(self)


def performance_manifest_path(parquet_path: Path) -> Path:
    """Return the non-conflicting performance manifest path for a Silver artifact."""

    return parquet_path.with_suffix(".performance.json")


def schema_signature(schema: object) -> str:
    """Return a stable digest for a schema or ordered column collection."""

    if isinstance(schema, dict):
        normalized: object = [(str(key), str(value)) for key, value in schema.items()]
    else:
        normalized = [str(value) for value in schema] if isinstance(schema, (list, tuple)) else str(schema)
    return _digest(normalized)


def source_fingerprint(
    *,
    bronze_root: Path,
    source_files: list[str],
    source_schema: object,
    exchange: str,
    symbol: str,
    timeframe: str,
    builder_contract_version: str,
) -> str:
    """Fingerprint the exact Bronze input identity and Silver build contract.

    File content hashes deliberately complement metadata so a restored file with the
    same size and timestamp cannot be mistaken for an unchanged input.
    """

    root = bronze_root.resolve()
    entries: list[dict[str, object]] = []
    for source_file in sorted(Path(value) for value in source_files):
        stat = source_file.stat()
        entries.append(
            {
                "path": source_file.resolve().relative_to(root).as_posix(),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "sha256": _file_digest(source_file),
            }
        )
    return _digest(
        {
            "source_files": entries,
            "source_schema": schema_signature(source_schema),
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "builder_contract_version": builder_contract_version,
        }
    )


def load_current_manifest(
    *,
    parquet_path: Path,
    expected_input_fingerprint: str,
    expected_builder_contract_version: str,
) -> SilverPartitionManifest | None:
    """Return a valid matching manifest, otherwise treat the partition as a cache miss."""

    manifest_path = performance_manifest_path(parquet_path)
    if not parquet_path.is_file() or not manifest_path.is_file():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest = SilverPartitionManifest(
            version=str(payload["version"]),
            status=payload["status"],
            input_fingerprint=str(payload["input_fingerprint"]),
            output_fingerprint=str(payload["output_fingerprint"]),
            source_schema_signature=str(payload["source_schema_signature"]),
            output_schema_signature=str(payload["output_schema_signature"]),
            row_count=int(payload["row_count"]),
            sort_keys=tuple(str(value) for value in payload["sort_keys"]),
            deduplication_keys=tuple(str(value) for value in payload["deduplication_keys"]),
            builder_contract_version=str(payload["builder_contract_version"]),
        )
    except KeyError, TypeError, ValueError, json.JSONDecodeError:
        return None
    if (
        manifest.version != MANIFEST_VERSION
        or manifest.status != "published"
        or manifest.input_fingerprint != expected_input_fingerprint
        or manifest.builder_contract_version != expected_builder_contract_version
        or manifest.output_fingerprint != _file_digest(parquet_path)
    ):
        return None
    return manifest


def publish_partition_atomically(
    *,
    frame: ParquetFrame,
    parquet_path: Path,
    input_fingerprint: str,
    source_schema: object,
    sort_keys: tuple[str, ...],
    deduplication_keys: tuple[str, ...],
    builder_contract_version: str,
) -> SilverPartitionManifest:
    """Publish a validated parquet artifact and its manifest with rollback on failure."""

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = performance_manifest_path(parquet_path)
    token = uuid4().hex
    staged_parquet = parquet_path.with_name(f".{parquet_path.name}.{token}.tmp")
    staged_manifest = manifest_path.with_name(f".{manifest_path.name}.{token}.tmp")
    backup_parquet = parquet_path.with_name(f".{parquet_path.name}.{token}.previous")
    had_previous = parquet_path.exists()

    try:
        frame.write_parquet(staged_parquet)
        _validate_parquet(staged_parquet)
        manifest = SilverPartitionManifest(
            version=MANIFEST_VERSION,
            status="published",
            input_fingerprint=input_fingerprint,
            output_fingerprint=_file_digest(staged_parquet),
            source_schema_signature=schema_signature(source_schema),
            output_schema_signature=schema_signature(getattr(frame, "schema", frame.columns)),
            row_count=frame.height,
            sort_keys=sort_keys,
            deduplication_keys=deduplication_keys,
            builder_contract_version=builder_contract_version,
        )
        _write_json_fsync(staged_manifest, manifest.to_dict())
        _validate_manifest(staged_manifest)
        if had_previous:
            os.replace(parquet_path, backup_parquet)
        os.replace(staged_parquet, parquet_path)
        os.replace(staged_manifest, manifest_path)
    except Exception:
        if had_previous and backup_parquet.exists():
            os.replace(backup_parquet, parquet_path)
        elif not had_previous:
            parquet_path.unlink(missing_ok=True)
        raise
    finally:
        staged_parquet.unlink(missing_ok=True)
        staged_manifest.unlink(missing_ok=True)
        backup_parquet.unlink(missing_ok=True)
    return manifest


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _file_digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _validate_parquet(path: Path) -> None:
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for Silver parquet publication.") from exc
    pl.read_parquet(path, n_rows=1)


def _write_json_fsync(path: Path, payload: dict[str, object]) -> None:
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temporary_path = Path(handle.name)
        try:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
            os.replace(temporary_path, path)
        finally:
            temporary_path.unlink(missing_ok=True)


def _validate_manifest(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("version") != MANIFEST_VERSION or payload.get("status") != "published":
        raise ValueError("Invalid Silver performance manifest")
