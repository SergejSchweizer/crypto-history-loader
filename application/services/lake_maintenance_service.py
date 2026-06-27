"""Application-facing maintenance helpers for parquet lake side effects."""

from __future__ import annotations

from ingestion.lake_sidecars import ensure_bronze_sidecars as _ensure_bronze_sidecars


def ensure_bronze_sidecars(*, lake_root: str) -> list[str]:
    """Repair missing Bronze sidecar files through the lake maintenance adapter."""

    return _ensure_bronze_sidecars(lake_root=lake_root)
