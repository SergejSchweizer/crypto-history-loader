"""Application-facing maintenance helpers for parquet lake side effects."""

from __future__ import annotations

from typing import Any

from ingestion.lake_sidecars import ensure_bronze_sidecars as _ensure_bronze_sidecars


def ensure_bronze_sidecars(
    *,
    lake_root: str,
    dataset_types: list[str] | None = None,
    log_fn: Any | None = None,
) -> list[str]:
    """Repair missing Bronze sidecar files through the lake maintenance adapter."""

    return _ensure_bronze_sidecars(lake_root=lake_root, dataset_types=dataset_types, log_fn=log_fn)
