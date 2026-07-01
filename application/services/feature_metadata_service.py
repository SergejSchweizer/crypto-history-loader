"""Application-facing feature metadata contract helpers."""

from __future__ import annotations

from typing import Any

from ingestion.feature_metadata import (
    feature_hash as _feature_hash,
)
from ingestion.feature_metadata import (
    feature_metadata as _feature_metadata,
)
from ingestion.feature_metadata import (
    feature_source_dataset as _feature_source_dataset,
)


def feature_hash(columns: list[str]) -> str:
    """Return a stable short hash for an ordered feature column set."""

    return _feature_hash(columns)


def feature_source_dataset(column_name: str) -> str:
    """Infer the source dataset label from a derived feature column name."""

    return _feature_source_dataset(column_name)


def feature_metadata(pl: Any, frame: Any, exchange: str) -> dict[str, dict[str, object]]:
    """Build manifest metadata for each feature column in a frame."""

    return _feature_metadata(pl, frame, exchange)
