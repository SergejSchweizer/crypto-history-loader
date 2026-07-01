"""Application-facing feature plot side-effect interface."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ingestion.feature_profile import write_feature_distribution_plot as _write_feature_distribution_plot


def write_feature_distribution_plot(
    frame: Any,
    output_path: Path,
    *,
    normalize_y: bool = True,
) -> str | None:
    """Write a numeric feature profile plot and return the resolved path when generated."""

    return _write_feature_distribution_plot(frame, output_path, normalize_y=normalize_y)
