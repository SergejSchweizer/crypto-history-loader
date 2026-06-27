"""Tests for Bronze parquet sidecar helpers."""

from __future__ import annotations

from ingestion.lake_sidecars import DEFAULT_BRONZE_SIDECAR_DATASET_TYPES, ensure_bronze_sidecars


def test_default_bronze_sidecar_dataset_types_are_stable() -> None:
    """Guard the default repair scan surface for Bronze sidecars."""

    assert DEFAULT_BRONZE_SIDECAR_DATASET_TYPES == (
        "spot",
        "perp",
        "oi",
        "funding",
        "perp_trades",
        "option_trades",
    )


def test_ensure_bronze_sidecars_returns_empty_for_missing_root(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """Missing lake roots are a no-op for sidecar repair."""

    assert ensure_bronze_sidecars(lake_root=str(tmp_path / "missing")) == []
