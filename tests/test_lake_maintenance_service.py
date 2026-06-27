"""Tests for application-facing lake maintenance helpers."""

from __future__ import annotations

from pathlib import Path

from application.services import lake_maintenance_service


def test_ensure_bronze_sidecars_delegates_to_lake_sidecar_adapter(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """Application callers should not need to import lake sidecar repair adapters directly."""

    captured: list[str] = []

    def _fake_ensure_bronze_sidecars(*, lake_root: str) -> list[str]:
        captured.append(lake_root)
        return ["manifest.json"]

    monkeypatch.setattr(lake_maintenance_service, "_ensure_bronze_sidecars", _fake_ensure_bronze_sidecars)

    assert lake_maintenance_service.ensure_bronze_sidecars(lake_root=str(tmp_path)) == ["manifest.json"]
    assert captured == [str(tmp_path)]
