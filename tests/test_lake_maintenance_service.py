"""Tests for application-facing lake maintenance helpers."""

from __future__ import annotations

from pathlib import Path

from application.services import lake_maintenance_service


def test_ensure_bronze_sidecars_delegates_to_lake_sidecar_adapter(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """Application callers should not need to import lake sidecar repair adapters directly."""

    captured: dict[str, object] = {}

    def _fake_ensure_bronze_sidecars(
        *,
        lake_root: str,
        dataset_types: list[str] | None = None,
        log_fn: object | None = None,
    ) -> list[str]:
        captured["lake_root"] = lake_root
        captured["dataset_types"] = dataset_types
        captured["log_fn"] = log_fn
        return ["manifest.json"]

    monkeypatch.setattr(lake_maintenance_service, "_ensure_bronze_sidecars", _fake_ensure_bronze_sidecars)

    log_fn = object()
    assert lake_maintenance_service.ensure_bronze_sidecars(
        lake_root=str(tmp_path),
        dataset_types=["perps_trades"],
        log_fn=log_fn,
    ) == ["manifest.json"]
    assert captured == {"lake_root": str(tmp_path), "dataset_types": ["perps_trades"], "log_fn": log_fn}
