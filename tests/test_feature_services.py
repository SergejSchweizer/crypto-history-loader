"""Tests for application-facing feature metadata and plotting interfaces."""

from __future__ import annotations

from pathlib import Path

import pytest

from application.services import feature_metadata_service, feature_plot_service


def test_feature_metadata_service_exposes_stable_contract_helpers() -> None:
    """Application services should use metadata through an application-owned boundary."""

    assert feature_metadata_service.feature_source_dataset("spot_close_price") == "spot_1m"
    assert feature_metadata_service.feature_hash(["a", "b"]) != feature_metadata_service.feature_hash(["b", "a"])


def test_feature_plot_service_delegates_to_plot_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Application services should use plot side effects through an explicit adapter boundary."""

    calls: list[tuple[object, Path, bool]] = []

    def fake_writer(frame: object, output_path: Path, *, normalize_y: bool = True) -> str:
        calls.append((frame, output_path, normalize_y))
        return str(output_path)

    monkeypatch.setattr(feature_plot_service, "_write_feature_distribution_plot", fake_writer)

    output = feature_plot_service.write_feature_distribution_plot(
        {"frame": "sentinel"},
        Path("plot.png"),
        normalize_y=False,
    )

    assert output == "plot.png"
    assert calls == [({"frame": "sentinel"}, Path("plot.png"), False)]
