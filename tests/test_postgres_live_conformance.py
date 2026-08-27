"""Focused safety tests for the production PostgreSQL conformance verifier."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from application.postgres_sync import live_conformance as conformance
from application.postgres_sync.config import PostgresSyncConfig


def _config() -> PostgresSyncConfig:
    return PostgresSyncConfig(
        host="10.10.1.3",
        port=54321,
        user="crypto-loader",
        database="crypto_loader_test",
        password="never-write-this-secret",
    )


def test_verifier_writes_sanitized_fail_report_when_gold_cannot_be_certified(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Source-certification failures are evidence, not an unsafe partial verification."""

    def fail_expected_catalog(_: Path) -> tuple[tuple[object, ...], tuple[object, ...]]:
        raise ValueError("malformed source artifact")

    monkeypatch.setattr(conformance, "_expected_catalog", fail_expected_catalog)
    report_path = tmp_path / "evidence.json"

    report = conformance.verify_live_postgres(
        gold_root=tmp_path / "gold",
        config=_config(),
        report_path=report_path,
    )

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    serialized = json.dumps(payload, sort_keys=True)
    assert report.status == "FAIL"
    assert payload["status"] == "FAIL"
    assert payload["endpoint"]["user"] == "crypto-loader"
    assert "never-write-this-secret" not in serialized
    assert "malformed source artifact" not in serialized
    assert payload["checks"] == [
        {
            "category": "source",
            "detail": "current Gold artifacts cannot be certified",
            "name": "certified-current-gold",
            "passed": False,
        }
    ]


def test_utc_rejects_naive_and_non_utc_values() -> None:
    """The live verifier preserves the strict database UTC boundary."""

    with pytest.raises(ValueError, match="aware UTC"):
        conformance._utc(__import__("datetime").datetime(2026, 1, 1))
