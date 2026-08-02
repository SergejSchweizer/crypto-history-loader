"""Tests for read-only Medallion Gold freshness auditing."""

from __future__ import annotations

import json
from pathlib import Path

from application.services.medallion_freshness import audit_gold_history_freshness


def test_audit_gold_history_freshness_reports_current_missing_and_blocked(tmp_path: Path) -> None:
    """Manifest state determines the audit status without modifying the lake."""

    root = tmp_path / "gold"
    current = (
        root
        / "dataset_id=gold.history.full.m1"
        / "dataset_type=gold_symbol_dataset"
        / "feature_set_version=v1"
        / "exchange=deribit"
        / "symbol=BTC"
    )
    current.mkdir(parents=True)
    parquet = current / "BTC.parquet"
    parquet.write_bytes(b"parquet")
    parquet.with_suffix(".json").write_text(
        json.dumps({"source_dataset_id": "silver", "max_timestamp": "2026-01-01T00:00:00Z"}), encoding="utf-8"
    )
    blocked = (
        root
        / "dataset_id=gold.history.full.m5"
        / "dataset_type=gold_symbol_dataset"
        / "feature_set_version=v1"
        / "exchange=deribit"
        / "symbol=BTC"
    )
    blocked.mkdir(parents=True)
    (blocked / "BTC.parquet").write_bytes(b"parquet")

    records = audit_gold_history_freshness(gold_root=root, exchange="deribit", symbols=["BTC"])

    assert next(item for item in records if item["dataset_id"] == "gold.history.full.m1")["status"] == "current"
    assert next(item for item in records if item["dataset_id"] == "gold.history.full.m5")["status"] == "blocked"
    assert next(item for item in records if item["dataset_id"] == "gold.history.extended.h1")["status"] == "missing"
