"""Read-only freshness audit for canonical and extended Gold history artifacts."""

from __future__ import annotations

import json
from pathlib import Path

GOLD_HISTORY_DATASET_IDS = (
    "gold.history.full.m1",
    "gold.history.full.m5",
    "gold.history.full.m30",
    "gold.history.full.h1",
    "gold.history.extended.m1",
    "gold.history.extended.m5",
    "gold.history.extended.m30",
    "gold.history.extended.h1",
)


def audit_gold_history_freshness(*, gold_root: Path, exchange: str, symbols: list[str]) -> list[dict[str, object]]:
    """Return deterministic artifact availability for Gold history dataset partitions.

    Args:
        gold_root: Root directory of the Gold lake.
        exchange: Exchange partition to inspect.
        symbols: Canonical symbols to audit.

    Returns:
        One read-only status record per dataset and symbol, sorted deterministically.
    """

    records: list[dict[str, object]] = []
    for dataset_id in GOLD_HISTORY_DATASET_IDS:
        for symbol in sorted(set(symbols)):
            artifacts = sorted(
                (gold_root / f"dataset_id={dataset_id}" / "dataset_type=gold_symbol_dataset").glob(
                    f"feature_set_version=*/exchange={exchange}/symbol={symbol}/*.parquet"
                ),
                key=lambda path: (path.stat().st_mtime, str(path)),
            )
            if not artifacts:
                records.append({"dataset_id": dataset_id, "symbol": symbol, "status": "missing"})
                continue
            parquet_path = artifacts[-1]
            manifest_path = parquet_path.with_suffix(".json")
            if not manifest_path.is_file():
                records.append({"dataset_id": dataset_id, "symbol": symbol, "status": "blocked"})
                continue
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            records.append(
                {
                    "dataset_id": dataset_id,
                    "symbol": symbol,
                    "status": "current",
                    "source_dataset_id": manifest.get("source_dataset_id"),
                    "max_timestamp": manifest.get("max_timestamp"),
                }
            )
    return records
