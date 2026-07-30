"""Tests for the Gold lake mirror cron job."""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import sync_gold_to_temp
from scripts.sync_gold_to_temp import mirror_gold_tree


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_mirror_gold_tree_copies_updates_and_deletes_stale_files(tmp_path: Path) -> None:
    source = tmp_path / "lake" / "gold"
    destination = tmp_path / "volume1" / "Temp" / "gold"
    _write_file(source / "dataset_id=gold.market.full.m1" / "exchange=deribit" / "symbol=BTC" / "file.parquet", "new")
    _write_file(source / "dataset_id=gold.market.full.m1" / "exchange=deribit" / "symbol=BTC" / "meta.json", "{}")
    _write_file(
        destination / "dataset_id=gold.market.full.m1" / "exchange=deribit" / "symbol=BTC" / "stale.parquet", "old-old"
    )
    _write_file(
        destination / "dataset_id=gold.market.full.m1" / "exchange=deribit" / "symbol=BTC" / "file.parquet", "old-old"
    )
    _write_file(
        destination / "dataset_id=gold.market.full.m1" / "exchange=deribit" / "symbol=ETH" / "stale.parquet", "old-old"
    )

    logger = logging.getLogger("test.sync-gold")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())

    report = mirror_gold_tree(source_root=source, destination_root=destination, logger=logger)

    assert report.copied_files + report.skipped_files == 2
    assert report.deleted_files >= 2
    assert (
        destination / "dataset_id=gold.market.full.m1" / "exchange=deribit" / "symbol=BTC" / "file.parquet"
    ).read_text(encoding="utf-8") == "new"
    assert not (
        destination / "dataset_id=gold.market.full.m1" / "exchange=deribit" / "symbol=BTC" / "stale.parquet"
    ).exists()
    assert not (destination / "dataset_id=gold.market.full.m1" / "exchange=deribit" / "symbol=ETH").exists()


def test_mirror_gold_tree_rejects_nested_destination(tmp_path: Path) -> None:
    source = tmp_path / "lake" / "gold"
    destination = source / "nested" / "gold"
    source.mkdir(parents=True)

    logger = logging.getLogger("test.sync-gold")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())

    with pytest.raises(ValueError, match="destination root must not be the source root"):
        mirror_gold_tree(source_root=source, destination_root=destination, logger=logger)


def test_parse_args_defaults_to_nightly_gold_sync_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["sync_gold_to_temp.py"])
    args = sync_gold_to_temp.parse_args()

    assert args.source_root == "lake/gold"
    assert args.destination_root == "/volume1/Temp/gold"
    assert args.debug is False
    assert args.lock_file.endswith(".run/sync-gold-to-temp.lock")


def test_cron_recipe_declares_2330_sync_schedule() -> None:
    cron_file = Path(__file__).resolve().parents[1] / "docs" / "cron" / "gold-sync.cron"
    content = cron_file.read_text(encoding="utf-8").splitlines()

    assert content[0].startswith("# Mirror the current Gold lake")
    assert content[1].startswith("30 23 * * * ")
    assert "/volume1/Temp/gold" in content[1]
    assert "scripts/sync_gold_to_temp.py" in content[1]
    assert "--source-root lake/gold" in content[1]
    assert "--lock-file .run/sync-gold-to-temp.lock" in content[1]


def test_script_runs_from_repo_path_with_help() -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "sync_gold_to_temp.py"
    result = subprocess.run([sys.executable, str(script), "--help"], check=True, capture_output=True, text=True)

    assert "Mirror the current Gold lake to /volume1/Temp/gold" in result.stdout
