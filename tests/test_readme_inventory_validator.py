"""Tests for README inventory drift validation."""

from __future__ import annotations

from pathlib import Path

from application.dataset_contracts import GOLD_DATASET_CONTRACTS
from scripts.validate_readme_inventory import readme_gold_dataset_ids, validate_readme_inventory


def _write_readme(path: Path, dataset_ids: list[str]) -> None:
    lines = ["# Test", "", "Available Gold dataset IDs:", ""]
    lines.extend(f"- `{dataset_id}`" for dataset_id in dataset_ids)
    lines.extend(["", "## Next"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_readme_inventory_validator_accepts_contract_gold_dataset_ids(tmp_path: Path) -> None:
    """README Gold dataset list should match the typed Gold contract registry."""

    readme = tmp_path / "README.md"
    _write_readme(readme, sorted(GOLD_DATASET_CONTRACTS))

    assert readme_gold_dataset_ids(readme) == set(GOLD_DATASET_CONTRACTS)
    assert (
        validate_readme_inventory(
            readme_path=readme,
            bronze_root=tmp_path / "bronze",
            silver_root=tmp_path / "silver",
            gold_root=tmp_path / "gold",
        )
        == []
    )


def test_readme_inventory_validator_reports_missing_and_unknown_gold_ids(tmp_path: Path) -> None:
    """README drift should fail loudly before stale inventory docs ship."""

    readme = tmp_path / "README.md"
    contracted = sorted(GOLD_DATASET_CONTRACTS)
    missing = contracted[0]
    _write_readme(readme, [*contracted[1:], "gold.unknown.dataset.m1"])

    errors = validate_readme_inventory(
        readme_path=readme,
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
    )

    assert any(missing in error for error in errors)
    assert any("gold.unknown.dataset.m1" in error for error in errors)
