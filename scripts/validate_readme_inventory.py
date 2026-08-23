"""Validate the Gold dataset catalog against the typed contract inventory."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from application.dataset_contracts import supported_gold_dataset_ids
from application.services.dataset_inventory import build_dataset_inventory, inventory_to_markdown

GOLD_DATASET_RE = re.compile(r"`(gold\.[a-z0-9_.]+)`")
CONTRACT_TABLE_ID_RE = re.compile(r"^\| `(gold\.[a-z0-9_.]+)` \|", flags=re.MULTILINE)
CONTRACT_INVENTORY_MARKER = "## Contract inventory"
CONTRACT_DETAIL_MARKER = "## Dataset contracts and exact feature membership"


def documented_gold_dataset_ids(datasets_path: Path) -> set[str]:
    """Return Gold IDs from the canonical contract-inventory table."""

    text = datasets_path.read_text(encoding="utf-8")
    try:
        inventory_start = text.index(CONTRACT_INVENTORY_MARKER)
        detail_start = text.index(CONTRACT_DETAIL_MARKER, inventory_start)
    except ValueError as exc:
        raise ValueError(
            f"{datasets_path} must contain {CONTRACT_INVENTORY_MARKER!r} followed by {CONTRACT_DETAIL_MARKER!r}"
        ) from exc
    return set(CONTRACT_TABLE_ID_RE.findall(text[inventory_start:detail_start]))


def readme_gold_dataset_ids(readme_path: Path) -> set[str]:
    """Return Gold IDs from DATASETS.md or the former README section."""

    text = readme_path.read_text(encoding="utf-8")
    if CONTRACT_INVENTORY_MARKER in text and CONTRACT_DETAIL_MARKER in text:
        return documented_gold_dataset_ids(readme_path)

    marker = "Available Gold dataset IDs:"
    try:
        section_start = text.index(marker)
    except ValueError as exc:
        sibling_catalog = readme_path.with_name("DATASETS.md")
        if readme_path.name == "README.md" and sibling_catalog.exists():
            return documented_gold_dataset_ids(sibling_catalog)
        raise ValueError(f"{readme_path} missing section: {marker}") from exc

    section = text[section_start:]
    next_heading = re.search(r"\n##\s+", section)
    if next_heading is not None:
        section = section[: next_heading.start()]
    return set(GOLD_DATASET_RE.findall(section))


def contracted_gold_dataset_ids() -> set[str]:
    """Return Gold IDs declared by the typed contract registry."""

    return set(supported_gold_dataset_ids())


def _inventory_policy_errors(
    *,
    contract_ids: set[str],
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
) -> list[str]:
    errors: list[str] = []
    rows = build_dataset_inventory(bronze_root=bronze_root, silver_root=silver_root, gold_root=gold_root)
    markdown = inventory_to_markdown(rows)
    for dataset_id in sorted(contract_ids):
        if f"`{dataset_id}`" not in markdown:
            errors.append(f"Generated inventory markdown missing Gold dataset: {dataset_id}")
    for row in rows:
        if row.layer != "gold":
            continue
    expected_origin = "crypto-live-loader" if row.dataset.startswith("gold.live.") else "crypto-loader"
        if row.origin_repository != expected_origin:
            errors.append(
                f"Gold dataset has wrong origin: {row.dataset} -> {row.origin_repository}; expected {expected_origin}"
            )
    return errors


def validate_readme_inventory(
    *,
    readme_path: Path,
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
) -> list[str]:
    """Return dataset-catalog and generated-inventory drift errors."""

    errors: list[str] = []
    documented_ids = readme_gold_dataset_ids(readme_path)
    contract_ids = contracted_gold_dataset_ids()
    missing = sorted(contract_ids - documented_ids)
    extra = sorted(documented_ids - contract_ids)
    if missing:
        errors.append("DATASETS.md Gold dataset IDs missing contracts: " + ", ".join(missing))
    if extra:
        errors.append("DATASETS.md Gold dataset IDs contain unknown contracts: " + ", ".join(extra))
    errors.extend(
        _inventory_policy_errors(
            contract_ids=contract_ids,
            bronze_root=bronze_root,
            silver_root=silver_root,
            gold_root=gold_root,
        )
    )
    return errors


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--datasets",
        "--readme",
        dest="readme",
        type=Path,
        default=Path("DATASETS.md"),
        help="Path to the authoritative Gold dataset catalog.",
    )
    parser.add_argument("--bronze-root", type=Path, default=Path("lake/bronze"))
    parser.add_argument("--silver-root", type=Path, default=Path("lake/silver"))
    parser.add_argument("--gold-root", type=Path, default=Path("lake/gold"))
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the Gold dataset catalog drift validator."""

    args = _parser().parse_args(argv)
    errors = validate_readme_inventory(
        readme_path=args.readme,
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        gold_root=args.gold_root,
    )
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
