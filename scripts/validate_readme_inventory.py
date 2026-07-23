"""Validate DATASETS.md against typed Gold contracts and physical inventory policy."""

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
FEATURE_DICTIONARY_MARKER = "## Feature dictionary"


def documented_gold_dataset_ids(datasets_path: Path) -> set[str]:
    """Return Gold IDs from the canonical contract-inventory table.

    Args:
        datasets_path: Path to DATASETS.md.

    Returns:
        Dataset IDs declared in the contract inventory.

    Raises:
        ValueError: If required catalog markers are absent or out of order.
    """

    text = datasets_path.read_text(encoding="utf-8")
    try:
        inventory_start = text.index(CONTRACT_INVENTORY_MARKER)
        detail_start = text.index(CONTRACT_DETAIL_MARKER, inventory_start)
    except ValueError as exc:
        raise ValueError(
            f"{datasets_path} must contain {CONTRACT_INVENTORY_MARKER!r} followed by "
            f"{CONTRACT_DETAIL_MARKER!r}"
        ) from exc
    return set(CONTRACT_TABLE_ID_RE.findall(text[inventory_start:detail_start]))


def contracted_gold_dataset_ids() -> set[str]:
    """Return Gold IDs declared by the typed contract registry."""

    return set(supported_gold_dataset_ids())


def _dataset_section(text: str, dataset_id: str) -> str | None:
    marker = f"### {dataset_id}"
    start = text.find(marker)
    if start < 0:
        return None
    next_section = text.find("\n### gold.", start + len(marker))
    feature_dictionary = text.find(f"\n{FEATURE_DICTIONARY_MARKER}", start + len(marker))
    boundaries = [value for value in (next_section, feature_dictionary) if value >= 0]
    end = min(boundaries) if boundaries else len(text)
    return text[start:end]


def _inventory_policy_errors(
    *,
    contract_ids: set[str],
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
) -> list[str]:
    errors: list[str] = []
    rows = build_dataset_inventory(
        bronze_root=bronze_root,
        silver_root=silver_root,
        gold_root=gold_root,
    )
    markdown = inventory_to_markdown(rows)
    for dataset_id in sorted(contract_ids):
        if f"`{dataset_id}`" not in markdown:
            errors.append(f"Generated inventory markdown missing Gold dataset: {dataset_id}")
    for row in rows:
        if row.layer != "gold":
            continue
        expected_origin = (
            "crypto-live-loader"
            if row.dataset.startswith("gold.live.")
            else "crypto-history-loader"
        )
        if row.origin_repository != expected_origin:
            errors.append(
                f"Gold dataset has wrong origin: {row.dataset} -> {row.origin_repository}; "
                f"expected {expected_origin}"
            )
    return errors


def validate_dataset_catalog(
    *,
    datasets_path: Path,
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
) -> list[str]:
    """Return catalog and physical-inventory drift errors without mutating files.

    Args:
        datasets_path: Canonical Gold dataset catalog.
        bronze_root: Bronze Lake root used by physical inventory generation.
        silver_root: Silver Lake root used by physical inventory generation.
        gold_root: Gold Lake root used by physical inventory generation.

    Returns:
        Human-readable errors. An empty list means validation passed.
    """

    errors: list[str] = []
    catalog_ids = documented_gold_dataset_ids(datasets_path)
    contract_ids = contracted_gold_dataset_ids()
    missing = sorted(contract_ids - catalog_ids)
    extra = sorted(catalog_ids - contract_ids)
    if missing:
        errors.append(
            "DATASETS.md contract inventory missing contracts: " + ", ".join(missing)
        )
    if extra:
        errors.append(
            "DATASETS.md contract inventory contains unknown contracts: "
            + ", ".join(extra)
        )

    text = datasets_path.read_text(encoding="utf-8")
    if FEATURE_DICTIONARY_MARKER not in text:
        errors.append(f"DATASETS.md missing section: {FEATURE_DICTIONARY_MARKER}")
    if "| Feature | Description |" not in text:
        errors.append("DATASETS.md missing feature-description tables")

    for dataset_id in sorted(contract_ids):
        section = _dataset_section(text, dataset_id)
        if section is None:
            errors.append(f"DATASETS.md missing dataset detail section: {dataset_id}")
            continue
        if "Feature groups:" not in section and "feature groups:" not in section:
            errors.append(
                f"DATASETS.md dataset section missing feature membership: {dataset_id}"
            )
        if "**Keys**" not in section:
            errors.append(f"DATASETS.md dataset section missing Keys group: {dataset_id}")

    feature_dictionary = text[text.index(FEATURE_DICTIONARY_MARKER) :]
    for key in ("timestamp_m1", "exchange", "symbol"):
        if f"`{key}`" not in feature_dictionary:
            errors.append(f"DATASETS.md feature dictionary missing key feature: {key}")

    errors.extend(
        _inventory_policy_errors(
            contract_ids=contract_ids,
            bronze_root=bronze_root,
            silver_root=silver_root,
            gold_root=gold_root,
        )
    )
    return errors


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


def validate_readme_inventory(
    *,
    readme_path: Path,
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
) -> list[str]:
    """Validate DATASETS.md or preserve the former README validator API.

    Args:
        readme_path: DATASETS.md or a legacy README fixture.
        bronze_root: Bronze Lake root.
        silver_root: Silver Lake root.
        gold_root: Gold Lake root.

    Returns:
        Human-readable validation errors.
    """

    text = readme_path.read_text(encoding="utf-8")
    if CONTRACT_INVENTORY_MARKER in text and CONTRACT_DETAIL_MARKER in text:
        return validate_dataset_catalog(
            datasets_path=readme_path,
            bronze_root=bronze_root,
            silver_root=silver_root,
            gold_root=gold_root,
        )

    errors: list[str] = []
    readme_ids = readme_gold_dataset_ids(readme_path)
    contract_ids = contracted_gold_dataset_ids()
    missing = sorted(contract_ids - readme_ids)
    extra = sorted(readme_ids - contract_ids)
    if missing:
        errors.append(
            "README Available Gold dataset IDs missing contracts: " + ", ".join(missing)
        )
    if extra:
        errors.append(
            "README Available Gold dataset IDs contain unknown contracts: "
            + ", ".join(extra)
        )
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
    )
    parser.add_argument("--bronze-root", type=Path, default=Path("lake/bronze"))
    parser.add_argument("--silver-root", type=Path, default=Path("lake/silver"))
    parser.add_argument("--gold-root", type=Path, default=Path("lake/gold"))
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the Gold dataset catalog drift validator."""

    args = _parser().parse_args(argv)
    errors = validate_dataset_catalog(
        datasets_path=args.readme,
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        gold_root=args.gold_root,
    )
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
