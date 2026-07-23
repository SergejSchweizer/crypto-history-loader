"""Validate the Gold dataset catalog against the typed contract inventory."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from application.dataset_contracts import supported_gold_dataset_ids
from application.services.dataset_inventory import build_dataset_inventory, inventory_to_markdown

GOLD_DATASET_RE = re.compile(r"`(gold\.[a-z0-9_.]+)`")


def readme_gold_dataset_ids(readme_path: Path) -> set[str]:
    """Return Gold dataset IDs listed in the authoritative dataset catalog.

    The legacy function name is retained to avoid breaking callers and tests while
    documentation ownership moves from README.md to DATASETS.md.
    """

    text = readme_path.read_text(encoding="utf-8")
    marker = "## Available Gold dataset IDs"
    try:
        section_start = text.index(marker)
    except ValueError as exc:
        raise ValueError(f"{readme_path} missing section: {marker}") from exc
    section = text[section_start:]
    next_heading = re.search(r"\n##\s+", section[len(marker) :])
    if next_heading is not None:
        section = section[: len(marker) + next_heading.start()]
    return set(GOLD_DATASET_RE.findall(section))


def contracted_gold_dataset_ids() -> set[str]:
    """Return Gold dataset IDs declared by the typed contract registry."""

    return set(supported_gold_dataset_ids())


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

    rows = build_dataset_inventory(bronze_root=bronze_root, silver_root=silver_root, gold_root=gold_root)
    markdown = inventory_to_markdown(rows)
    for dataset_id in sorted(contract_ids):
        if f"`{dataset_id}`" not in markdown:
            errors.append(f"Generated inventory markdown missing Gold dataset: {dataset_id}")
    for row in rows:
        if (
            row.layer == "gold"
            and row.dataset.startswith("gold.live.")
            and row.origin_repository != "crypto-live-loader"
        ):
            errors.append(f"Live Gold dataset has wrong origin: {row.dataset} -> {row.origin_repository}")
        if (
            row.layer == "gold"
            and not row.dataset.startswith("gold.live.")
            and row.origin_repository != "crypto-history-loader"
        ):
            errors.append(f"Historical Gold dataset has wrong origin: {row.dataset} -> {row.origin_repository}")
    return errors


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--readme",
        type=Path,
        default=Path("DATASETS.md"),
        help="Path to the authoritative Gold dataset catalog (legacy option name).",
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
