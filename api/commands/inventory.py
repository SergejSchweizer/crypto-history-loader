"""Dataset inventory command."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from application.services.dataset_inventory import (
    build_dataset_inventory,
    inventory_to_json,
    inventory_to_markdown,
)


def add_dataset_inventory_parser(subparsers: Any) -> None:
    """Register ``dataset-inventory`` parser."""

    parser = subparsers.add_parser(
        "dataset-inventory",
        help="Build a read-only Bronze/Silver/Gold inventory report",
    )
    parser.add_argument("--bronze-root", default="lake/bronze", help="Bronze lake root")
    parser.add_argument("--silver-root", default="lake/silver", help="Silver lake root")
    parser.add_argument("--gold-root", default="lake/gold", help="Gold lake root")
    parser.add_argument("--output", help="Optional report output path")
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown", help="Report format")
    parser.add_argument("--no-json-output", action="store_true", help="Suppress stdout output")


def run_dataset_inventory(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run read-only dataset inventory reporting."""

    rows = build_dataset_inventory(
        bronze_root=Path(str(args.bronze_root)),
        silver_root=Path(str(args.silver_root)),
        gold_root=Path(str(args.gold_root)),
    )
    if str(args.format) == "json":
        rendered = inventory_to_json(rows)
    else:
        rendered = inventory_to_markdown(rows)

    output = getattr(args, "output", None)
    if output:
        output_path = Path(str(output))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
        logger.info("Dataset inventory written path=%s rows=%s", output_path, len(rows))
    if not bool(getattr(args, "no_json_output", False)):
        if str(args.format) == "json":
            print(json.dumps([row.to_dict() for row in rows], indent=2, sort_keys=True))
        else:
            print(rendered, end="")
