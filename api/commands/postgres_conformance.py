"""CLI adapter for the read-only PostgreSQL live conformance verifier."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from application.postgres_sync.config import PostgresSyncConfig
from application.postgres_sync.live_conformance import verify_live_postgres


def add_postgres_live_conformance_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("postgres-live-conformance", help="Read-only PostgreSQL serving-plane audit")
    parser.add_argument("--gold-root", default="lake/gold", help="Current Gold lake root")
    parser.add_argument(
        "--report-file",
        default="artifacts/acceptance/postgres-live-conformance-v2.json",
        help="Sanitized evidence JSON output path",
    )


def run_postgres_live_conformance(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        report = verify_live_postgres(
            gold_root=Path(str(args.gold_root)),
            config=PostgresSyncConfig.from_env(),
            report_path=Path(str(args.report_file)),
        )
    except (OSError, ValueError, TypeError) as exc:
        raise RuntimeError("postgres-live-conformance could not initialize") from exc
    logger.info("PostgreSQL live conformance status=%s lineages=%s", report.status, report.lineage_count)
    print(json.dumps(report.payload(), sort_keys=True))
    return 0 if report.status == "PASS" else 1
