"""CLI adapter for guarded PostgreSQL serving-plane reconstruction."""

from __future__ import annotations

import argparse
import importlib
import json
import logging
from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from application.postgres_sync.config import PostgresSyncConfig
from application.postgres_sync.live_conformance import verify_live_postgres
from application.services.postgres_reconstruction import (
    AdapterFactory,
    ReconstructionAdapter,
    run_postgres_reconstruction,
)


def add_postgres_reconstruction_parser(subparsers: Any) -> None:
    """Register the production reconstruction command."""

    parser = subparsers.add_parser("postgres-production-reconstruction", help="Certify or reconstruct PostgreSQL")
    parser.add_argument(
        "--current-report",
        default="artifacts/acceptance/postgres-live-conformance-v2.json",
        help="Sanitized current PR-101 conformance evidence",
    )
    parser.add_argument(
        "--evidence-file",
        default="artifacts/acceptance/postgres-production-reconstruction-v2.json",
        help="Sanitized PR-102 evidence output path",
    )
    parser.add_argument("--gold-root", default="lake/gold", help="Certified current Gold lake root")
    parser.add_argument(
        "--adapter-factory",
        help="Operator adapter factory as module:callable; required only for reconstruction",
    )


def _operator_adapter_factory(path: str) -> AdapterFactory:
    def create_adapter() -> ReconstructionAdapter:
        module_name, separator, attribute_name = path.partition(":")
        if not separator or not module_name or not attribute_name:
            raise ValueError("adapter factory must use module:callable format")
        factory = getattr(importlib.import_module(module_name), attribute_name)
        if not callable(factory):
            raise TypeError("adapter factory must be callable")
        return cast(ReconstructionAdapter, factory())

    return create_adapter


def run_postgres_production_reconstruction(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Run the fail-closed PR-102 coordinator with operator-provided destructive operations."""

    current_report = json.loads(Path(str(args.current_report)).read_text(encoding="utf-8"))
    if not isinstance(current_report, dict):
        raise RuntimeError("postgres-production-reconstruction requires a JSON object report")

    def verify_independently() -> Mapping[str, object]:
        with TemporaryDirectory() as directory:
            report = verify_live_postgres(
                gold_root=Path(str(args.gold_root)),
                config=PostgresSyncConfig.from_env(),
                report_path=Path(directory) / "postgres-live-conformance-v2.json",
            )
            return report.payload()

    factory_path = getattr(args, "adapter_factory", None)
    factory = _operator_adapter_factory(str(factory_path)) if factory_path else None
    mode = run_postgres_reconstruction(
        current_report=cast(dict[str, object], current_report),
        evidence_path=Path(str(args.evidence_file)),
        verify_independently=verify_independently,
        adapter_factory=factory,
    )
    logger.info("PostgreSQL production reconstruction status=PASS mode=%s", mode)
    return 0
