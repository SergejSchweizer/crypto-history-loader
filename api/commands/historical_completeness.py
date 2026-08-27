"""Read-only historical Bronze completeness audit command."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

from application.data_quality_report import HistoricalCompletenessReport
from application.services.historical_completeness import (
    audit_historical_completeness,
    lineage_audit_specs_from_config,
    parse_utc_bound,
)


def add_historical_completeness_parser(subparsers: Any) -> None:
    """Register the ``historical-completeness-audit`` parser.

    Args:
        subparsers: Top-level argparse subparser collection.
    """

    parser = subparsers.add_parser(
        "historical-completeness-audit",
        help="Audit configured historical Bronze completeness without writes",
    )
    parser.add_argument("--bronze-root", default="lake/bronze", help="Existing Bronze lake root")
    parser.add_argument(
        "--end-time",
        help="Inclusive deterministic UTC upper bound (or configure historical-completeness-audit.end_time)",
    )


def configure_read_only_logging(*, debug: bool) -> logging.Logger:
    """Create repository-format stream logging without opening a logfile.

    Args:
        debug: Whether to emit debug-level diagnostics.

    Returns:
        A command-scoped stderr logger with the standard message structure.

    Side Effects:
        Adds one in-memory stream handler. No filesystem paths are opened.
    """

    logger = logging.getLogger("crypto_loader.historical-completeness-audit")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)
    return logger


def run_historical_completeness_audit(
    *, args: argparse.Namespace, config: dict[str, object], logger: logging.Logger
) -> HistoricalCompletenessReport:
    """Run the configured read-only audit and print its deterministic report.

    Args:
        args: Parsed command arguments.
        config: Validated runtime configuration containing Bronze lineages.
        logger: Stream-only command logger.

    Returns:
        The typed completeness report also rendered to stdout.

    Raises:
        ValueError: If no deterministic UTC end bound is configured.

    Side Effects:
        Writes sanitized JSON to stdout and informational messages to stderr only.
    """

    end_time = getattr(args, "end_time", None)
    if not isinstance(end_time, str) or not end_time.strip():
        raise ValueError("historical-completeness-audit requires --end-time or configured end_time")
    specs = lineage_audit_specs_from_config(config=config, end=parse_utc_bound(end_time))
    report = audit_historical_completeness(
        bronze_root=Path(str(args.bronze_root)),
        specs=specs,
    )
    logger.info(
        "Historical completeness audit complete lineages=%s intervals=%s status=%s",
        len(specs),
        len(report.intervals),
        report.status,
    )
    print(report.to_json(), end="")
    return report
