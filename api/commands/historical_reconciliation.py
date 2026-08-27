"""Guarded historical source reconciliation command."""

from __future__ import annotations

import argparse
import importlib
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from application.services.historical_reconciliation import (
    JsonEvidenceSink,
    ReconciliationAdapter,
    RecoveryReport,
    load_historical_report,
    reconcile_historical_intervals,
)

AdapterFactory = Callable[..., ReconciliationAdapter]


def add_historical_reconciliation_parser(subparsers: Any) -> None:
    """Register the guarded ``historical-reconcile`` parser.

    Args:
        subparsers: Top-level argparse subparser collection.
    """

    parser = subparsers.add_parser(
        "historical-reconcile",
        help="Reconcile exact PR-99 non-PASS intervals and certify serving Gold",
    )
    parser.add_argument("--pr99-report", required=True, help="Validated PR-99 report JSON")
    parser.add_argument("--state-file", required=True, help="Sanitized reconciliation state JSON")
    parser.add_argument("--report-file", required=True, help="Sanitized terminal recovery report JSON")
    parser.add_argument(
        "--adapter-factory",
        required=True,
        help="Operator adapter factory as importable.module:factory",
    )


def run_historical_reconciliation(
    *, args: argparse.Namespace, config: dict[str, object], logger: logging.Logger
) -> RecoveryReport:
    """Load the operator adapter and run guarded PR-100 orchestration.

    Args:
        args: Parsed command paths and adapter factory.
        config: Validated repository runtime configuration.
        logger: Repository stream/file logger honoring the global ``--debug`` switch.

    Returns:
        Sanitized terminal report.

    Raises:
        ValueError: If the adapter factory reference is malformed.

    Side Effects:
        Loads an explicit operator adapter, delegates reconciliation, writes evidence, and prints JSON.
    """

    factory = _load_adapter_factory(str(args.adapter_factory))
    adapter = factory(args=args, config=config, logger=logger)
    sink = JsonEvidenceSink(state_path=Path(args.state_file), report_path=Path(args.report_file))
    report = reconcile_historical_intervals(
        input_report=load_historical_report(Path(args.pr99_report)),
        adapter=adapter,
        sink=sink,
    )
    logger.info(
        "Historical reconciliation complete mode=%s targets=%s status=%s downstream_blocked=%s",
        report.mode,
        len(report.target_intervals),
        report.status,
        report.status != "PASS",
    )
    print(report.to_json(), end="")
    return report


def _load_adapter_factory(reference: str) -> AdapterFactory:
    module_name, separator, attribute = reference.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("--adapter-factory must use importable.module:factory")
    module = importlib.import_module(module_name)
    factory = getattr(module, attribute, None)
    if not callable(factory):
        raise ValueError("--adapter-factory does not reference a callable")
    return cast(AdapterFactory, factory)
