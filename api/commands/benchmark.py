"""Read-only Medallion benchmark command."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from application.services.benchmark_service import BenchmarkTelemetryEvent, benchmark_stage, write_benchmark_report
from application.services.runtime_service import POLARS_MAX_THREADS


def add_benchmark_build_parser(subparsers: Any) -> None:
    """Register the ``benchmark-build`` parser."""

    parser = subparsers.add_parser("benchmark-build", help="Read-only benchmark of Bronze, Silver, and Gold artifacts")
    parser.add_argument("--bronze-root", help="Bronze artifact root to measure")
    parser.add_argument("--silver-root", help="Silver artifact root to measure")
    parser.add_argument("--gold-root", help="Gold artifact root to measure")
    parser.add_argument("--fixture-only", action="store_true", help="Measure an isolated deterministic fixture")
    parser.add_argument("--output-report", required=True, help="JSON report path outside measured lake roots")
    parser.add_argument("--maxprocesses", type=int, default=1, help="Maximum benchmark workers (1-4)")
    parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")


def _write_fixture(root: Path) -> dict[str, Path]:
    """Create a deterministic three-stage Parquet fixture outside production lake paths."""

    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for benchmark-build. Install project dependencies.") from exc

    roots: dict[str, Path] = {}
    for stage, values in (("bronze", [1, 2]), ("silver", [10, 20]), ("gold", [100, 200])):
        stage_root = root / stage
        artifact = stage_root / "spot_ohlcv" / "symbol=BTC" / "month=2026-01.parquet"
        artifact.parent.mkdir(parents=True)
        pl.DataFrame({"timestamp": [1, 2], "value": values}).write_parquet(artifact)
        roots[stage] = stage_root
    return roots


def _root_arguments(args: argparse.Namespace) -> dict[str, Path]:
    """Resolve explicit stage roots and reject accidental production-lake output paths."""

    roots = {
        "bronze": cast(str | None, args.bronze_root),
        "silver": cast(str | None, args.silver_root),
        "gold": cast(str | None, args.gold_root),
    }
    resolved = {stage: Path(root).resolve() for stage, root in roots.items() if root}
    if not resolved:
        raise ValueError("Provide one or more stage roots or use --fixture-only")
    return resolved


def _validate_output_path(*, output: Path, roots: dict[str, Path]) -> None:
    """Ensure report publication cannot create an artifact within a measured lake root."""

    resolved_output = output.resolve()
    for root in roots.values():
        if resolved_output.is_relative_to(root):
            raise ValueError("--output-report must be outside every measured stage root")


def _benchmark_roots(*, roots: dict[str, Path], worker_count: int) -> list[BenchmarkTelemetryEvent]:
    """Run every configured stage in stable Medallion order."""

    events: list[BenchmarkTelemetryEvent] = []
    for stage in ("bronze", "silver", "gold"):
        root = roots.get(stage)
        if root is not None:
            events.extend(benchmark_stage(stage=stage, root=root, worker_count=worker_count))
    return events


def run_benchmark_build(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Measure configured or temporary fixture artifacts without changing their contents."""

    worker_count = int(args.maxprocesses)
    if worker_count < 1 or worker_count > POLARS_MAX_THREADS:
        raise ValueError(f"Invalid --maxprocesses '{worker_count}'. Value must be between 1 and {POLARS_MAX_THREADS}")
    output = Path(cast(str, args.output_report))

    if bool(args.fixture_only):
        with TemporaryDirectory(prefix="crypto-loader-benchmark-") as temporary_directory:
            roots = _write_fixture(Path(temporary_directory))
            _validate_output_path(output=output, roots=roots)
            events = _benchmark_roots(roots=roots, worker_count=worker_count)
    else:
        roots = _root_arguments(args)
        _validate_output_path(output=output, roots=roots)
        events = _benchmark_roots(roots=roots, worker_count=worker_count)

    write_benchmark_report(events=events, output=output)
    for event in events:
        logger.info("benchmark_telemetry=%s", json.dumps(event.to_dict(), sort_keys=True))
    if not bool(args.no_json_output):
        print(json.dumps({"report": str(output.resolve()), "events": [event.to_dict() for event in events]}, indent=2))
    logger.info("Command complete: benchmark-build events=%s report=%s", len(events), output.resolve())
