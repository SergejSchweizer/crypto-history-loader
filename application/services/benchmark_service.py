"""Read-only benchmark reporting for Medallion lake artifacts."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Literal

from application.services.runtime_service import POLARS_MAX_THREADS

BenchmarkEventType = Literal["planned", "skipped", "built", "published", "failed"]
BenchmarkStage = Literal["bronze", "silver", "gold"]


@dataclass(frozen=True)
class BenchmarkTelemetryEvent:
    """One read-only benchmark measurement for a Medallion artifact."""

    event_type: BenchmarkEventType
    stage: BenchmarkStage
    dataset: str
    symbol: str | None
    partition: str | None
    rows_in: int
    rows_out: int
    bytes_read: int
    bytes_written: int
    elapsed_seconds: float
    worker_count: int
    polars_thread_count: int
    timestamp: str

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible telemetry payload."""

        return asdict(self)


def _parquet_rows(path: Path) -> int:
    """Read a Parquet row count without materializing its column data."""

    try:
        from pyarrow.parquet import ParquetFile
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for benchmark-build. Install project dependencies.") from exc
    return int(ParquetFile(path).metadata.num_rows)


def _identity_from_path(root: Path, artifact: Path) -> tuple[str, str | None, str | None]:
    """Extract stable dataset, symbol, and partition labels from a lake-relative path."""

    parts = artifact.relative_to(root).parts
    dataset = parts[0] if parts else artifact.stem
    symbol = next((part.split("=", 1)[1] for part in parts if part.startswith("symbol=")), None)
    partition = next(
        (part for part in parts if part.startswith(("year=", "month=", "date=", "partition="))),
        artifact.stem,
    )
    return dataset, symbol, partition


def benchmark_stage(
    *,
    stage: BenchmarkStage,
    root: Path,
    worker_count: int = 1,
    polars_thread_count: int = POLARS_MAX_THREADS,
) -> list[BenchmarkTelemetryEvent]:
    """Measure every Parquet artifact below a stage root without modifying it."""

    if worker_count < 1 or worker_count > POLARS_MAX_THREADS:
        raise ValueError(f"worker_count must be between 1 and {POLARS_MAX_THREADS}")
    if polars_thread_count < 1 or polars_thread_count > POLARS_MAX_THREADS:
        raise ValueError(f"polars_thread_count must be between 1 and {POLARS_MAX_THREADS}")

    events: list[BenchmarkTelemetryEvent] = []
    artifacts = sorted(root.rglob("*.parquet")) if root.exists() else []
    timestamp = datetime.now(UTC).isoformat()
    if not artifacts:
        events.append(
            BenchmarkTelemetryEvent(
                event_type="skipped",
                stage=stage,
                dataset="none",
                symbol=None,
                partition=None,
                rows_in=0,
                rows_out=0,
                bytes_read=0,
                bytes_written=0,
                elapsed_seconds=0.0,
                worker_count=worker_count,
                polars_thread_count=polars_thread_count,
                timestamp=timestamp,
            )
        )
        return events

    for artifact in artifacts:
        dataset, symbol, partition = _identity_from_path(root, artifact)
        started = perf_counter()
        try:
            byte_count = artifact.stat().st_size
            row_count = _parquet_rows(artifact)
        except Exception:
            events.append(
                BenchmarkTelemetryEvent(
                    event_type="failed",
                    stage=stage,
                    dataset=dataset,
                    symbol=symbol,
                    partition=partition,
                    rows_in=0,
                    rows_out=0,
                    bytes_read=0,
                    bytes_written=0,
                    elapsed_seconds=perf_counter() - started,
                    worker_count=worker_count,
                    polars_thread_count=polars_thread_count,
                    timestamp=timestamp,
                )
            )
            continue
        events.extend(
            (
                BenchmarkTelemetryEvent(
                    event_type="planned",
                    stage=stage,
                    dataset=dataset,
                    symbol=symbol,
                    partition=partition,
                    rows_in=row_count,
                    rows_out=0,
                    bytes_read=byte_count,
                    bytes_written=0,
                    elapsed_seconds=0.0,
                    worker_count=worker_count,
                    polars_thread_count=polars_thread_count,
                    timestamp=timestamp,
                ),
                BenchmarkTelemetryEvent(
                    event_type="built",
                    stage=stage,
                    dataset=dataset,
                    symbol=symbol,
                    partition=partition,
                    rows_in=row_count,
                    rows_out=row_count,
                    bytes_read=byte_count,
                    bytes_written=0,
                    elapsed_seconds=perf_counter() - started,
                    worker_count=worker_count,
                    polars_thread_count=polars_thread_count,
                    timestamp=timestamp,
                ),
                BenchmarkTelemetryEvent(
                    event_type="published",
                    stage=stage,
                    dataset=dataset,
                    symbol=symbol,
                    partition=partition,
                    rows_in=row_count,
                    rows_out=row_count,
                    bytes_read=byte_count,
                    bytes_written=0,
                    elapsed_seconds=perf_counter() - started,
                    worker_count=worker_count,
                    polars_thread_count=polars_thread_count,
                    timestamp=timestamp,
                ),
            )
        )
    return events


def write_benchmark_report(*, events: list[BenchmarkTelemetryEvent], output: Path) -> None:
    """Write the benchmark report outside the measured lake roots."""

    resolved_output = output.resolve()
    payload = {
        "schema_version": 1,
        "event_count": len(events),
        "events": [event.to_dict() for event in events],
    }
    resolved_output.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = resolved_output.with_suffix(f"{resolved_output.suffix}.tmp-{os.getpid()}")
    temporary_output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary_output.replace(resolved_output)
