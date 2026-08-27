"""Deterministic schemas for sanitized historical completeness reports."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal

GapCategory = Literal[
    "observed_rows",
    "confirmed_empty",
    "expected_provider_gap",
    "unverified_acquisition",
]
AuditStatus = Literal["PASS", "RECONCILE_REQUIRED"]
EvidenceSource = Literal[
    "bronze_data:open_time",
    "bronze_sidecar:confirmed_empty",
    "config:expected_provider_gap",
    "missing:acquisition_evidence",
]


@dataclass(frozen=True, order=True)
class HistoricalBronzeLineage:
    """Identify one configured historical Bronze series.

    Args:
        dataset_type: Canonical Bronze dataset family.
        exchange: Provider partition name.
        instrument_type: Instrument partition name.
        symbol: Sanitized storage symbol.
        timeframe: Stored source cadence or ``tick`` for trades.
    """

    dataset_type: str
    exchange: str
    instrument_type: str
    symbol: str
    timeframe: str

    @property
    def identifier(self) -> str:
        """Return a stable lineage identifier.

        Returns:
            Pipe-delimited partition values suitable for deterministic reports.
        """

        return "|".join((self.dataset_type, self.exchange, self.instrument_type, self.symbol, self.timeframe))


@dataclass(frozen=True, order=True)
class CompletenessInterval:
    """Describe one contiguous interval with uniform completeness evidence.

    Args:
        lineage: Historical Bronze series being audited.
        start: Inclusive UTC interval bound in canonical ISO-8601 form.
        end: Inclusive UTC interval bound in canonical ISO-8601 form.
        category: Evidence-backed completeness classification.
        evidence: Sanitized evidence source; paths and payloads are excluded.
        status: ``PASS`` only for verified observations, empties, or expected gaps.

    Raises:
        ValueError: If an unverified interval is marked ``PASS``.
    """

    lineage: HistoricalBronzeLineage
    start: str
    end: str
    category: GapCategory
    evidence: EvidenceSource
    status: AuditStatus

    def __post_init__(self) -> None:
        """Enforce the fail-closed interval status invariant."""

        if self.category == "unverified_acquisition" and self.status != "RECONCILE_REQUIRED":
            raise ValueError("unverified acquisition intervals cannot PASS")

    def to_dict(self) -> dict[str, str]:
        """Return a sanitized JSON-compatible interval.

        Returns:
            Deterministically ordered report fields without provider payloads or paths.
        """

        return {
            "lineage": self.lineage.identifier,
            "start": self.start,
            "end": self.end,
            "gap_category": self.category,
            "evidence_source": self.evidence,
            "status": self.status,
        }


@dataclass(frozen=True)
class HistoricalCompletenessReport:
    """Contain the deterministic result of one read-only Bronze audit.

    Args:
        intervals: Sorted, non-overlapping interval classifications.
        schema_version: Stable report contract version.
    """

    intervals: tuple[CompletenessInterval, ...]
    schema_version: str = "historical-lake-completeness-v1"

    @property
    def status(self) -> AuditStatus:
        """Return the fail-closed aggregate status.

        Returns:
            ``RECONCILE_REQUIRED`` when any interval is unverified; otherwise ``PASS``.
        """

        if any(interval.status == "RECONCILE_REQUIRED" for interval in self.intervals):
            return "RECONCILE_REQUIRED"
        return "PASS"

    def to_json(self) -> str:
        """Render stable, sanitized JSON.

        Returns:
            Newline-terminated JSON with deterministic key and interval ordering.
        """

        payload = {
            "schema_version": self.schema_version,
            "status": self.status,
            "intervals": [interval.to_dict() for interval in sorted(self.intervals)],
        }
        return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
