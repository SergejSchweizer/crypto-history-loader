"""Read-only historical Bronze completeness auditing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any, Literal, cast

from application.data_quality_report import (
    AuditStatus,
    CompletenessInterval,
    EvidenceSource,
    GapCategory,
    HistoricalBronzeLineage,
    HistoricalCompletenessReport,
)
from application.datasets import DATASET_REGISTRY, CliDataType
from application.services.bronze_runtime_service import (
    parse_exchange_symbol_start_dates,
    parse_start_date_to_open_ms,
    parse_symbol_start_dates,
    symbol_start_open_ms_bound,
)
from ingestion.lake_layout import partition_data_files, partition_empty_minute_files
from ingestion.spot_ohlcv import Exchange, normalize_storage_symbol

AuditFamily = Literal["ohlcv", "funding", "open_interest", "trade"]


@dataclass(frozen=True)
class ExpectedProviderGap:
    """Represent an explicitly configured provider-absence interval.

    Args:
        start: Inclusive UTC start.
        end: Inclusive UTC end.

    Raises:
        ValueError: If bounds are naive, non-UTC, or reversed.
    """

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        """Validate explicit provider-gap bounds."""

        _validate_bounds(self.start, self.end)

    def contains(self, value: datetime) -> bool:
        """Return whether a timestamp has explicit provider-gap evidence.

        Args:
            value: UTC timestamp to classify.

        Returns:
            True when ``value`` is inside the inclusive configured interval.
        """

        return self.start <= value <= self.end


@dataclass(frozen=True)
class LineageAuditSpec:
    """Define one configured lineage and its provider semantics.

    Args:
        lineage: Bronze partition identity.
        family: Dataset-family audit policy.
        start: Inclusive configured listing bound.
        end: Inclusive deterministic audit bound.
        cadence: Expected provider cadence for regular series; None for event-driven history.
        expected_provider_gaps: Explicit provider-absence evidence from configuration.

    Raises:
        ValueError: If bounds or cadence conflict with the family contract.
    """

    lineage: HistoricalBronzeLineage
    family: AuditFamily
    start: datetime
    end: datetime
    cadence: timedelta | None
    expected_provider_gaps: tuple[ExpectedProviderGap, ...] = ()

    def __post_init__(self) -> None:
        """Validate bounds and family-specific cadence semantics."""

        _validate_bounds(self.start, self.end)
        if self.family == "open_interest":
            if self.cadence is not None:
                raise ValueError("open-interest settlement history must use event-driven cadence")
        elif self.cadence is None or self.cadence <= timedelta(0):
            raise ValueError(f"{self.family} requires a positive provider cadence")


def lineage_audit_specs_from_config(*, config: dict[str, object], end: datetime) -> tuple[LineageAuditSpec, ...]:
    """Build bounded audit specs for every configured historical Bronze lineage.

    Args:
        config: Validated repository runtime configuration.
        end: Inclusive deterministic UTC audit bound supplied by the operator.

    Returns:
        Sorted family-specific lineage specifications.

    Raises:
        ValueError: If Bronze datasets, exchanges, symbols, or listing starts are missing.

    Side Effects:
        None. Configuration is read without mutation.
    """

    bronze = _bronze_config(config)
    datasets = _string_list(bronze.get("dataset"), field="bronze-build.dataset")
    symbols = _string_list(bronze.get("symbols"), field="bronze-build.symbols")
    exchanges_raw = bronze.get("exchanges")
    exchanges = (
        _string_list(exchanges_raw, field="bronze-build.exchanges")
        if exchanges_raw is not None
        else [str(bronze.get("exchange", "deribit"))]
    )
    global_start_ms = parse_start_date_to_open_ms(_optional_string(bronze.get("start_date")))
    symbol_starts = parse_symbol_start_dates(_optional_string_list(bronze.get("symbol_start_dates")))
    exchange_symbol_starts = parse_exchange_symbol_start_dates(
        _optional_string_list(bronze.get("exchange_symbol_start_dates"))
    )
    configured_gaps = _configured_provider_gaps(config)

    specs: list[LineageAuditSpec] = []
    for dataset_name in datasets:
        if dataset_name not in DATASET_REGISTRY:
            raise ValueError(f"unsupported historical Bronze dataset '{dataset_name}'")
        dataset_type = cast(CliDataType, dataset_name)
        dataset = DATASET_REGISTRY[dataset_type]
        family, timeframe, cadence = _family_policy(dataset_name)
        for exchange_name in exchanges:
            if exchange_name != "deribit":
                raise ValueError(f"unsupported historical Bronze exchange '{exchange_name}'")
            exchange = cast(Exchange, exchange_name)
            for base_symbol in symbols:
                start_ms = symbol_start_open_ms_bound(
                    exchange=exchange,
                    symbol=base_symbol,
                    global_start_open_ms=global_start_ms,
                    symbol_start_open_ms=symbol_starts,
                    exchange_symbol_start_open_ms=exchange_symbol_starts,
                )
                if start_ms is None:
                    raise ValueError(f"missing configured listing start for {exchange}:{base_symbol}")
                start = datetime.fromtimestamp(start_ms / 1000, tz=UTC)
                storage_symbol = _storage_symbol(
                    exchange=exchange,
                    symbol=base_symbol,
                    market=str(dataset.market),
                )
                lineage = HistoricalBronzeLineage(
                    dataset_type=dataset.dataset_type,
                    exchange=exchange,
                    instrument_type=dataset.instrument_type,
                    symbol=storage_symbol,
                    timeframe=timeframe,
                )
                specs.append(
                    LineageAuditSpec(
                        lineage=lineage,
                        family=family,
                        start=start,
                        end=end,
                        cadence=cadence,
                        expected_provider_gaps=configured_gaps.get(lineage.identifier, ()),
                    )
                )
    return tuple(sorted(specs, key=lambda item: item.lineage))


def audit_historical_completeness(
    *,
    bronze_root: Path,
    specs: tuple[LineageAuditSpec, ...],
) -> HistoricalCompletenessReport:
    """Audit configured historical Bronze lineages without writing any files.

    The scanner reads only the source timestamp column and, for trade datasets,
    the explicit confirmed-empty minute sidecar. Missing ordinary manifests are
    never treated as evidence of successful acquisition.

    Args:
        bronze_root: Existing Bronze lake root.
        specs: Fully bounded lineage contracts sorted by the report schema.

    Returns:
        A deterministic sanitized interval report. Any unverified interval makes
        the aggregate result ``RECONCILE_REQUIRED``.

    Raises:
        RuntimeError: If PyArrow is unavailable.

    Side Effects:
        None. The service opens existing parquet files in read-only mode.
    """

    intervals: list[CompletenessInterval] = []
    for spec in sorted(specs, key=lambda item: item.lineage):
        partition_root = _partition_root(bronze_root, spec.lineage)
        observed = _read_datetime_values(partition_data_files(partition_root), "open_time")
        if spec.family == "trade":
            observed = {_minute_start(value) for value in observed}
        if spec.family == "open_interest":
            intervals.extend(_audit_event_driven(spec=spec, observed=observed))
            continue
        confirmed_empty = (
            _read_confirmed_empty_minutes(partition_empty_minute_files(partition_root))
            if spec.family == "trade"
            else set()
        )
        intervals.extend(
            _audit_regular_grid(
                spec=spec,
                observed=observed,
                confirmed_empty=confirmed_empty,
            )
        )
    return HistoricalCompletenessReport(intervals=tuple(intervals))


def parse_utc_bound(value: str) -> datetime:
    """Parse an ISO-8601 audit bound and require UTC.

    Args:
        value: ISO-8601 timestamp, with ``Z`` accepted for UTC.

    Returns:
        A timezone-aware UTC datetime.

    Raises:
        ValueError: If the value is invalid or does not use UTC.
    """

    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid UTC audit bound '{value}'") from exc
    _validate_bounds(parsed, parsed)
    return parsed.astimezone(UTC)


def _audit_regular_grid(
    *,
    spec: LineageAuditSpec,
    observed: set[datetime],
    confirmed_empty: set[datetime],
) -> list[CompletenessInterval]:
    cadence = spec.cadence
    if cadence is None:
        raise ValueError("regular-grid audit requires cadence")

    intervals: list[CompletenessInterval] = []
    cursor = spec.start
    active_start = cursor
    active_classification: tuple[GapCategory, EvidenceSource, AuditStatus] | None = None
    previous = cursor
    while cursor <= spec.end:
        classification = _classify_timestamp(
            value=cursor,
            observed=observed,
            confirmed_empty=confirmed_empty,
            expected_provider_gaps=spec.expected_provider_gaps,
        )
        if active_classification is None:
            active_classification = classification
            active_start = cursor
        elif classification != active_classification:
            intervals.append(_interval(spec.lineage, active_start, previous, active_classification))
            active_start = cursor
            active_classification = classification
        previous = cursor
        cursor += cadence

    if active_classification is not None:
        intervals.append(_interval(spec.lineage, active_start, previous, active_classification))
    return intervals


def _audit_event_driven(*, spec: LineageAuditSpec, observed: set[datetime]) -> list[CompletenessInterval]:
    if any(gap.start <= spec.start and gap.end >= spec.end for gap in spec.expected_provider_gaps):
        classification: tuple[GapCategory, EvidenceSource, AuditStatus] = (
            "expected_provider_gap",
            "config:expected_provider_gap",
            "PASS",
        )
    elif spec.start == spec.end and spec.start in observed:
        classification = ("observed_rows", "bronze_data:open_time", "PASS")
    else:
        # Deribit's open-interest history is settlement-event driven. Observed
        # events do not prove that pagination acquired the complete bounded range.
        classification = (
            "unverified_acquisition",
            "missing:acquisition_evidence",
            "RECONCILE_REQUIRED",
        )
    return [_interval(spec.lineage, spec.start, spec.end, classification)]


def _classify_timestamp(
    *,
    value: datetime,
    observed: set[datetime],
    confirmed_empty: set[datetime],
    expected_provider_gaps: tuple[ExpectedProviderGap, ...],
) -> tuple[GapCategory, EvidenceSource, AuditStatus]:
    if value in observed:
        return ("observed_rows", "bronze_data:open_time", "PASS")
    if value in confirmed_empty:
        return ("confirmed_empty", "bronze_sidecar:confirmed_empty", "PASS")
    if any(gap.contains(value) for gap in expected_provider_gaps):
        return ("expected_provider_gap", "config:expected_provider_gap", "PASS")
    return (
        "unverified_acquisition",
        "missing:acquisition_evidence",
        "RECONCILE_REQUIRED",
    )


def _interval(
    lineage: HistoricalBronzeLineage,
    start: datetime,
    end: datetime,
    classification: tuple[GapCategory, EvidenceSource, AuditStatus],
) -> CompletenessInterval:
    category, evidence, status = classification
    return CompletenessInterval(
        lineage=lineage,
        start=_iso_utc(start),
        end=_iso_utc(end),
        category=category,
        evidence=evidence,
        status=status,
    )


def _partition_root(bronze_root: Path, lineage: HistoricalBronzeLineage) -> Path:
    return (
        bronze_root
        / f"dataset_type={lineage.dataset_type}"
        / f"exchange={lineage.exchange}"
        / f"instrument_type={lineage.instrument_type}"
        / f"symbol={lineage.symbol}"
        / f"timeframe={lineage.timeframe}"
    )


def _bronze_config(config: dict[str, object]) -> dict[str, object]:
    for key in ("bronze-build", "bronze-ingest", "loader"):
        section = config.get(key)
        if isinstance(section, dict):
            return cast(dict[str, object], section)
    raise ValueError("config missing historical Bronze build section")


def _family_policy(dataset_type: str) -> tuple[AuditFamily, str, timedelta | None]:
    if dataset_type == "funding":
        return ("funding", "8h", timedelta(hours=8))
    if dataset_type == "open_interest":
        return ("open_interest", "1m", None)
    if dataset_type in {"perps_trades", "options_trades"}:
        return ("trade", "tick", timedelta(minutes=1))
    return ("ohlcv", "1m", timedelta(minutes=1))


def _storage_symbol(*, exchange: Exchange, symbol: str, market: str) -> str:
    if market == "option":
        return symbol.upper().strip()
    if market in {"spot_ohlcv", "perp"}:
        return normalize_storage_symbol(exchange=exchange, symbol=symbol, market=cast(Any, market))
    return symbol.upper().strip()


def _configured_provider_gaps(
    config: dict[str, object],
) -> dict[str, tuple[ExpectedProviderGap, ...]]:
    section = config.get("historical-completeness-audit")
    if not isinstance(section, dict):
        return {}
    raw_gaps = cast(dict[str, object], section).get("expected_provider_gaps", [])
    if not isinstance(raw_gaps, list):
        raise ValueError("historical-completeness-audit.expected_provider_gaps must be a list")
    grouped: dict[str, list[ExpectedProviderGap]] = {}
    for raw in raw_gaps:
        if not isinstance(raw, dict):
            raise ValueError("expected provider gap entries must be mappings")
        entry = cast(dict[str, object], raw)
        lineage = _required_string(entry.get("lineage"), field="expected provider gap lineage")
        gap = ExpectedProviderGap(
            start=parse_utc_bound(_required_string(entry.get("start"), field="expected provider gap start")),
            end=parse_utc_bound(_required_string(entry.get("end"), field="expected provider gap end")),
        )
        grouped.setdefault(lineage, []).append(gap)
    return {lineage: tuple(sorted(gaps, key=lambda gap: (gap.start, gap.end))) for lineage, gaps in grouped.items()}


def _string_list(value: object, *, field: str) -> list[str]:
    values = _optional_string_list(value)
    if not values:
        raise ValueError(f"{field} must be a non-empty list")
    return values


def _optional_string_list(value: object) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError("configured list values must contain only strings")
    return [cast(str, item) for item in value]


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("configured text value must be a string")
    return value


def _required_string(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be non-empty text")
    return value.strip()


def _read_datetime_values(files: list[Path], column: str) -> set[datetime]:
    pq = _require_pyarrow_parquet()
    values: set[datetime] = set()
    for path in files:
        parquet_file = pq.ParquetFile(path)
        if column not in parquet_file.schema_arrow.names:
            continue
        for batch in parquet_file.iter_batches(columns=[column], batch_size=10_000):
            for raw in batch.column(0).to_pylist():
                if isinstance(raw, datetime):
                    values.add(_as_utc(raw))
    return values


def _read_confirmed_empty_minutes(files: list[Path]) -> set[datetime]:
    pq = _require_pyarrow_parquet()
    values: set[datetime] = set()
    for path in files:
        parquet_file = pq.ParquetFile(path)
        if not {"minute", "status"}.issubset(parquet_file.schema_arrow.names):
            continue
        for batch in parquet_file.iter_batches(columns=["minute", "status"], batch_size=10_000):
            for minute, status in zip(batch.column(0).to_pylist(), batch.column(1).to_pylist(), strict=True):
                if isinstance(minute, datetime) and status == "confirmed_empty":
                    values.add(_as_utc(minute))
    return values


def _require_pyarrow_parquet() -> Any:
    try:
        return import_module("pyarrow.parquet")
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for historical completeness audit") from exc


def _validate_bounds(start: datetime, end: datetime) -> None:
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("audit bounds must be timezone-aware UTC timestamps")
    if start.utcoffset() != timedelta(0) or end.utcoffset() != timedelta(0):
        raise ValueError("audit bounds must use UTC")
    if end < start:
        raise ValueError("audit end must not precede start")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _minute_start(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
