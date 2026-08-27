"""Tests for read-only historical Bronze completeness auditing."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from application.data_quality_report import CompletenessInterval, HistoricalBronzeLineage
from application.services.historical_completeness import (
    ExpectedProviderGap,
    LineageAuditSpec,
    audit_historical_completeness,
    lineage_audit_specs_from_config,
)


def _lineage(dataset_type: str, instrument_type: str, timeframe: str) -> HistoricalBronzeLineage:
    return HistoricalBronzeLineage(
        dataset_type=dataset_type,
        exchange="deribit",
        instrument_type=instrument_type,
        symbol="BTC-PERPETUAL",
        timeframe=timeframe,
    )


def _write_rows(root: Path, lineage: HistoricalBronzeLineage, name: str, rows: list[dict[str, object]]) -> None:
    path = (
        root
        / f"dataset_type={lineage.dataset_type}"
        / f"exchange={lineage.exchange}"
        / f"instrument_type={lineage.instrument_type}"
        / f"symbol={lineage.symbol}"
        / f"timeframe={lineage.timeframe}"
        / "year=2026"
        / "month=2026-01"
        / "date=2026-01-01"
        / name
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def _lake_bytes(root: Path) -> dict[str, bytes]:
    return {str(path.relative_to(root)): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


def test_trade_audit_distinguishes_all_evidence_categories_without_writes(tmp_path: Path) -> None:
    """Observed ticks, confirmed empties, provider gaps, and unknown minutes remain distinct."""

    root = tmp_path / "bronze"
    lineage = _lineage("perps_trades", "perp", "tick")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    _write_rows(root, lineage, "data.parquet", [{"open_time": start + timedelta(seconds=31)}])
    _write_rows(
        root,
        lineage,
        "empty_minutes.parquet",
        [{"minute": start + timedelta(minutes=1), "status": "confirmed_empty"}],
    )
    before = _lake_bytes(root)

    report = audit_historical_completeness(
        bronze_root=root,
        specs=(
            LineageAuditSpec(
                lineage=lineage,
                family="trade",
                start=start,
                end=start + timedelta(minutes=3),
                cadence=timedelta(minutes=1),
                expected_provider_gaps=(
                    ExpectedProviderGap(
                        start=start + timedelta(minutes=2),
                        end=start + timedelta(minutes=2),
                    ),
                ),
            ),
        ),
    )

    assert [interval.category for interval in report.intervals] == [
        "observed_rows",
        "confirmed_empty",
        "expected_provider_gap",
        "unverified_acquisition",
    ]
    assert report.status == "RECONCILE_REQUIRED"
    assert report.to_json() == report.to_json()
    assert str(root) not in report.to_json()
    assert _lake_bytes(root) == before


def test_ohlcv_and_funding_use_their_documented_cadences(tmp_path: Path) -> None:
    """OHLCV checks every minute while funding checks native eight-hour observations."""

    root = tmp_path / "bronze"
    start = datetime(2026, 1, 1, tzinfo=UTC)
    ohlcv = _lineage("perps_ohlcv", "perp", "1m")
    funding = _lineage("funding", "perp", "8h")
    _write_rows(
        root,
        ohlcv,
        "data.parquet",
        [{"open_time": start}, {"open_time": start + timedelta(minutes=2)}],
    )
    _write_rows(
        root,
        funding,
        "data.parquet",
        [{"open_time": start}, {"open_time": start + timedelta(hours=16)}],
    )

    report = audit_historical_completeness(
        bronze_root=root,
        specs=(
            LineageAuditSpec(
                lineage=ohlcv,
                family="ohlcv",
                start=start,
                end=start + timedelta(minutes=2),
                cadence=timedelta(minutes=1),
            ),
            LineageAuditSpec(
                lineage=funding,
                family="funding",
                start=start,
                end=start + timedelta(hours=16),
                cadence=timedelta(hours=8),
            ),
        ),
    )

    missing = [interval for interval in report.intervals if interval.category == "unverified_acquisition"]
    assert [(interval.lineage.dataset_type, interval.start, interval.end) for interval in missing] == [
        ("funding", "2026-01-01T08:00:00Z", "2026-01-01T08:00:00Z"),
        ("perps_ohlcv", "2026-01-01T00:01:00Z", "2026-01-01T00:01:00Z"),
    ]


def test_open_interest_uses_event_capability_and_fails_closed(tmp_path: Path) -> None:
    """Settlement observations do not fabricate or prove a generic one-minute history."""

    root = tmp_path / "bronze"
    start = datetime(2026, 1, 1, tzinfo=UTC)
    lineage = _lineage("open_interest", "perp", "1m")
    _write_rows(root, lineage, "data.parquet", [{"open_time": start + timedelta(seconds=34)}])

    report = audit_historical_completeness(
        bronze_root=root,
        specs=(
            LineageAuditSpec(
                lineage=lineage,
                family="open_interest",
                start=start,
                end=start + timedelta(days=1),
                cadence=None,
            ),
        ),
    )

    assert len(report.intervals) == 1
    assert report.intervals[0].category == "unverified_acquisition"
    assert report.status == "RECONCILE_REQUIRED"


def test_report_schema_rejects_pass_for_unverified_interval() -> None:
    """The report contract itself prevents false certification."""

    with pytest.raises(ValueError, match="cannot PASS"):
        CompletenessInterval(
            lineage=_lineage("perps_ohlcv", "perp", "1m"),
            start="2026-01-01T00:00:00Z",
            end="2026-01-01T00:00:00Z",
            category="unverified_acquisition",
            evidence="missing:acquisition_evidence",
            status="PASS",
        )


def test_config_expands_every_dataset_and_btc_eth_sol_lineage() -> None:
    """The existing Bronze contract expands all configured families and listing starts."""

    config: dict[str, object] = {
        "bronze-build": {
            "exchange": "deribit",
            "dataset": [
                "spot_ohlcv",
                "perps_ohlcv",
                "open_interest",
                "funding",
                "perps_trades",
                "options_trades",
                "volatility_index_data",
            ],
            "symbols": ["BTC", "ETH", "SOL"],
            "symbol_start_dates": ["BTC=2018-08-14", "ETH=2019-03-14", "SOL=2022-03-15"],
        }
    }

    specs = lineage_audit_specs_from_config(
        config=config,
        end=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert len(specs) == 21
    assert {spec.lineage.symbol for spec in specs if spec.lineage.instrument_type == "perp"} == {
        "BTC-PERPETUAL",
        "ETH-PERPETUAL",
        "SOL-PERPETUAL",
    }
    assert {spec.family for spec in specs} == {"ohlcv", "funding", "open_interest", "trade"}
