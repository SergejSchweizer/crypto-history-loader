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


def _write_empty_data_file(root: Path, lineage: HistoricalBronzeLineage) -> None:
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
        / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(schema={"open_time": pl.Datetime(time_unit="ms", time_zone="UTC")}).write_parquet(path)


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


@pytest.mark.parametrize("section_name", ["bronze-ingest", "loader"])
def test_config_accepts_historical_bronze_section_aliases(section_name: str) -> None:
    """Legacy Bronze section aliases retain the same audit configuration contract."""

    specs = lineage_audit_specs_from_config(
        config={
            section_name: {
                "dataset": ["perps_ohlcv"],
                "symbols": ["BTC"],
                "start_date": "2026-01-01",
            }
        },
        end=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert len(specs) == 1
    assert specs[0].start == datetime(2026, 1, 1, tzinfo=UTC)
    assert specs[0].lineage.symbol == "BTC-PERPETUAL"


def test_config_start_date_precedence_uses_specific_bound_and_global_floor() -> None:
    """Exchange-symbol starts win over symbol starts but cannot precede the global floor."""

    specs = lineage_audit_specs_from_config(
        config={
            "bronze-build": {
                "dataset": ["perps_ohlcv"],
                "symbols": ["BTC", "ETH"],
                "start_date": "2020-01-01",
                "symbol_start_dates": ["BTC=2019-01-01", "ETH=2021-01-01"],
                "exchange_symbol_start_dates": ["deribit:BTC=2022-01-01"],
            }
        },
        end=datetime(2026, 1, 2, tzinfo=UTC),
    )

    starts = {spec.lineage.symbol: spec.start for spec in specs}
    assert starts == {
        "BTC-PERPETUAL": datetime(2022, 1, 1, tzinfo=UTC),
        "ETH-PERPETUAL": datetime(2021, 1, 1, tzinfo=UTC),
    }


def test_config_attaches_sorted_provider_gaps_to_matching_lineage() -> None:
    """Configured UTC provider gaps are parsed, ordered, and scoped by lineage identity."""

    specs = lineage_audit_specs_from_config(
        config={
            "bronze-build": {
                "dataset": ["perps_ohlcv"],
                "symbols": ["BTC"],
                "start_date": "2026-01-01",
            },
            "historical-completeness-audit": {
                "expected_provider_gaps": [
                    {
                        "lineage": "perps_ohlcv|deribit|perp|BTC-PERPETUAL|1m",
                        "start": "2026-01-03T00:00:00Z",
                        "end": "2026-01-03T00:01:00Z",
                    },
                    {
                        "lineage": "perps_ohlcv|deribit|perp|BTC-PERPETUAL|1m",
                        "start": "2026-01-02T00:00:00Z",
                        "end": "2026-01-02T00:01:00Z",
                    },
                ]
            },
        },
        end=datetime(2026, 1, 4, tzinfo=UTC),
    )

    assert [(gap.start, gap.end) for gap in specs[0].expected_provider_gaps] == [
        (datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 2, 0, 1, tzinfo=UTC)),
        (datetime(2026, 1, 3, tzinfo=UTC), datetime(2026, 1, 3, 0, 1, tzinfo=UTC)),
    ]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"start": datetime(2026, 1, 1), "end": datetime(2026, 1, 1, tzinfo=UTC)},
            "timezone-aware UTC",
        ),
        (
            {
                "start": datetime(2026, 1, 1, tzinfo=UTC),
                "end": datetime(2025, 12, 31, tzinfo=UTC),
            },
            "must not precede",
        ),
    ],
)
def test_expected_provider_gap_rejects_invalid_bounds(kwargs: dict[str, datetime], message: str) -> None:
    """Configured provider-gap evidence requires ordered UTC bounds."""

    with pytest.raises(ValueError, match=message):
        ExpectedProviderGap(**kwargs)


@pytest.mark.parametrize(
    ("family", "cadence", "message"),
    [
        ("open_interest", timedelta(minutes=1), "event-driven cadence"),
        ("funding", None, "positive provider cadence"),
        ("trade", timedelta(0), "positive provider cadence"),
    ],
)
def test_lineage_audit_spec_rejects_invalid_family_cadence(
    family: str, cadence: timedelta | None, message: str
) -> None:
    """Family contracts prevent regular and event-driven audit policies from mixing."""

    with pytest.raises(ValueError, match=message):
        LineageAuditSpec(
            lineage=_lineage("perps_ohlcv", "perp", "1m"),
            family=family,  # type: ignore[arg-type]  # Parametrization exercises each literal family policy.
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 1, 1, tzinfo=UTC),
            cadence=cadence,
        )


@pytest.mark.parametrize(
    ("config", "message"),
    [
        ({}, "missing historical Bronze build section"),
        ({"bronze-build": {"dataset": [], "symbols": ["BTC"]}}, "dataset must be a non-empty list"),
        (
            {"bronze-build": {"dataset": ["perps_ohlcv"], "symbols": ["BTC"], "exchanges": ["binance"]}},
            "unsupported historical Bronze exchange",
        ),
        (
            {"bronze-build": {"dataset": ["unknown"], "symbols": ["BTC"], "start_date": "2026-01-01"}},
            "unsupported historical Bronze dataset",
        ),
        (
            {"bronze-build": {"dataset": ["perps_ohlcv"], "symbols": ["BTC"]}},
            "missing configured listing start",
        ),
        (
            {
                "bronze-build": {"dataset": ["perps_ohlcv"], "symbols": ["BTC"], "start_date": "2026-01-01"},
                "historical-completeness-audit": {"expected_provider_gaps": {}},
            },
            "expected_provider_gaps must be a list",
        ),
    ],
)
def test_config_rejects_invalid_historical_audit_contracts(config: dict[str, object], message: str) -> None:
    """Invalid audit configuration fails before it can produce a misleading report."""

    with pytest.raises(ValueError, match=message):
        lineage_audit_specs_from_config(config=config, end=datetime(2026, 1, 2, tzinfo=UTC))


def test_malformed_or_empty_parquet_evidence_fails_closed(tmp_path: Path) -> None:
    """Files without usable timestamp rows never count as observed acquisition evidence."""

    root = tmp_path / "bronze"
    start = datetime(2026, 1, 1, tzinfo=UTC)
    lineage = _lineage("perps_ohlcv", "perp", "1m")
    _write_empty_data_file(root, lineage)

    report = audit_historical_completeness(
        bronze_root=root,
        specs=(
            LineageAuditSpec(
                lineage=lineage,
                family="ohlcv",
                start=start,
                end=start,
                cadence=timedelta(minutes=1),
            ),
        ),
    )

    assert report.intervals[0].category == "unverified_acquisition"
    assert report.status == "RECONCILE_REQUIRED"


def test_parquet_without_timestamp_schema_fails_closed(tmp_path: Path) -> None:
    """A recognized data file missing open_time does not count as acquisition evidence."""

    root = tmp_path / "bronze"
    start = datetime(2026, 1, 1, tzinfo=UTC)
    lineage = _lineage("perps_ohlcv", "perp", "1m")
    _write_rows(root, lineage, "data.parquet", [{"not_open_time": "not-a-timestamp"}])

    report = audit_historical_completeness(
        bronze_root=root,
        specs=(
            LineageAuditSpec(
                lineage=lineage,
                family="ohlcv",
                start=start,
                end=start,
                cadence=timedelta(minutes=1),
            ),
        ),
    )

    assert report.intervals[0].category == "unverified_acquisition"
    assert report.status == "RECONCILE_REQUIRED"


def test_malformed_and_unconfirmed_empty_sidecars_do_not_certify_trade_minutes(tmp_path: Path) -> None:
    """Only correctly shaped confirmed-empty sidecars certify an otherwise missing trade minute."""

    root = tmp_path / "bronze"
    start = datetime(2026, 1, 1, tzinfo=UTC)
    lineage = _lineage("perps_trades", "perp", "tick")
    _write_rows(root, lineage, "empty_minutes.parquet", [{"minute": start, "state": "confirmed_empty"}])

    report = audit_historical_completeness(
        bronze_root=root,
        specs=(
            LineageAuditSpec(
                lineage=lineage,
                family="trade",
                start=start,
                end=start,
                cadence=timedelta(minutes=1),
            ),
        ),
    )

    assert report.intervals[0].category == "unverified_acquisition"
    assert report.status == "RECONCILE_REQUIRED"
