"""Tests for shared daily instrument metadata Silver views."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from application.dataset_contracts import SILVER_INSTRUMENT_METADATA_OBSERVED_COLUMNS
from application.services.silver_service import (
    build_futures_instrument_metadata_observed_for_symbol,
    build_instrument_metadata_observed_for_symbol,
    discover_instrument_metadata_symbols,
)

pl = pytest.importorskip("polars")


def _write_metadata(root: Path, *, dataset_type: str, rows: list[dict[str, object]]) -> None:
    target = (
        root
        / f"dataset_type={dataset_type}"
        / "exchange=deribit"
        / "year=2026"
        / "month=05"
        / "date=2026-05-25"
        / "hour=00"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target)


def _metadata_row(
    *,
    dataset_type: str,
    instrument_name: str,
    kind: str,
    ingested_at: datetime,
    expiration_timestamp: datetime | None,
    option_type: str | None,
    strike: float | None,
    tick_size: float,
    state: str | None = None,
) -> dict[str, object]:
    return {
        "schema_version": "v1",
        "dataset_type": dataset_type,
        "exchange": "deribit",
        "source": "rest_get_instruments",
        "snapshot_date": date(2026, 5, 25),
        "ingested_at": ingested_at,
        "instrument_name": instrument_name,
        "kind": kind,
        "base_currency": "BTC",
        "quote_currency": "BTC" if kind == "option" else "USD",
        "settlement_currency": "BTC",
        "instrument_type": "reversed",
        "state": state,
        "tick_size": tick_size,
        "contract_size": 1.0 if kind == "option" else 10.0,
        "min_trade_amount": 0.1 if kind == "option" else 10.0,
        "is_active": True,
        "creation_timestamp": datetime(2026, 5, 1, tzinfo=UTC),
        "expiration_timestamp": expiration_timestamp,
        "option_type": option_type,
        "strike": strike,
    }


def test_metadata_builders_share_contract_and_keep_latest_valid_daily_rows(tmp_path: Path) -> None:
    """Options and futures metadata should expose one common latest-valid daily contract."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    ingested = datetime(2026, 5, 25, 3, tzinfo=UTC)
    option = _metadata_row(
        dataset_type="instrument_metadata_snapshot_daily",
        instrument_name="BTC-12JUN26-65000-C",
        kind="option",
        ingested_at=ingested,
        expiration_timestamp=datetime(2026, 6, 12, 8, tzinfo=UTC),
        option_type="call",
        strike=65000.0,
        tick_size=0.0001,
    )
    _write_metadata(
        bronze,
        dataset_type="instrument_metadata_snapshot_daily",
        rows=[
            option,
            {**option, "ingested_at": ingested + timedelta(minutes=1), "tick_size": 0.0002},
            {**option, "instrument_name": "BROKEN", "strike": None},
        ],
    )
    future = _metadata_row(
        dataset_type="futures_instrument_metadata_snapshot_daily",
        instrument_name="BTC-19JUN26",
        kind="future",
        ingested_at=ingested,
        expiration_timestamp=datetime(2026, 6, 19, 8, tzinfo=UTC),
        option_type=None,
        strike=None,
        tick_size=2.5,
        state="open",
    )
    perpetual = _metadata_row(
        dataset_type="futures_instrument_metadata_snapshot_daily",
        instrument_name="BTC-PERPETUAL",
        kind="future",
        ingested_at=ingested,
        expiration_timestamp=None,
        option_type=None,
        strike=None,
        tick_size=0.5,
        state="open",
    )
    _write_metadata(
        bronze,
        dataset_type="futures_instrument_metadata_snapshot_daily",
        rows=[future, perpetual, {**future, "instrument_name": "BTC-BROKEN", "tick_size": -1.0}],
    )

    assert discover_instrument_metadata_symbols(
        bronze_root=str(bronze),
        exchange="deribit",
        dataset_type="instrument_metadata_snapshot_daily",
    ) == ["BTC"]
    option_report = build_instrument_metadata_observed_for_symbol(
        bronze_root=str(bronze), silver_root=str(silver), exchange="deribit", symbol="btc"
    )
    futures_report = build_futures_instrument_metadata_observed_for_symbol(
        bronze_root=str(bronze), silver_root=str(silver), exchange="deribit", symbol="BTC"
    )

    assert option_report.rows_in == 3
    assert option_report.rows_out == 1
    assert option_report.duplicates_removed == 1
    assert option_report.invalid_ohlc_rows == 1
    assert futures_report.rows_in == 3
    assert futures_report.rows_out == 2
    assert futures_report.invalid_ohlc_rows == 1
    option_path = (
        silver
        / "dataset_type=instrument_metadata_snapshot_daily_observed"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1d"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    )
    futures_path = Path(
        str(option_path).replace(
            "instrument_metadata_snapshot_daily_observed",
            "futures_instrument_metadata_snapshot_daily_observed",
        )
    )
    options = pl.read_parquet(option_path)
    futures = pl.read_parquet(futures_path).sort("instrument_name")
    assert options.columns == SILVER_INSTRUMENT_METADATA_OBSERVED_COLUMNS
    assert futures.columns == SILVER_INSTRUMENT_METADATA_OBSERVED_COLUMNS
    assert options["instrument_type"].to_list() == ["option"]
    assert options["option_type"].to_list() == ["C"]
    assert options["tick_size"].to_list() == [0.0002]
    assert options["is_listed"].to_list() == [True]
    assert options["listing_state"].to_list() == ["active"]
    assert futures["instrument_type"].to_list() == ["future", "perp"]
    assert futures["expiry"].to_list() == [date(2026, 6, 19), None]
    assert futures["listing_state"].to_list() == ["open", "open"]
    common = pl.concat([options, futures], how="vertical")
    assert common.select(["snapshot_date", "instrument_name"]).unique().height == 3
