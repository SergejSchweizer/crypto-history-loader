"""Tests for options ticker snapshot Silver transformations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from application.dataset_contracts import SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS
from application.services.silver_service import (
    build_options_ticker_observed_for_symbol,
    discover_options_ticker_symbols,
)

pl = pytest.importorskip("polars")


def _write_options_ticker_hour_file(
    root: Path,
    *,
    exchange: str,
    currency: str,
    month: str,
    day: str,
    hour: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    target = (
        root
        / "dataset_type=options_ticker_snapshot_1m"
        / f"exchange={exchange}"
        / "instrument_type=option"
        / f"currency={currency}"
        / "source=rest_get_book_summary_by_currency"
        / f"year={month.split('-', 1)[0]}"
        / f"month={month.split('-', 1)[1]}"
        / f"date={day}"
        / f"hour={hour}"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([dict(row) for row in rows]).write_parquet(target)


def test_build_options_ticker_parses_contracts_and_filters_invalid_rows(tmp_path: Path) -> None:
    """Options ticker observed output should parse calls/puts and reject bad contract metadata."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    t0 = datetime(2026, 5, 24, 8, 0, tzinfo=UTC)
    common = {
        "exchange": "deribit",
        "dataset_type": "options_ticker_snapshot_1m",
        "source": "rest_get_book_summary_by_currency",
        "currency": "BTC",
        "requested_currency": "BTC",
        "source_currency": "BTC",
        "base_currency": "BTC",
        "quote_currency": "BTC",
        "instrument_type": "option",
        "snapshot_time": t0,
        "exchange_creation_time": t0,
        "ingested_at": t0,
        "run_id": "r1",
        "bid_price": 1.0,
        "ask_price": 2.0,
        "mid_price": 1.5,
        "mark_price": 1.6,
        "mark_iv": 60.0,
        "underlying_price": 100000.0,
        "underlying_index": "btc_usd",
        "interest_rate": 0.01,
        "open_interest": 10.0,
        "volume": 3.0,
        "volume_usd": 300.0,
        "high": 2.0,
        "low": 1.0,
        "last": 1.7,
        "price_change": 0.1,
        "raw_payload_hash": "h",
        "schema_version": "v1",
    }
    _write_options_ticker_hour_file(
        bronze,
        exchange="deribit",
        currency="BTC",
        month="2026-05",
        day="2026-05-24",
        hour="08",
        rows=[
            {**common, "instrument_name": "BTC-12JUN26-65000-C"},
            {**common, "instrument_name": "BTC-12JUN26-65000-P", "mark_price": 1.8},
            {**common, "instrument_name": "BTC-12BAD26-65000-C"},
            {**common, "instrument_name": "BTC-12JUN26-0-P"},
        ],
    )

    assert discover_options_ticker_symbols(bronze_root=str(bronze), exchange="deribit") == ["BTC"]

    report = build_options_ticker_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

    assert report.rows_in == 4
    assert report.rows_out == 2
    assert report.invalid_ohlc_rows == 2
    output = pl.read_parquet(
        silver
        / "dataset_type=options_ticker_snapshot_1m_observed"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    )
    assert output.columns == SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS
    assert output["instrument_name"].to_list() == ["BTC-12JUN26-65000-C", "BTC-12JUN26-65000-P"]
    assert output["underlying"].to_list() == ["BTC", "BTC"]
    assert output["expiry"].to_list() == [date(2026, 6, 12), date(2026, 6, 12)]
    assert output["strike"].to_list() == [65000.0, 65000.0]
    assert output["option_type"].to_list() == ["C", "P"]
    assert output["implied_volatility"].to_list() == [60.0, 60.0]
    assert output["delta"].to_list() == [None, None]
    assert output["gamma"].to_list() == [None, None]
    assert output["vega"].to_list() == [None, None]
    assert output["theta"].to_list() == [None, None]
