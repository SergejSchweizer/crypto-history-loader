"""Tests for deterministic option-surface Silver features."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from application.dataset_contracts import (
    SILVER_OPTION_SURFACE_FEATURE_COLUMNS,
    SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS,
)
from application.services.silver_service import (
    build_options_surface_1m_feature_for_symbol,
    discover_options_surface_symbols,
)

pl = pytest.importorskip("polars")


def _write_observed(
    root: Path,
    *,
    dataset_type: str,
    symbol: str,
    month: str,
    rows: Sequence[dict[str, object]],
) -> None:
    target = (
        root
        / f"dataset_type={dataset_type}"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / f"year={month[:4]}"
        / f"month={month}"
        / f"{symbol}-{month}.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).select(SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS).write_parquet(target)


def _option_row(
    *,
    timestamp: datetime,
    instrument_name: str,
    expiry: date,
    strike: float,
    option_type: str,
    implied_volatility: float,
    ingested_after_seconds: int = 10,
    bid_price: float = 1.0,
    ask_price: float = 2.0,
    source_endpoint: str = "currency",
) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "exchange": "deribit",
        "symbol": "BTC",
        "instrument_name": instrument_name,
        "underlying": "BTC",
        "expiry": expiry,
        "strike": strike,
        "underlying_price": 100.0,
        "index_price": 100.0,
        "option_type": option_type,
        "mark_price": 1.5,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "implied_volatility": implied_volatility,
        "delta": None,
        "gamma": None,
        "vega": None,
        "theta": None,
        "open_interest": 1.0,
        "volume": 1.0,
        "ingested_at": timestamp + timedelta(seconds=ingested_after_seconds),
        "source_endpoint": source_endpoint,
    }


def test_options_surface_buckets_precedence_freshness_and_no_future_leakage(tmp_path: Path) -> None:
    """Surface proxies should use fixed buckets and keep future minutes isolated."""

    silver = tmp_path / "silver"
    t0 = datetime(2026, 5, 1, 12, 0, 10, tzinfo=UTC)
    short_expiry = date(2026, 5, 5)
    long_expiry = date(2026, 6, 15)
    overlapping = _option_row(
        timestamp=t0,
        instrument_name="BTC-5MAY26-100-C",
        expiry=short_expiry,
        strike=100.0,
        option_type="C",
        implied_volatility=40.0,
    )
    _write_observed(
        silver,
        dataset_type="options_ticker_snapshot_1m_observed",
        symbol="BTC",
        month="2026-05",
        rows=[overlapping],
    )

    instrument_rows = [
        {**overlapping, "implied_volatility": 52.0, "source_endpoint": "instrument"},
        _option_row(
            timestamp=t0,
            instrument_name="BTC-5MAY26-100-P",
            expiry=short_expiry,
            strike=100.0,
            option_type="P",
            implied_volatility=56.0,
            ingested_after_seconds=120,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-15JUN26-100-C",
            expiry=long_expiry,
            strike=100.0,
            option_type="C",
            implied_volatility=60.0,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-15JUN26-100-P",
            expiry=long_expiry,
            strike=100.0,
            option_type="P",
            implied_volatility=62.0,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-15JUN26-90-P",
            expiry=long_expiry,
            strike=90.0,
            option_type="P",
            implied_volatility=70.0,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-15JUN26-110-C",
            expiry=long_expiry,
            strike=110.0,
            option_type="C",
            implied_volatility=60.0,
            bid_price=0.0,
            ask_price=0.0,
        ),
        _option_row(
            timestamp=t0 + timedelta(minutes=1),
            instrument_name="BTC-5MAY26-100-C",
            expiry=short_expiry,
            strike=100.0,
            option_type="C",
            implied_volatility=99.0,
        ),
    ]
    _write_observed(
        silver,
        dataset_type="options_instrument_ticker_snapshot_1m_observed",
        symbol="BTC",
        month="2026-05",
        rows=instrument_rows,
    )

    assert discover_options_surface_symbols(silver_root=str(silver), exchange="deribit") == ["BTC"]
    report = build_options_surface_1m_feature_for_symbol(silver_root=str(silver), exchange="deribit", symbol="btc")

    assert report.dataset == "options_surface_1m_feature"
    assert report.rows_in == 8
    assert report.rows_out == 2
    assert report.duplicates_removed == 1
    output = pl.read_parquet(
        silver
        / "dataset_type=options_surface_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    ).sort("timestamp_m1")
    assert output.columns == SILVER_OPTION_SURFACE_FEATURE_COLUMNS
    first = output.row(0, named=True)
    second = output.row(1, named=True)
    assert first["atm_iv"] == pytest.approx(58.0)
    assert first["short_dated_iv"] == pytest.approx(54.0)
    assert first["skew"] == pytest.approx(10.0)
    assert first["term_structure"] == pytest.approx(7.0)
    assert first["put_call_iv_spread"] == pytest.approx(3.0)
    assert first["contract_count"] == 6
    assert first["fresh_quote_count"] == 5
    assert first["stale_quote_count"] == 1
    assert first["max_quote_age_seconds"] == pytest.approx(120.0)
    assert first["quote_coverage_ratio"] == pytest.approx(5.0 / 6.0)
    assert second["atm_iv"] == pytest.approx(99.0)
    assert second["contract_count"] == 1


def test_options_surface_bucket_boundaries_and_latest_ingest_tie_break(tmp_path: Path) -> None:
    """Surface buckets should use documented edge rules and newest ingested duplicate quotes."""

    silver = tmp_path / "silver"
    t0 = datetime(2026, 5, 1, 12, 0, 10, tzinfo=UTC)
    expiry = date(2026, 6, 15)
    rows = [
        _option_row(
            timestamp=t0,
            instrument_name="BTC-15JUN26-100-C",
            expiry=expiry,
            strike=100.0,
            option_type="C",
            implied_volatility=30.0,
            ingested_after_seconds=5,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-15JUN26-100-C",
            expiry=expiry,
            strike=100.0,
            option_type="C",
            implied_volatility=50.0,
            ingested_after_seconds=20,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-15JUN26-100-P",
            expiry=expiry,
            strike=100.0,
            option_type="P",
            implied_volatility=70.0,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-15JUN26-95-P",
            expiry=expiry,
            strike=95.0,
            option_type="P",
            implied_volatility=90.0,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-15JUN26-94.9-P",
            expiry=expiry,
            strike=94.9,
            option_type="P",
            implied_volatility=80.0,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-15JUN26-105-C",
            expiry=expiry,
            strike=105.0,
            option_type="C",
            implied_volatility=40.0,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-15JUN26-106-C",
            expiry=expiry,
            strike=106.0,
            option_type="C",
            implied_volatility=60.0,
        ),
    ]
    _write_observed(
        silver,
        dataset_type="options_instrument_ticker_snapshot_1m_observed",
        symbol="BTC",
        month="2026-05",
        rows=rows,
    )

    report = build_options_surface_1m_feature_for_symbol(silver_root=str(silver), exchange="deribit", symbol="BTC")

    assert report.rows_in == 7
    assert report.rows_out == 1
    assert report.duplicates_removed == 1
    output = pl.read_parquet(
        silver
        / "dataset_type=options_surface_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    )
    row = output.row(0, named=True)
    assert row["atm_iv"] == pytest.approx(50.0)
    assert row["put_call_iv_spread"] == pytest.approx(25.0)
    assert row["skew"] == pytest.approx(20.0)
