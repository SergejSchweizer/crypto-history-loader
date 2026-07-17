"""Tests for OHLCV-derived realized-volatility Silver features."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from math import log
from pathlib import Path

import pytest

from application.dataset_contracts import SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS
from application.services.silver_service import (
    build_realized_volatility_1m_feature_for_symbol,
    discover_realized_volatility_symbols,
)

pl = pytest.importorskip("polars")


def _write_silver_ohlcv_file(
    root: Path,
    *,
    dataset_type: str,
    exchange: str,
    symbol: str,
    month: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    target = (
        root
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / f"year={month.split('-', 1)[0]}"
        / f"month={month}"
        / f"{symbol}-{month}.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([dict(row) for row in rows]).write_parquet(target)


def test_build_realized_volatility_uses_trailing_ohlcv_windows(tmp_path: Path) -> None:
    """RV windows should use current and past OHLCV rows only."""

    silver = tmp_path / "silver"
    month = "2026-06"
    timestamps = [
        datetime(2026, 6, 12, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 12, 0, 5, tzinfo=UTC),
        datetime(2026, 6, 12, 0, 15, tzinfo=UTC),
        datetime(2026, 6, 12, 1, 0, tzinfo=UTC),
        datetime(2026, 6, 12, 1, 1, tzinfo=UTC),
    ]
    closes = [100.0, 105.0, 110.0, 120.0, 1000.0]
    rows = []
    for index, timestamp in enumerate(timestamps):
        close = closes[index]
        rows.append(
            {
                "schema_version": "v1",
                "dataset_type": "spot_ohlcv",
                "exchange": "deribit",
                "symbol": "BTC_USDC",
                "instrument_type": "spot_ohlcv",
                "event_time": timestamp,
                "ingested_at": timestamp,
                "run_id": f"r{index}",
                "source_endpoint": "public_get_tradingview_chart_data",
                "open_time": timestamp,
                "close_time": timestamp,
                "timeframe": "1m",
                "open_price": close - 1.0,
                "high_price": close + 1.0,
                "low_price": close - 2.0,
                "close_price": close,
                "volume": 1.0,
                "quote_volume": 1.0,
                "trade_count": 1,
                "origin_payload": "{}",
            }
        )
    _write_silver_ohlcv_file(
        silver,
        dataset_type="spot_ohlcv",
        exchange="deribit",
        symbol="BTC_USDC",
        month=month,
        rows=rows,
    )

    assert discover_realized_volatility_symbols(
        silver_root=str(silver),
        exchange="deribit",
    ) == ["BTC"]

    report = build_realized_volatility_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

    assert report.dataset == "realized_volatility_1m_feature"
    assert report.rows_in == 5
    assert report.rows_out == 5
    output_path = (
        silver
        / "dataset_type=realized_volatility_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-06"
        / "BTC-2026-06.parquet"
    )
    written = pl.read_parquet(output_path)
    assert written.columns == SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS
    assert written["spot_available"].to_list() == [True, True, True, True, True]
    assert written["perps_available"].to_list() == [False, False, False, False, False]
    assert written["rv_5m"].to_list()[0] is None
    assert written["rv_5m"].to_list()[1] == pytest.approx(abs(log(105.0 / 100.0)))
    assert written["rv_5m"].to_list()[3] == pytest.approx(abs(log(120.0 / 110.0)))
    assert written["rv_1h"].to_list()[3] == pytest.approx(
        (log(105.0 / 100.0) ** 2 + log(110.0 / 105.0) ** 2 + log(120.0 / 110.0) ** 2) ** 0.5
    )
    assert written["jump_proxy"].to_list()[0] is None


def test_annualized_rv_matches_hand_calculated_reference(tmp_path: Path) -> None:
    """QC-01: *_annualized_pct fields must scale the raw RV by the documented 365-day basis."""

    silver = tmp_path / "silver"
    month = "2026-06"
    timestamps = [
        datetime(2026, 6, 12, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 12, 0, 5, tzinfo=UTC),
    ]
    closes = [100.0, 105.0]
    rows = []
    for index, timestamp in enumerate(timestamps):
        close = closes[index]
        rows.append(
            {
                "schema_version": "v1",
                "dataset_type": "spot_ohlcv",
                "exchange": "deribit",
                "symbol": "BTC_USDC",
                "instrument_type": "spot_ohlcv",
                "event_time": timestamp,
                "ingested_at": timestamp,
                "run_id": f"r{index}",
                "source_endpoint": "public_get_tradingview_chart_data",
                "open_time": timestamp,
                "close_time": timestamp,
                "timeframe": "1m",
                "open_price": close - 1.0,
                "high_price": close + 1.0,
                "low_price": close - 2.0,
                "close_price": close,
                "volume": 1.0,
                "quote_volume": 1.0,
                "trade_count": 1,
                "origin_payload": "{}",
            }
        )
    _write_silver_ohlcv_file(
        silver,
        dataset_type="spot_ohlcv",
        exchange="deribit",
        symbol="BTC_USDC",
        month=month,
        rows=rows,
    )

    report = build_realized_volatility_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )
    assert report.rows_out == 2
    output_path = (
        silver
        / "dataset_type=realized_volatility_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-06"
        / "BTC-2026-06.parquet"
    )
    written = pl.read_parquet(output_path)

    raw_rv_5m = abs(log(105.0 / 100.0))
    minutes_per_year = 365 * 24 * 60
    expected_annualized_pct = raw_rv_5m * (minutes_per_year / 5) ** 0.5 * 100.0

    assert written["rv_5m"].to_list()[1] == pytest.approx(raw_rv_5m)
    assert written["rv_5m_annualized_pct"].to_list()[1] == pytest.approx(expected_annualized_pct)
    # Null raw RV (insufficient trailing history) must stay null once annualized.
    assert written["rv_5m"].to_list()[0] is None
    assert written["rv_5m_annualized_pct"].to_list()[0] is None
