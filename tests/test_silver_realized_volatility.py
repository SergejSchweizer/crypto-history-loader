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


def test_build_realized_volatility_keeps_spot_and_perps_returns_separate(tmp_path: Path) -> None:
    """QC-03: spot/perp availability gaps must not create artificial basis returns."""

    silver = tmp_path / "silver"
    month = "2026-06"
    t0 = datetime(2026, 6, 12, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 6, 12, 0, 1, tzinfo=UTC)
    t2 = datetime(2026, 6, 12, 0, 2, tzinfo=UTC)
    t3 = datetime(2026, 6, 12, 0, 3, tzinfo=UTC)

    def _row(index: int, timestamp: datetime, close: float, dataset_type: str, symbol: str) -> dict[str, object]:
        return {
            "schema_version": "v1",
            "dataset_type": dataset_type,
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": dataset_type,
            "event_time": timestamp,
            "ingested_at": timestamp,
            "run_id": f"r{index}",
            "source_endpoint": "public_get_tradingview_chart_data",
            "open_time": timestamp,
            "close_time": timestamp,
            "timeframe": "1m",
            "open_price": close,
            "high_price": close + 1.0,
            "low_price": close - 1.0,
            "close_price": close,
            "volume": 1.0,
            "quote_volume": 1.0,
            "trade_count": 1,
            "origin_payload": "{}",
        }

    _write_silver_ohlcv_file(
        silver,
        dataset_type="spot_ohlcv",
        exchange="deribit",
        symbol="BTC_USDC",
        month=month,
        rows=[
            _row(0, t0, 100.0, "spot_ohlcv", "BTC_USDC"),
            _row(1, t1, 101.0, "spot_ohlcv", "BTC_USDC"),
            _row(2, t3, 102.0, "spot_ohlcv", "BTC_USDC"),
        ],
    )
    _write_silver_ohlcv_file(
        silver,
        dataset_type="perps_ohlcv",
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        month=month,
        rows=[
            _row(3, t0, 200.0, "perps_ohlcv", "BTC-PERPETUAL"),
            _row(4, t2, 202.0, "perps_ohlcv", "BTC-PERPETUAL"),
            _row(5, t3, 204.0, "perps_ohlcv", "BTC-PERPETUAL"),
        ],
    )

    build_realized_volatility_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

    written = pl.read_parquet(
        silver
        / "dataset_type=realized_volatility_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-06"
        / "BTC-2026-06.parquet"
    )

    assert written["canonical_rv_source"].to_list() == ["perps", "perps", "perps", "perps"]
    assert written["spot_available"].to_list() == [True, True, False, True]
    assert written["perps_available"].to_list() == [True, False, True, True]
    assert written["canonical_rv_source_available"].to_list() == [True, False, True, True]
    assert written["rv_5m"].to_list()[1] is None
    assert written["spot_log_return"].to_list()[1] == pytest.approx(log(101.0 / 100.0))
    assert written["perps_log_return"].to_list()[2] == pytest.approx(log(202.0 / 200.0))
    assert written["rv_5m"].to_list()[2] == pytest.approx(abs(log(202.0 / 200.0)))
    assert written["rv_5m"].to_list()[2] != pytest.approx(abs(log(202.0 / 101.0)))
    assert written["spot_rv_5m"].to_list()[3] == pytest.approx(
        (log(101.0 / 100.0) ** 2 + log(102.0 / 101.0) ** 2) ** 0.5
    )
    assert written["perps_rv_5m"].to_list()[3] == pytest.approx(
        (log(202.0 / 200.0) ** 2 + log(204.0 / 202.0) ** 2) ** 0.5
    )


def test_build_realized_volatility_preserves_previous_close_across_month_boundary(tmp_path: Path) -> None:
    """QC-02: the first minute of a month must see the prior month's final close."""

    silver = tmp_path / "silver"

    def _row(index: int, timestamp: datetime, close: float) -> dict[str, object]:
        return {
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

    _write_silver_ohlcv_file(
        silver,
        dataset_type="spot_ohlcv",
        exchange="deribit",
        symbol="BTC_USDC",
        month="2026-01",
        rows=[_row(0, datetime(2026, 1, 31, 23, 59, tzinfo=UTC), 100.0)],
    )
    _write_silver_ohlcv_file(
        silver,
        dataset_type="spot_ohlcv",
        exchange="deribit",
        symbol="BTC_USDC",
        month="2026-02",
        rows=[_row(1, datetime(2026, 2, 1, 0, 0, tzinfo=UTC), 105.0)],
    )

    report = build_realized_volatility_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

    assert report.rows_in == 2
    assert report.rows_out == 2

    january_output = pl.read_parquet(
        silver
        / "dataset_type=realized_volatility_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-01"
        / "BTC-2026-01.parquet"
    )
    february_output = pl.read_parquet(
        silver
        / "dataset_type=realized_volatility_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-02"
        / "BTC-2026-02.parquet"
    )

    # Storage-partition trimming: each month's output only contains its own rows.
    assert january_output.height == 1
    assert february_output.height == 1

    expected_log_return = log(105.0 / 100.0)
    # Without cross-month buffering this would be null because the prior close
    # lived in a different monthly partition.
    assert february_output["rv_5m"].to_list()[0] == pytest.approx(abs(expected_log_return))


def test_build_realized_volatility_preserves_previous_close_across_year_boundary(tmp_path: Path) -> None:
    """QC-02: the first minute of a year must see the prior year's final close."""

    silver = tmp_path / "silver"

    def _row(index: int, timestamp: datetime, close: float) -> dict[str, object]:
        return {
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

    _write_silver_ohlcv_file(
        silver,
        dataset_type="spot_ohlcv",
        exchange="deribit",
        symbol="BTC_USDC",
        month="2025-12",
        rows=[_row(0, datetime(2025, 12, 31, 23, 59, tzinfo=UTC), 100.0)],
    )
    _write_silver_ohlcv_file(
        silver,
        dataset_type="spot_ohlcv",
        exchange="deribit",
        symbol="BTC_USDC",
        month="2026-01",
        rows=[_row(1, datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 105.0)],
    )

    report = build_realized_volatility_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

    assert report.rows_in == 2
    assert report.rows_out == 2

    december_output = pl.read_parquet(
        silver
        / "dataset_type=realized_volatility_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2025"
        / "month=2025-12"
        / "BTC-2025-12.parquet"
    )
    january_output = pl.read_parquet(
        silver
        / "dataset_type=realized_volatility_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-01"
        / "BTC-2026-01.parquet"
    )

    assert december_output.height == 1
    assert january_output.height == 1

    expected_log_return = log(105.0 / 100.0)
    # Without cross-year buffering this would be null because the prior close
    # lived in a different calendar-year monthly partition.
    assert january_output["rv_5m"].to_list()[0] == pytest.approx(abs(expected_log_return))
