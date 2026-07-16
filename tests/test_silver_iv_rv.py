"""Tests for IV/RV spread Silver features."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.dataset_contracts import SILVER_IV_RV_FEATURE_COLUMNS
from application.services.silver_service import build_iv_rv_1m_feature_for_symbol, discover_iv_rv_symbols

pl = pytest.importorskip("polars")


def _write_feature_file(
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


def test_build_iv_rv_feature_keeps_missing_iv_explicit(tmp_path: Path) -> None:
    """SOL rows with RV but no IV should remain present with explicit availability flags."""

    silver = tmp_path / "silver"
    month = "2026-06"
    t0 = datetime(2026, 6, 12, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 6, 12, 0, 1, tzinfo=UTC)

    _write_feature_file(
        silver,
        dataset_type="volatility_index_1m_feature",
        exchange="deribit",
        symbol="BTC",
        month=month,
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": "deribit",
                "symbol": "BTC",
                "iv_close": 60.0,
                "minutes_since_iv_observation": 0,
            },
            {
                "timestamp_m1": t1,
                "exchange": "deribit",
                "symbol": "BTC",
                "iv_close": 66.0,
                "minutes_since_iv_observation": 0,
            },
        ],
    )
    _write_feature_file(
        silver,
        dataset_type="realized_volatility_1m_feature",
        exchange="deribit",
        symbol="BTC",
        month=month,
        rows=[
            {"timestamp_m1": t0, "exchange": "deribit", "symbol": "BTC", "rv_1h": 10.0, "rv_1d": 20.0},
            {"timestamp_m1": t1, "exchange": "deribit", "symbol": "BTC", "rv_1h": 11.0, "rv_1d": 22.0},
        ],
    )
    _write_feature_file(
        silver,
        dataset_type="realized_volatility_1m_feature",
        exchange="deribit",
        symbol="SOL",
        month=month,
        rows=[
            {"timestamp_m1": t0, "exchange": "deribit", "symbol": "SOL", "rv_1h": 5.0, "rv_1d": 7.0},
        ],
    )

    assert discover_iv_rv_symbols(silver_root=str(silver), exchange="deribit") == ["BTC", "SOL"]

    btc_report = build_iv_rv_1m_feature_for_symbol(silver_root=str(silver), exchange="deribit", symbol="BTC")
    sol_report = build_iv_rv_1m_feature_for_symbol(silver_root=str(silver), exchange="deribit", symbol="SOL")

    assert btc_report.rows_out == 2
    assert sol_report.rows_out == 1
    btc = pl.read_parquet(
        silver
        / "dataset_type=iv_rv_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-06"
        / "BTC-2026-06.parquet"
    )
    sol = pl.read_parquet(
        silver
        / "dataset_type=iv_rv_1m_feature"
        / "exchange=deribit"
        / "symbol=SOL"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-06"
        / "SOL-2026-06.parquet"
    )

    assert btc.columns == SILVER_IV_RV_FEATURE_COLUMNS
    assert btc["iv_minus_rv_1h"].to_list() == [50.0, 55.0]
    assert btc["iv_rv_ratio_1h"].to_list() == [6.0, 6.0]
    assert btc["iv_available"].to_list() == [True, True]
    assert btc["rv_available"].to_list() == [True, True]
    assert sol.columns == SILVER_IV_RV_FEATURE_COLUMNS
    assert sol["symbol"].to_list() == ["SOL"]
    assert sol["iv_minus_rv_1h"].to_list() == [None]
    assert sol["iv_rv_ratio_1h"].to_list() == [None]
    assert sol["iv_available"].to_list() == [False]
    assert sol["rv_available"].to_list() == [True]
