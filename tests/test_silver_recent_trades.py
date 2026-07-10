"""Tests for snapshot-derived recent-trade Silver normalization."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from application.dataset_contracts import (
    SILVER_RECENT_TRADE_SNAPSHOT_OBSERVED_COLUMNS,
    SILVER_TRADES_OBSERVED_COLUMNS,
)
from application.services.silver_service import (
    build_recent_trade_snapshot_observed_for_symbol,
    discover_recent_trade_symbols,
)

pl = pytest.importorskip("polars")


def _trade_row(
    *,
    instrument_type: str,
    instrument_name: str,
    trade_id: str | None,
    trade_time: datetime,
    snapshot_time: datetime,
    price: float,
    amount: float,
    direction: str,
) -> dict[str, object]:
    return {
        "schema_version": "v1",
        "dataset_type": "recent_trade_snapshot_1m",
        "exchange": "deribit",
        "source": "rest_get_last_trades_by_currency",
        "currency": "BTC",
        "instrument_name": instrument_name,
        "instrument_type": instrument_type,
        "trade_id": trade_id,
        "exchange_timestamp": trade_time,
        "snapshot_time": snapshot_time,
        "ingested_at": snapshot_time + timedelta(seconds=1),
        "price": price,
        "amount": amount,
        "direction": direction,
    }


def _write_bronze(root: Path, *, instrument_type: str, rows: list[dict[str, object]]) -> None:
    target = (
        root
        / "dataset_type=recent_trade_snapshot_1m"
        / "exchange=deribit"
        / f"instrument_type={instrument_type}"
        / "currency=BTC"
        / "source=rest_get_last_trades_by_currency"
        / "year=2026"
        / "month=06"
        / "date=2026-06-12"
        / "hour=12"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target)


def test_recent_trades_use_source_id_then_composite_fallback_and_reconcile(tmp_path: Path) -> None:
    """Snapshot trades should dedupe deterministically and report historical overlap."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    t0 = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
    _write_bronze(
        bronze,
        instrument_type="perp",
        rows=[
            _trade_row(
                instrument_type="perp",
                instrument_name="BTC-PERPETUAL",
                trade_id="known-1",
                trade_time=t0,
                snapshot_time=t0 + timedelta(minutes=1),
                price=100.0,
                amount=2.0,
                direction="buy",
            ),
            _trade_row(
                instrument_type="perp",
                instrument_name="BTC-PERPETUAL",
                trade_id="known-1",
                trade_time=t0,
                snapshot_time=t0 + timedelta(minutes=2),
                price=101.0,
                amount=2.0,
                direction="buy",
            ),
        ],
    )
    option_trade = _trade_row(
        instrument_type="option",
        instrument_name="BTC-19JUN26-65000-C",
        trade_id=None,
        trade_time=t0 + timedelta(seconds=30),
        snapshot_time=t0 + timedelta(minutes=1),
        price=0.1,
        amount=1.5,
        direction="sell",
    )
    _write_bronze(
        bronze,
        instrument_type="option",
        rows=[
            option_trade,
            {**option_trade, "snapshot_time": t0 + timedelta(minutes=2)},
            {**option_trade, "price": -1.0, "snapshot_time": t0 + timedelta(minutes=3)},
        ],
    )
    reference_path = (
        silver
        / "dataset_type=perps_trades_observed"
        / "exchange=deribit"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=tick"
        / "year=2026"
        / "month=2026-06"
        / "BTC-PERPETUAL-2026-06.parquet"
    )
    reference_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "trade_time": t0,
                "exchange": "deribit",
                "symbol": "BTC-PERPETUAL",
                "instrument_type": "perp",
                "trade_id": "known-1",
                "price": 100.0,
                "quantity": 2.0,
                "side": "buy",
            }
        ]
    ).select(SILVER_TRADES_OBSERVED_COLUMNS).write_parquet(reference_path)

    assert discover_recent_trade_symbols(bronze_root=str(bronze), exchange="deribit") == ["BTC"]
    report = build_recent_trade_snapshot_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="btc",
    )

    assert report.rows_in == 5
    assert report.rows_out == 2
    assert report.duplicates_removed == 2
    assert report.invalid_ohlc_rows == 1
    output_path = (
        silver
        / "dataset_type=recent_trade_snapshot_1m_observed"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=tick"
        / "year=2026"
        / "month=2026-06"
        / "BTC-2026-06.parquet"
    )
    output = pl.read_parquet(output_path).sort("instrument_type")
    assert output.columns == SILVER_RECENT_TRADE_SNAPSHOT_OBSERVED_COLUMNS
    assert output["snapshot_derived"].to_list() == [True, True]
    assert output["trade_id_is_source"].to_list() == [False, True]
    assert output["trade_id"].to_list() == [None, "known-1"]
    assert output["deduplication_key"][0].startswith("fallback:")
    assert output["price"].to_list() == [0.1, 101.0]
    assert output["expiry"].to_list()[0].isoformat() == "2026-06-19"
    assert output["strike"].to_list() == [65000.0, None]
    assert output["option_type"].to_list() == ["C", None]
    reconciliation = json.loads(output_path.with_suffix(".reconciliation.json").read_text(encoding="utf-8"))
    assert reconciliation["coverage_type"] == "snapshot_derived_not_full_history"
    assert reconciliation["source_trade_id_rows"] == 1
    assert reconciliation["fallback_key_rows"] == 1
    assert reconciliation["overlapping_trade_ids"] == 1
    assert reconciliation["field_mismatch_counts"]["price"] == 1
