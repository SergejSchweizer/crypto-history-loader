"""Tests for L2 observed and minute-feature Silver builders."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from application.dataset_contracts import SILVER_L2_FEATURE_COLUMNS, SILVER_L2_OBSERVED_COLUMNS
from application.services import silver_l2
from application.services.silver_service import (
    build_options_l2_1m_feature_for_symbol,
    build_options_l2_observed_for_symbol,
    build_perps_l2_1m_feature_for_symbol,
    build_perps_l2_observed_for_symbol,
    discover_l2_symbols,
)

pl = pytest.importorskip("polars")


def _level(price: float, amount: float) -> dict[str, float]:
    return {"price": price, "amount": amount}


def _row(
    *,
    timestamp: datetime,
    bids: list[dict[str, float]],
    asks: list[dict[str, float]],
    quote_age_seconds: int = 0,
) -> dict[str, object]:
    return {
        "schema_version": "v1",
        "dataset_type": "perps_l2_snapshot_1m",
        "exchange": "deribit",
        "symbol": "BTC-PERPETUAL",
        "instrument_type": "perp",
        "event_time": timestamp,
        "ingested_at": timestamp + timedelta(seconds=quote_age_seconds),
        "run_id": "test",
        "source": "rest_order_book",
        "depth": 50,
        "bids": bids,
        "asks": asks,
    }


def _write_bronze(root: Path, rows: list[dict[str, object]]) -> None:
    target = (
        root
        / "dataset_type=perps_l2_snapshot_1m"
        / "exchange=deribit"
        / "instrument_type=perp"
        / "symbol=BTC-PERPETUAL"
        / "depth=50"
        / "source=rest_order_book"
        / "year=2026"
        / "month=05"
        / "date=2026-05-05"
        / "hour=12"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target)


def test_perps_l2_filters_malformed_books_and_uses_latest_snapshot_per_minute(
    tmp_path: Path,
) -> None:
    """Malformed books should be rejected while empty observed books remain explicit."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    t0 = datetime(2026, 5, 5, 12, 0, 10, tzinfo=UTC)
    rows = [
        _row(
            timestamp=t0,
            bids=[_level(99.9, 1.0), _level(99.8, 2.0)],
            asks=[_level(100.2, 1.0), _level(100.3, 2.0)],
        ),
        _row(
            timestamp=t0.replace(second=40),
            bids=[_level(100.0, 6.0), _level(99.95, 4.0)],
            asks=[_level(100.1, 3.0), _level(100.15, 4.0)],
        ),
        _row(
            timestamp=t0.replace(second=20),
            bids=[_level(99.0, 1.0), _level(100.0, 1.0)],
            asks=[_level(101.0, 1.0)],
        ),
        _row(
            timestamp=t0.replace(second=21),
            bids=[_level(100.0, -1.0)],
            asks=[_level(101.0, 1.0)],
        ),
        _row(
            timestamp=t0.replace(second=22),
            bids=[_level(101.0, 1.0)],
            asks=[_level(100.0, 1.0)],
        ),
        _row(timestamp=t0.replace(minute=1), bids=[], asks=[]),
        _row(
            timestamp=t0.replace(minute=2),
            bids=[_level(99.9, 1.0)],
            asks=[_level(100.1, 1.0)],
            quote_age_seconds=125,
        ),
    ]
    _write_bronze(bronze, rows)

    assert discover_l2_symbols(bronze_root=str(bronze), exchange="deribit") == ["BTC-PERPETUAL"]
    observed_report = build_perps_l2_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="btc-perpetual",
    )
    feature_report = build_perps_l2_1m_feature_for_symbol(
        silver_root=str(silver), exchange="deribit", symbol="BTC-PERPETUAL"
    )

    assert observed_report.rows_in == 7
    assert observed_report.rows_out == 4
    assert observed_report.invalid_ohlc_rows == 3
    assert feature_report.rows_in == 4
    assert feature_report.rows_out == 3
    assert feature_report.duplicates_removed == 1
    observed_path = (
        silver
        / "dataset_type=perps_l2_snapshot_1m_observed"
        / "exchange=deribit"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-PERPETUAL-2026-05.parquet"
    )
    feature_path = Path(str(observed_path).replace("perps_l2_snapshot_1m_observed", "perps_l2_1m_feature"))
    observed = pl.read_parquet(observed_path)
    feature = pl.read_parquet(feature_path).sort("timestamp_m1")
    assert observed.columns == SILVER_L2_OBSERVED_COLUMNS
    assert feature.columns == SILVER_L2_FEATURE_COLUMNS
    first = feature.row(0, named=True)
    second = feature.row(1, named=True)
    third = feature.row(2, named=True)
    assert first["best_bid_price"] == pytest.approx(100.0)
    assert first["best_ask_price"] == pytest.approx(100.1)
    assert first["mid_price"] == pytest.approx(100.05)
    assert first["spread"] == pytest.approx(0.1)
    assert first["top_of_book_imbalance"] == pytest.approx(1.0 / 3.0)
    assert first["bid_depth_10bps"] == pytest.approx(10.0)
    assert first["ask_depth_10bps"] == pytest.approx(7.0)
    assert first["bid_depth_50bps"] == pytest.approx(10.0)
    assert first["ask_depth_50bps"] == pytest.approx(7.0)
    assert first["quote_available"] is True
    assert first["quote_age_seconds"] == pytest.approx(0.0)
    assert first["stale_quote"] is False
    assert first["minutes_since_l2_observation"] == 0
    assert second["quote_available"] is False
    assert second["mid_price"] is None
    assert second["bid_depth_10bps"] is None
    assert third["quote_available"] is True
    assert third["quote_age_seconds"] == pytest.approx(125.0)
    assert third["stale_quote"] is True
    assert third["minutes_since_l2_observation"] == 2


def _option_row(
    *,
    timestamp: datetime,
    instrument_name: str,
    bids: list[dict[str, float]],
    asks: list[dict[str, float]],
    quote_age_seconds: int,
) -> dict[str, object]:
    return {
        "schema_version": "v1",
        "dataset_type": "options_l2_snapshot_1m",
        "exchange": "deribit",
        "symbol": instrument_name,
        "currency": "BTC",
        "instrument_name": instrument_name,
        "instrument_type": "option",
        "event_time": timestamp,
        "snapshot_time": timestamp,
        "ingested_at": timestamp + timedelta(seconds=quote_age_seconds),
        "run_id": "test",
        "source": "rest_order_book",
        "depth": 50,
        "bids": bids,
        "asks": asks,
    }


def test_options_l2_parses_contracts_and_exposes_liquidity_filter_keys(tmp_path: Path) -> None:
    """Option L2 rows should stay contract-level and expose freshness for surface joins."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    t0 = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)
    rows = [
        _option_row(
            timestamp=t0,
            instrument_name="BTC-10JUL26-60000-C",
            bids=[_level(0.10, 2.0)],
            asks=[_level(0.11, 3.0)],
            quote_age_seconds=10,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-10JUL26-60000-P",
            bids=[_level(0.08, 2.0)],
            asks=[_level(0.09, 3.0)],
            quote_age_seconds=120,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-17JUL26-65000-C",
            bids=[],
            asks=[],
            quote_age_seconds=10,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="BTC-17JUL26-65000-P",
            bids=[_level(0.12, 1.0)],
            asks=[_level(0.11, 1.0)],
            quote_age_seconds=10,
        ),
        _option_row(
            timestamp=t0,
            instrument_name="NOT-A-CONTRACT",
            bids=[_level(0.10, 1.0)],
            asks=[_level(0.11, 1.0)],
            quote_age_seconds=10,
        ),
    ]
    target = (
        bronze
        / "dataset_type=options_l2_snapshot_1m"
        / "exchange=deribit"
        / "instrument_type=option"
        / "symbol=BTC"
        / "depth=50"
        / "source=rest_order_book"
        / "year=2026"
        / "month=07"
        / "date=2026-07-03"
        / "hour=12"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target)

    observed_report = build_options_l2_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )
    feature_report = build_options_l2_1m_feature_for_symbol(silver_root=str(silver), exchange="deribit", symbol="BTC")

    assert observed_report.rows_in == 5
    assert observed_report.rows_out == 3
    assert observed_report.invalid_ohlc_rows == 2
    assert feature_report.rows_out == 3
    feature_path = (
        silver
        / "dataset_type=options_l2_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-07"
        / "BTC-2026-07.parquet"
    )
    feature = pl.read_parquet(feature_path).sort("instrument_name")
    assert feature.columns == SILVER_L2_FEATURE_COLUMNS
    assert feature["underlying"].unique().to_list() == ["BTC"]
    assert feature["strike"].to_list() == [60000.0, 60000.0, 65000.0]
    assert feature["option_type"].to_list() == ["C", "P", "C"]
    assert feature["quote_age_seconds"].to_list() == [10.0, 120.0, 10.0]
    assert feature["stale_quote"].to_list() == [False, True, False]
    assert feature["quote_available"].to_list() == [True, True, False]
    mid_prices = feature["mid_price"].to_list()
    assert mid_prices[:2] == pytest.approx([0.105, 0.085])
    assert mid_prices[2] is None
    assert feature["bid_depth_10bps"].to_list() == [0.0, 0.0, None]
    quality = feature.filter(pl.col("quote_available") & ~pl.col("stale_quote"))
    assert quality["instrument_name"].to_list() == ["BTC-10JUL26-60000-C"]


def test_l2_builders_return_empty_reports_when_their_input_partition_is_missing(tmp_path: Path) -> None:
    """Absent Bronze and observed partitions should produce explicit empty reports."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"

    observed_report = build_perps_l2_observed_for_symbol(
        bronze_root=str(bronze), silver_root=str(silver), exchange="deribit", symbol="btc-perpetual"
    )
    feature_report = build_perps_l2_1m_feature_for_symbol(
        silver_root=str(silver), exchange="deribit", symbol="btc-perpetual"
    )

    for report in (observed_report, feature_report):
        assert report.rows_in == 0
        assert report.rows_out == 0
        assert report.months_processed == []
        assert report.period_start is None
        assert report.period_end is None
        assert report.min_timestamp is None
        assert report.max_timestamp is None


def test_perps_l2_builders_reuse_matching_partition_manifests(tmp_path: Path) -> None:
    """Unchanged input partitions should be cache hits without re-reading their rows."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    timestamp = datetime(2026, 5, 5, 12, 0, tzinfo=UTC)
    _write_bronze(
        bronze,
        [
            _row(
                timestamp=timestamp,
                bids=[_level(99.9, 2.0)],
                asks=[_level(100.1, 3.0)],
            )
        ],
    )

    first_observed = build_perps_l2_observed_for_symbol(
        bronze_root=str(bronze), silver_root=str(silver), exchange="deribit", symbol="BTC-PERPETUAL"
    )
    cached_observed = build_perps_l2_observed_for_symbol(
        bronze_root=str(bronze), silver_root=str(silver), exchange="deribit", symbol="BTC-PERPETUAL"
    )
    first_feature = build_perps_l2_1m_feature_for_symbol(
        silver_root=str(silver), exchange="deribit", symbol="BTC-PERPETUAL"
    )
    cached_feature = build_perps_l2_1m_feature_for_symbol(
        silver_root=str(silver), exchange="deribit", symbol="BTC-PERPETUAL"
    )

    assert (first_observed.rows_in, first_observed.rows_out) == (1, 1)
    assert (cached_observed.rows_in, cached_observed.rows_out) == (0, 1)
    assert (first_feature.rows_in, first_feature.rows_out) == (1, 1)
    assert (cached_feature.rows_in, cached_feature.rows_out) == (0, 1)


def test_l2_feature_keeps_zero_sized_quotes_and_uses_strict_staleness_boundary(tmp_path: Path) -> None:
    """A valid zero-sized top book remains observable but has no imbalance or stale flag at 60 seconds."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    timestamp = datetime(2026, 5, 5, 12, 0, tzinfo=UTC)
    _write_bronze(
        bronze,
        [
            _row(
                timestamp=timestamp,
                bids=[_level(99.9, 0.0)],
                asks=[_level(100.1, 0.0)],
                quote_age_seconds=60,
            )
        ],
    )

    build_perps_l2_observed_for_symbol(
        bronze_root=str(bronze), silver_root=str(silver), exchange="deribit", symbol="BTC-PERPETUAL"
    )
    build_perps_l2_1m_feature_for_symbol(silver_root=str(silver), exchange="deribit", symbol="BTC-PERPETUAL")

    feature_path = (
        silver
        / "dataset_type=perps_l2_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-PERPETUAL-2026-05.parquet"
    )
    feature = pl.read_parquet(feature_path).row(0, named=True)
    assert feature["quote_available"] is True
    assert feature["top_of_book_imbalance"] is None
    assert feature["bid_depth_10bps"] == pytest.approx(0.0)
    assert feature["ask_depth_10bps"] == pytest.approx(0.0)
    assert feature["quote_age_seconds"] == pytest.approx(60.0)
    assert feature["stale_quote"] is False
    assert feature["minutes_since_l2_observation"] == 1


@pytest.mark.parametrize(
    ("row", "side", "bps", "expected"),
    [
        ({"mid_price": 100.0, "bids": [_level(99.9, 2.0), _level(99.5, 3.0)]}, "bids", 10, 2.0),
        ({"mid_price": 100.0, "asks": [_level(100.1, 2.0), _level(100.5, 3.0)]}, "asks", 10, 2.0),
        ({"mid_price": "100", "bids": []}, "bids", 10, None),
        ({"mid_price": 100.0, "asks": "not-a-book"}, "asks", 10, None),
        ({"mid_price": 100.0, "bids": ["bad", {"price": 99.95}, {"amount": 1.0}]}, "bids", 10, 0.0),
    ],
)
def test_depth_within_bps_handles_inclusive_boundaries_and_malformed_levels(
    row: dict[str, object], side: str, bps: int, expected: float | None
) -> None:
    """Depth includes the exact band edge and ignores malformed levels without failing a feature build."""

    result = silver_l2._depth_within_bps(row, side=side, bps=bps)

    assert result == pytest.approx(expected) if expected is not None else result is None


def test_collect_l2_files_falls_back_only_for_schema_errors() -> None:
    """Only schema mismatches should use per-file diagonal reads."""

    class SchemaError(Exception):
        pass

    class Scan:
        def __init__(self, error: Exception) -> None:
            self.error = error

        def collect(self) -> object:
            raise self.error

    class PolarsStub:
        def __init__(self, error: Exception) -> None:
            self.error = error
            self.concatenated: list[object] | None = None

        def scan_parquet(self, paths: list[str]) -> Scan:
            assert paths == ["first.parquet", "second.parquet"]
            return Scan(self.error)

        def read_parquet(self, path: str) -> str:
            return f"read:{path}"

        def concat(self, frames: list[object], *, how: str) -> object:
            assert how == "diagonal_relaxed"
            self.concatenated = frames
            return "fallback-frame"

    schema_stub = PolarsStub(SchemaError("incompatible schema"))
    assert silver_l2._collect_files(schema_stub, ["first.parquet", "second.parquet"]) == "fallback-frame"
    assert schema_stub.concatenated == ["read:first.parquet", "read:second.parquet"]

    with pytest.raises(RuntimeError, match="network failure"):
        silver_l2._collect_files(PolarsStub(RuntimeError("network failure")), ["first.parquet", "second.parquet"])
