"""Tests for read-only dataset inventory reporting."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl

from api.commands.inventory import run_dataset_inventory
from application.services.dataset_inventory import (
    build_dataset_inventory,
    inventory_to_json,
    inventory_to_markdown,
)


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def test_inventory_reports_partition_dates_and_mixed_series_lifetimes(tmp_path: Path) -> None:
    """Missing days are calculated per series between that series' own first and last date."""

    bronze = tmp_path / "bronze"
    _write_parquet(
        bronze
        / "dataset_type=spot_ohlcv"
        / "exchange=deribit"
        / "instrument_type=spot_ohlcv"
        / "symbol=BTC"
        / "timeframe=1m"
        / "date=2026-01-01"
        / "data.parquet",
        [{"open_time": datetime(2026, 1, 1), "symbol": "BTC", "close_price": 1.0}],
    )
    _write_parquet(
        bronze
        / "dataset_type=spot_ohlcv"
        / "exchange=deribit"
        / "instrument_type=spot_ohlcv"
        / "symbol=BTC"
        / "timeframe=1m"
        / "date=2026-01-03"
        / "data.parquet",
        [{"open_time": datetime(2026, 1, 3), "symbol": "BTC", "close_price": 1.0}],
    )
    _write_parquet(
        bronze
        / "dataset_type=spot_ohlcv"
        / "exchange=deribit"
        / "instrument_type=spot_ohlcv"
        / "symbol=ETH"
        / "timeframe=1m"
        / "date=2026-01-10"
        / "data.parquet",
        [{"open_time": datetime(2026, 1, 10), "symbol": "ETH", "close_price": 1.0}],
    )

    rows = build_dataset_inventory(bronze_root=bronze, silver_root=tmp_path / "silver", gold_root=tmp_path / "gold")
    spot = next(row for row in rows if row.layer == "bronze" and row.dataset == "spot_ohlcv")

    assert spot.file_count == 3
    assert spot.row_count == 3
    assert spot.start_date == "2026-01-01"
    assert spot.end_date == "2026-01-10"
    assert spot.expected_days == 4
    assert spot.observed_days == 3
    assert spot.missing_days == 1
    assert spot.per_series_missing_days == ("BTC=1", "ETH=0")


def test_inventory_reports_legacy_perp_as_canonical_perps_ohlcv(tmp_path: Path) -> None:
    """Legacy Silver dataset_type=perp remains visible as canonical perps_ohlcv work."""

    silver = tmp_path / "silver"
    parquet_path = (
        silver
        / "dataset_type=perp"
        / "exchange=deribit"
        / "symbol=BTC"
        / "year=2026"
        / "month=2026-01"
        / "data.parquet"
    )
    _write_parquet(
        parquet_path,
        [{"open_time": datetime(2026, 1, 1), "symbol": "BTC", "close_price": 1.0}],
    )

    rows = build_dataset_inventory(bronze_root=tmp_path / "bronze", silver_root=silver, gold_root=tmp_path / "gold")
    perps = next(row for row in rows if row.layer == "silver" and row.dataset == "perps_ohlcv")

    assert perps.state == "legacy_artifact"
    assert perps.physical_dataset == "perp"
    assert perps.file_count == 1
    assert perps.missing_days == 0


def test_inventory_renders_stable_json_and_markdown(tmp_path: Path) -> None:
    """Rendered reports should be deterministic and include absent contracted datasets."""

    rows = build_dataset_inventory(
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
    )

    assert inventory_to_json(rows) == inventory_to_json(rows)
    markdown = inventory_to_markdown(rows)
    assert "`iv_rv_1m_feature`" in markdown
    assert "`gold.market.regime_features.m1`" in markdown


def test_inventory_command_writes_explicit_output_only(tmp_path: Path) -> None:
    """Command writes a report only when the caller provides an output path."""

    output = tmp_path / "report" / "inventory.md"
    args = type(
        "Args",
        (),
        {
            "bronze_root": str(tmp_path / "bronze"),
            "silver_root": str(tmp_path / "silver"),
            "gold_root": str(tmp_path / "gold"),
            "output": str(output),
            "format": "markdown",
            "no_json_output": True,
        },
    )()

    run_dataset_inventory(args=args, logger=_NullLogger())

    assert output.exists()
    assert "`spot_ohlcv`" in output.read_text(encoding="utf-8")


class _NullLogger:
    def info(self, *_args: object, **_kwargs: object) -> None:
        """Accept command logging without writing a real logfile."""
