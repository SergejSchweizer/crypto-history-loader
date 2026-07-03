"""Tests for Gold manifest audit helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.services.gold_audit import missing_value_audit, source_dataset_summary, time_span_coverage

pl = pytest.importorskip("polars")


def test_time_span_coverage_reports_minute_span_and_gaps() -> None:
    """Gold coverage audit should describe row coverage across the timestamp span."""

    frame = pl.DataFrame(
        {
            "timestamp_m1": [
                datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
                datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            ]
        }
    )

    min_ts, max_ts, expected_minutes, missing_minutes, coverage_ratio = time_span_coverage(pl, frame)

    assert min_ts == datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    assert max_ts == datetime(2026, 5, 1, 0, 2, tzinfo=UTC)
    assert expected_minutes == 3
    assert missing_minutes == 1
    assert coverage_ratio == pytest.approx(2 / 3)


def test_source_dataset_summary_includes_symbols_and_l2_artifact() -> None:
    """Gold source summaries should preserve legacy manifest keys and L2 artifact names."""

    raw_by_dataset = {
        "spot_ohlcv": pl.DataFrame({"symbol": ["BTC", "BTC"], "open_time": [1, 2]}),
        "gold_l2_m1": pl.DataFrame({"symbol": ["BTC"], "l2_coverage_ratio": [1.0]}),
    }

    summary = source_dataset_summary(pl, raw_by_dataset, Path("BTC_L2_hash.parquet"))

    assert summary["spot_ohlcv_1m"]["source_symbols"] == ["BTC"]
    assert summary["spot_ohlcv_1m"]["rows"] == 2
    assert summary["gold_l2_m1"]["source_artifact"] == "BTC_L2_hash.parquet"


def test_missing_value_audit_counts_by_column_and_total() -> None:
    """Gold missing-value audit should count nulls per feature and globally."""

    frame = pl.DataFrame({"a": [1.0, None], "b": [None, None]})

    missing_by_column, missing_total = missing_value_audit(pl, frame)

    assert missing_by_column == {"a": 1, "b": 2}
    assert missing_total == 3
