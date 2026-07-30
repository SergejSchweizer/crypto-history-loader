"""Tests for plotting-independent feature metadata helpers."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ingestion.feature_metadata import feature_hash, feature_metadata, feature_source_dataset

pl = pytest.importorskip("polars")


def test_feature_source_dataset_maps_known_prefixes() -> None:
    """Feature source inference should stay stable for manifest metadata."""

    assert feature_source_dataset("spot_ohlcv_close_price") == "spot_ohlcv_1m"
    assert feature_source_dataset("perp_close_price") == "perps_ohlcv_1m"
    assert feature_source_dataset("open_interest_observation_lag_sec") == "open_interest_1m_feature"
    assert feature_source_dataset("funding_rate_last_known") == "funding_1m_feature"
    assert feature_source_dataset("perps_trades_open_price") == "perps_trades_1m_feature"
    assert feature_source_dataset("options_trades_open_price") == "options_trades_1m_feature"
    assert feature_source_dataset("historical_prediction_perps_rv_1h") == "historical_prediction_1m_feature"
    assert feature_source_dataset("volatility_index_data_value") == "volatility_index_data_observed"
    assert feature_source_dataset("custom_col") == "gold_merged"


def test_feature_hash_preserves_column_order() -> None:
    """Column hashes should change when feature ordering changes."""

    assert feature_hash(["a", "b"]) == feature_hash(["a", "b"])
    assert feature_hash(["a", "b"]) != feature_hash(["b", "a"])


def test_feature_metadata_reports_missing_values_and_time_range() -> None:
    """Feature metadata should not require plot generation dependencies."""

    frame = pl.DataFrame(
        {
            "timestamp_m1": [
                datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
                datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
            ],
            "spot_ohlcv_close_price": [100.0, None],
        }
    )

    metadata = feature_metadata(pl, frame, "deribit")

    assert metadata["spot_ohlcv_close_price"]["missing_values"] == 1
    assert metadata["spot_ohlcv_close_price"]["source_dataset"] == "spot_ohlcv_1m"
    assert metadata["spot_ohlcv_close_price"]["source_exchange"] == "deribit"
    assert metadata["spot_ohlcv_close_price"]["time_range"]["min_timestamp"] == "2026-01-01T00:00:00Z"
    assert metadata["spot_ohlcv_close_price"]["time_range"]["max_timestamp"] == "2026-01-01T00:00:00Z"
