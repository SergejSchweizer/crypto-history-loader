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


@pytest.mark.parametrize(
    ("column_name", "expected_source"),
    [
        ("iv_30d_annualized_pct", "volatility_index_1m_feature"),
        ("iv_available", "iv_rv_1m_feature"),
        ("rv_1h", "realized_volatility_1m_feature"),
        ("perps_l2_spread", "perps_l2_1m_feature"),
        ("options_l2_spread", "options_l2_1m_feature"),
        ("options_surface_skew", "options_surface_1m_feature"),
        ("index_price_close", "index_price_1m_feature"),
        ("futures_summary_basis", "futures_summary_1m_feature"),
        ("strategy_signal", "gold_strategy_features"),
        ("target_iv", "gold_prediction_targets"),
        ("historical_volatility_value", "historical_volatility_observed"),
        ("as_of", "gold_live_lineage"),
    ],
)
def test_feature_source_dataset_maps_extended_gold_features(column_name: str, expected_source: str) -> None:
    """Manifest lineage must distinguish every extended Gold feature family."""

    assert feature_source_dataset(column_name) == expected_source


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


def test_feature_metadata_includes_numeric_statistics_and_utc_timestamps() -> None:
    """Numeric manifests include statistics and preserve explicit UTC timestamps."""

    frame = pl.DataFrame(
        {
            "timestamp_m1": [
                datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
                datetime(2026, 1, 1, 12, 1, tzinfo=UTC),
            ],
            "funding_rate": [1.0, 3.0],
            "label_state": ["calm", None],
        }
    )

    metadata = feature_metadata(pl, frame, "deribit")

    assert metadata["funding_rate"].get("count") == 2
    assert metadata["funding_rate"].get("mean") == 2.0
    assert metadata["funding_rate"]["time_range"]["min_timestamp"] == "2026-01-01T12:00:00Z"
    assert metadata["label_state"]["null_count"] == 1
    assert "mean" not in metadata["label_state"]
