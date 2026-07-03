"""Tests for Bronze lake read helper exports."""

from __future__ import annotations

from ingestion import lake, lake_reads


def test_lake_read_helpers_remain_available_from_compatibility_module() -> None:
    """Existing ``ingestion.lake`` read imports must continue to point at the adapter."""

    assert lake.load_spot_ohlcv_candles_from_lake is lake_reads.load_spot_ohlcv_candles_from_lake
    assert lake.load_open_interest_from_lake is lake_reads.load_open_interest_from_lake
    assert lake.load_funding_from_lake is lake_reads.load_funding_from_lake
