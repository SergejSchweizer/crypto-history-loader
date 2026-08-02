"""Tests for Bronze dataset labels."""

from __future__ import annotations

import pytest

from ingestion.lake_datasets import bronze_trade_dataset_type_for_market, ohlcv_dataset_type_for_market


@pytest.mark.parametrize(
    ("market", "expected"),
    [("spot_ohlcv", "spot_ohlcv"), ("perp", "perps_ohlcv")],
)
def test_ohlcv_dataset_type_for_market(market: str, expected: str) -> None:
    """OHLCV labels map the physical perpetual family to the canonical dataset id."""

    assert ohlcv_dataset_type_for_market(market) == expected


@pytest.mark.parametrize(
    ("market", "expected"),
    [("perp", "perps_trades"), ("option", "options_trades")],
)
def test_trade_dataset_type_for_market(market: str, expected: str) -> None:
    """Trade labels are explicit for perpetual and option executions."""

    assert bronze_trade_dataset_type_for_market(market) == expected


def test_dataset_label_helpers_reject_unsupported_markets() -> None:
    """Unsupported market families cannot silently enter the Bronze lake."""

    with pytest.raises(ValueError, match="Unsupported OHLCV market"):
        ohlcv_dataset_type_for_market("option")
    with pytest.raises(ValueError, match="Unsupported trade market"):
        bronze_trade_dataset_type_for_market("spot_ohlcv")
