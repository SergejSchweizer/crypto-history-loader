"""Dataset labels shared by parquet lake adapters."""

from __future__ import annotations

OPEN_INTEREST_DATASET_TYPE = "open_interest"


def ohlcv_dataset_type_for_market(market: str) -> str:
    """Return dataset_type label used for OHLCV parquet storage by market.

    Args:
        market: OHLCV market family.

    Returns:
        Bronze dataset_type partition label.

    Raises:
        ValueError: Market is not an OHLCV dataset family.
    """

    if market == "spot_ohlcv":
        return "spot_ohlcv"
    if market == "perp":
        return "perps_ohlcv"
    raise ValueError(f"Unsupported OHLCV market '{market}'")


def bronze_trade_dataset_type_for_market(market: str) -> str:
    """Return the bronze dataset_type label used for trade parquet storage by market."""

    if market == "option":
        return "options_trades"
    if market == "perp":
        return "perps_trades"
    raise ValueError(f"Unsupported trade market '{market}'")
