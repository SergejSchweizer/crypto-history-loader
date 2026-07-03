"""Dataset labels shared by parquet lake adapters."""

from __future__ import annotations

OI_DATASET_TYPE = "oi"


def ohlcv_dataset_type_for_market(market: str) -> str:
    """Return dataset_type label used for OHLCV parquet storage by market.

    Args:
        market: OHLCV market family.

    Returns:
        Bronze dataset_type partition label.

    Raises:
        ValueError: Market is not an OHLCV dataset family.
    """

    if market == "spot":
        return "spot"
    if market == "perp":
        return "peprs_ohlcv"
    raise ValueError(f"Unsupported OHLCV market '{market}'")
