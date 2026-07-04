"""Typed Silver and Gold dataset contracts used by transformation services."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

MissingDataPolicy = Literal["drop_invalid", "observed_only", "forward_fill", "asof_join", "none"]
TimestampSemantics = Literal["event_open_time", "observed_timestamp", "minute_grid", "trade_time"]


@dataclass(frozen=True)
class SilverDatasetContract:
    """Schema and time-alignment contract for one Silver output dataset."""

    dataset_type: str
    timeframe: str
    timestamp_column: str
    timestamp_semantics: TimestampSemantics
    missing_data_policy: MissingDataPolicy
    output_columns: tuple[str, ...]


@dataclass(frozen=True)
class GoldSourceRequirement:
    """One Silver input required to build a Gold dataset."""

    dataset_type: str
    timeframe: str

    def as_tuple(self) -> tuple[str, str]:
        """Return the legacy `(dataset_type, timeframe)` shape used by existing callers."""

        return (self.dataset_type, self.timeframe)


@dataclass(frozen=True)
class GoldDatasetContract:
    """Input and side-effect contract for one model-ready Gold dataset."""

    dataset_id: str
    requirements: tuple[GoldSourceRequirement, ...]
    include_l2: bool
    timestamp_column: str = "timestamp_m1"
    timestamp_semantics: TimestampSemantics = "minute_grid"
    missing_data_policy: MissingDataPolicy = "asof_join"

    def legacy_spec(self) -> dict[str, object]:
        """Return the previous dict shape to keep public service constants compatible."""

        return {
            "requirements": [requirement.as_tuple() for requirement in self.requirements],
            "include_l2": self.include_l2,
        }


SILVER_OHLCV_COLUMNS = [
    "schema_version",
    "dataset_type",
    "exchange",
    "symbol",
    "instrument_type",
    "event_time",
    "ingested_at",
    "run_id",
    "source_endpoint",
    "open_time",
    "close_time",
    "timeframe",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_volume",
    "trade_count",
    "origin_payload",
]
SILVER_FUNDING_OBSERVED_COLUMNS = [
    "funding_time",
    "exchange",
    "symbol",
    "base_asset",
    "instrument_type",
    "funding_rate",
    "funding_interval_hours",
    "ingested_at_min",
    "ingested_at_max",
    "source_row_count",
    "silver_built_at",
    "data_quality_status",
]
SILVER_FUNDING_FEATURE_COLUMNS = [
    "timestamp",
    "exchange",
    "symbol",
    "funding_rate_last_known",
    "funding_observed_at",
    "minutes_since_funding",
    "is_funding_observation_minute",
    "funding_data_available",
]
SILVER_OPEN_INTEREST_OBSERVED_COLUMNS = [
    "timestamp",
    "exchange",
    "symbol",
    "open_interest",
    "open_interest_source_timestamp",
    "ingested_at",
    "source_endpoint",
]
SILVER_OPEN_INTEREST_M1_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "open_interest",
    "open_interest_is_observed",
    "open_interest_is_ffill",
    "minutes_since_open_interest_observation",
    "open_interest_observation_lag_sec",
    "open_interest_source_timestamp",
]
SILVER_TRADES_M1_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "instrument_type",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_volume",
    "trade_count",
    "buy_volume",
    "sell_volume",
    "buy_trade_count",
    "sell_trade_count",
    "buy_volume_share",
]
SILVER_TRADES_OBSERVED_COLUMNS = [
    "trade_time",
    "exchange",
    "symbol",
    "instrument_type",
    "trade_id",
    "price",
    "quantity",
    "side",
]
SILVER_VOLATILITY_OBSERVED_COLUMNS = [
    "timestamp",
    "exchange",
    "symbol",
    "instrument_type",
    "dataset_type",
    "volatility_value",
    "volatility_source_timestamp",
    "ingested_at",
    "source_endpoint",
]


SILVER_DATASET_CONTRACTS: dict[str, SilverDatasetContract] = {
    "spot_ohlcv": SilverDatasetContract(
        dataset_type="spot_ohlcv",
        timeframe="1m",
        timestamp_column="open_time",
        timestamp_semantics="event_open_time",
        missing_data_policy="drop_invalid",
        output_columns=tuple(SILVER_OHLCV_COLUMNS),
    ),
    "perps_ohlcv": SilverDatasetContract(
        dataset_type="perps_ohlcv",
        timeframe="1m",
        timestamp_column="open_time",
        timestamp_semantics="event_open_time",
        missing_data_policy="drop_invalid",
        output_columns=tuple(SILVER_OHLCV_COLUMNS),
    ),
    "funding_observed": SilverDatasetContract(
        dataset_type="funding_observed",
        timeframe="8h",
        timestamp_column="funding_time",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_FUNDING_OBSERVED_COLUMNS),
    ),
    "funding_1m_feature": SilverDatasetContract(
        dataset_type="funding_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="minute_grid",
        missing_data_policy="forward_fill",
        output_columns=tuple(SILVER_FUNDING_FEATURE_COLUMNS),
    ),
    "open_interest_observed": SilverDatasetContract(
        dataset_type="open_interest_observed",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_OPEN_INTEREST_OBSERVED_COLUMNS),
    ),
    "open_interest_1m_feature": SilverDatasetContract(
        dataset_type="open_interest_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="forward_fill",
        output_columns=tuple(SILVER_OPEN_INTEREST_M1_FEATURE_COLUMNS),
    ),
    "perps_trades_observed": SilverDatasetContract(
        dataset_type="perps_trades_observed",
        timeframe="tick",
        timestamp_column="trade_time",
        timestamp_semantics="trade_time",
        missing_data_policy="drop_invalid",
        output_columns=tuple(SILVER_TRADES_OBSERVED_COLUMNS),
    ),
    "options_trades_observed": SilverDatasetContract(
        dataset_type="options_trades_observed",
        timeframe="tick",
        timestamp_column="trade_time",
        timestamp_semantics="trade_time",
        missing_data_policy="drop_invalid",
        output_columns=tuple(SILVER_TRADES_OBSERVED_COLUMNS),
    ),
    "perps_trades_1m_feature": SilverDatasetContract(
        dataset_type="perps_trades_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_TRADES_M1_FEATURE_COLUMNS),
    ),
    "options_trades_1m_feature": SilverDatasetContract(
        dataset_type="options_trades_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_TRADES_M1_FEATURE_COLUMNS),
    ),
    "volatility_index_data_observed": SilverDatasetContract(
        dataset_type="volatility_index_data_observed",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_VOLATILITY_OBSERVED_COLUMNS),
    ),
}


FULL_MARKET_GOLD_REQUIREMENTS = (
    GoldSourceRequirement("spot_ohlcv", "1m"),
    GoldSourceRequirement("perps_ohlcv", "1m"),
    GoldSourceRequirement("open_interest_1m_feature", "1m"),
    GoldSourceRequirement("funding_1m_feature", "1m"),
    GoldSourceRequirement("perps_trades_1m_feature", "1m"),
    GoldSourceRequirement("options_trades_1m_feature", "1m"),
    GoldSourceRequirement("volatility_index_data_observed", "1m"),
)

GOLD_DATASET_CONTRACTS: dict[str, GoldDatasetContract] = {
    "gold.market.perps_trades.m1": GoldDatasetContract(
        dataset_id="gold.market.perps_trades.m1",
        requirements=(GoldSourceRequirement("perps_trades_1m_feature", "1m"),),
        include_l2=False,
    ),
    "gold.market.options_trades.m1": GoldDatasetContract(
        dataset_id="gold.market.options_trades.m1",
        requirements=(GoldSourceRequirement("options_trades_1m_feature", "1m"),),
        include_l2=False,
    ),
    "gold.market.core.m1": GoldDatasetContract(
        dataset_id="gold.market.core.m1",
        requirements=(GoldSourceRequirement("spot_ohlcv", "1m"), GoldSourceRequirement("perps_ohlcv", "1m")),
        include_l2=False,
    ),
    "gold.market.core_funding.m1": GoldDatasetContract(
        dataset_id="gold.market.core_funding.m1",
        requirements=(
            GoldSourceRequirement("spot_ohlcv", "1m"),
            GoldSourceRequirement("perps_ohlcv", "1m"),
            GoldSourceRequirement("funding_1m_feature", "1m"),
        ),
        include_l2=False,
    ),
    "gold.market.full.m1": GoldDatasetContract(
        dataset_id="gold.market.full.m1",
        requirements=FULL_MARKET_GOLD_REQUIREMENTS,
        include_l2=False,
    ),
    "gold.hybrid.full_l2.m1": GoldDatasetContract(
        dataset_id="gold.hybrid.full_l2.m1",
        requirements=FULL_MARKET_GOLD_REQUIREMENTS,
        include_l2=True,
    ),
}


def silver_dataset_contract(dataset_type: str) -> SilverDatasetContract:
    """Return the explicit Silver contract for one output dataset type."""

    try:
        return SILVER_DATASET_CONTRACTS[dataset_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported silver dataset_type: {dataset_type}") from exc


def gold_dataset_contract(dataset_id: str) -> GoldDatasetContract:
    """Return the explicit Gold contract for one model-ready dataset ID."""

    try:
        return GOLD_DATASET_CONTRACTS[dataset_id]
    except KeyError as exc:
        raise ValueError(f"Unsupported dataset_id: {dataset_id}") from exc
