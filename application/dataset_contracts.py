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
    "volatility_open",
    "volatility_high",
    "volatility_low",
    "volatility_close",
    "volatility_source_timestamp",
    "ingested_at",
    "source_endpoint",
]
SILVER_VOLATILITY_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "iv_open",
    "iv_high",
    "iv_low",
    "iv_close",
    "iv_range",
    "iv_return_1m",
    "iv_change_5m",
    "iv_change_15m",
    "iv_change_1h",
    "iv_zscore_1d",
    "iv_zscore_7d",
    "iv_percentile_30d",
    "iv_source_dataset",
    "iv_source_timestamp",
    "minutes_since_iv_observation",
    "iv_data_available",
]
SILVER_INDEX_PRICE_OBSERVED_COLUMNS = [
    "timestamp",
    "exchange",
    "symbol",
    "index_name",
    "index_price",
    "index_price_source_timestamp",
    "ingested_at",
    "source_endpoint",
]
SILVER_INDEX_PRICE_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "index_price",
    "index_price_is_observed",
    "index_price_source_timestamp",
    "minutes_since_index_price_observation",
]
SILVER_FUTURES_SUMMARY_OBSERVED_COLUMNS = [
    "timestamp",
    "exchange",
    "symbol",
    "instrument_type",
    "mark_price",
    "index_price",
    "open_interest",
    "volume",
    "turnover",
    "funding_rate",
    "ingested_at",
    "source_endpoint",
]
SILVER_FUTURES_SUMMARY_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "instrument_type",
    "mark_price",
    "index_price",
    "mark_index_spread",
    "mark_index_ratio",
    "open_interest",
    "volume",
    "turnover",
    "funding_rate",
    "summary_is_observed",
    "minutes_since_summary_observation",
]
SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS = [
    "timestamp",
    "exchange",
    "symbol",
    "instrument_name",
    "underlying",
    "expiry",
    "strike",
    "underlying_price",
    "index_price",
    "option_type",
    "mark_price",
    "bid_price",
    "ask_price",
    "implied_volatility",
    "delta",
    "gamma",
    "vega",
    "theta",
    "open_interest",
    "volume",
    "ingested_at",
    "source_endpoint",
]
SILVER_L2_OBSERVED_COLUMNS = [
    "timestamp",
    "exchange",
    "symbol",
    "instrument_type",
    "instrument_name",
    "underlying",
    "expiry",
    "strike",
    "option_type",
    "best_bid_price",
    "best_bid_size",
    "best_ask_price",
    "best_ask_size",
    "bids",
    "asks",
    "ingested_at",
    "source_endpoint",
]
SILVER_L2_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "instrument_type",
    "instrument_name",
    "underlying",
    "expiry",
    "strike",
    "option_type",
    "best_bid_price",
    "best_ask_price",
    "mid_price",
    "spread",
    "top_bid_size",
    "top_ask_size",
    "top_of_book_imbalance",
    "bid_depth_10bps",
    "ask_depth_10bps",
    "bid_depth_50bps",
    "ask_depth_50bps",
    "quote_available",
    "quote_age_seconds",
    "stale_quote",
    "minutes_since_l2_observation",
]
SILVER_RECENT_TRADE_SNAPSHOT_OBSERVED_COLUMNS = [
    "trade_time",
    "exchange",
    "symbol",
    "instrument_type",
    "instrument_name",
    "underlying",
    "expiry",
    "strike",
    "option_type",
    "trade_id",
    "deduplication_key",
    "trade_id_is_source",
    "price",
    "quantity",
    "side",
    "snapshot_timestamp",
    "snapshot_derived",
    "ingested_at",
    "source_endpoint",
]
SILVER_INSTRUMENT_METADATA_OBSERVED_COLUMNS = [
    "snapshot_date",
    "exchange",
    "instrument_name",
    "symbol",
    "instrument_type",
    "base_currency",
    "quote_currency",
    "settlement_currency",
    "expiry",
    "strike",
    "option_type",
    "tick_size",
    "contract_size",
    "min_trade_amount",
    "creation_timestamp",
    "is_active",
    "is_listed",
    "listing_state",
    "ingested_at",
    "source_endpoint",
]
SILVER_HISTORICAL_VOLATILITY_OBSERVED_COLUMNS = [
    "timestamp",
    "exchange",
    "symbol",
    "historical_volatility",
    "historical_volatility_source_timestamp",
    "ingested_at",
    "source_endpoint",
]
SILVER_OPTION_SURFACE_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "atm_iv",
    "short_dated_iv",
    "skew",
    "term_structure",
    "put_call_iv_spread",
    "contract_count",
    "fresh_quote_count",
    "stale_quote_count",
    "max_quote_age_seconds",
    "quote_coverage_ratio",
]
SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "rv_5m",
    "rv_15m",
    "rv_1h",
    "rv_4h",
    "rv_1d",
    "parkinson_rv_1h",
    "jump_proxy",
    "spot_available",
    "perps_available",
]
SILVER_IV_RV_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "iv_minus_rv_1h",
    "iv_minus_rv_1d",
    "iv_rv_ratio_1h",
    "iv_rv_ratio_1d",
    "iv_rv_zscore_1d",
    "iv_rv_percentile_30d",
    "minutes_since_iv_observation",
    "minutes_since_rv_observation",
    "iv_available",
    "rv_available",
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
    "volatility_index_1m_observed": SilverDatasetContract(
        dataset_type="volatility_index_1m_observed",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_VOLATILITY_OBSERVED_COLUMNS),
    ),
    "volatility_index_snapshot_1m_observed": SilverDatasetContract(
        dataset_type="volatility_index_snapshot_1m_observed",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_VOLATILITY_OBSERVED_COLUMNS),
    ),
    "volatility_index_1m_feature": SilverDatasetContract(
        dataset_type="volatility_index_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_VOLATILITY_FEATURE_COLUMNS),
    ),
    "realized_volatility_1m_feature": SilverDatasetContract(
        dataset_type="realized_volatility_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS),
    ),
    "iv_rv_1m_feature": SilverDatasetContract(
        dataset_type="iv_rv_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_IV_RV_FEATURE_COLUMNS),
    ),
    "index_price_snapshot_1m_observed": SilverDatasetContract(
        dataset_type="index_price_snapshot_1m_observed",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_INDEX_PRICE_OBSERVED_COLUMNS),
    ),
    "index_price_1m_feature": SilverDatasetContract(
        dataset_type="index_price_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="forward_fill",
        output_columns=tuple(SILVER_INDEX_PRICE_FEATURE_COLUMNS),
    ),
    "futures_summary_snapshot_1m_observed": SilverDatasetContract(
        dataset_type="futures_summary_snapshot_1m_observed",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_FUTURES_SUMMARY_OBSERVED_COLUMNS),
    ),
    "futures_summary_1m_feature": SilverDatasetContract(
        dataset_type="futures_summary_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="forward_fill",
        output_columns=tuple(SILVER_FUTURES_SUMMARY_FEATURE_COLUMNS),
    ),
    "options_ticker_snapshot_1m_observed": SilverDatasetContract(
        dataset_type="options_ticker_snapshot_1m_observed",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS),
    ),
    "options_instrument_ticker_snapshot_1m_observed": SilverDatasetContract(
        dataset_type="options_instrument_ticker_snapshot_1m_observed",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS),
    ),
    "options_surface_1m_feature": SilverDatasetContract(
        dataset_type="options_surface_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_OPTION_SURFACE_FEATURE_COLUMNS),
    ),
    "perps_l2_snapshot_1m_observed": SilverDatasetContract(
        dataset_type="perps_l2_snapshot_1m_observed",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_L2_OBSERVED_COLUMNS),
    ),
    "perps_l2_1m_feature": SilverDatasetContract(
        dataset_type="perps_l2_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_L2_FEATURE_COLUMNS),
    ),
    "options_l2_snapshot_1m_observed": SilverDatasetContract(
        dataset_type="options_l2_snapshot_1m_observed",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_L2_OBSERVED_COLUMNS),
    ),
    "options_l2_1m_feature": SilverDatasetContract(
        dataset_type="options_l2_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_L2_FEATURE_COLUMNS),
    ),
    "recent_trade_snapshot_1m_observed": SilverDatasetContract(
        dataset_type="recent_trade_snapshot_1m_observed",
        timeframe="tick",
        timestamp_column="trade_time",
        timestamp_semantics="trade_time",
        missing_data_policy="drop_invalid",
        output_columns=tuple(SILVER_RECENT_TRADE_SNAPSHOT_OBSERVED_COLUMNS),
    ),
    "instrument_metadata_snapshot_daily_observed": SilverDatasetContract(
        dataset_type="instrument_metadata_snapshot_daily_observed",
        timeframe="1d",
        timestamp_column="snapshot_date",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="none",
        output_columns=tuple(SILVER_INSTRUMENT_METADATA_OBSERVED_COLUMNS),
    ),
    "futures_instrument_metadata_snapshot_daily_observed": SilverDatasetContract(
        dataset_type="futures_instrument_metadata_snapshot_daily_observed",
        timeframe="1d",
        timestamp_column="snapshot_date",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="none",
        output_columns=tuple(SILVER_INSTRUMENT_METADATA_OBSERVED_COLUMNS),
    ),
    "historical_volatility_observed": SilverDatasetContract(
        dataset_type="historical_volatility_observed",
        timeframe="1m",
        timestamp_column="timestamp",
        timestamp_semantics="observed_timestamp",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_HISTORICAL_VOLATILITY_OBSERVED_COLUMNS),
    ),
}

BRONZE_TO_SILVER_DATASETS: dict[str, tuple[str, ...]] = {
    "spot_ohlcv": ("spot_ohlcv",),
    "perps_ohlcv": ("perps_ohlcv",),
    "funding": ("funding_observed", "funding_1m_feature"),
    "open_interest": ("open_interest_observed", "open_interest_1m_feature"),
    "perps_trades": ("perps_trades_observed", "perps_trades_1m_feature"),
    "options_trades": ("options_trades_observed", "options_trades_1m_feature"),
    "volatility_index_data": (
        "volatility_index_data_observed",
        "volatility_index_1m_observed",
        "volatility_index_1m_feature",
    ),
    "volatility_index_snapshot_1m": ("volatility_index_snapshot_1m_observed", "volatility_index_1m_feature"),
    "historical_volatility": ("historical_volatility_observed",),
    "index_price_snapshot_1m": ("index_price_snapshot_1m_observed", "index_price_1m_feature"),
    "futures_summary_snapshot_1m": ("futures_summary_snapshot_1m_observed", "futures_summary_1m_feature"),
    "options_ticker_snapshot_1m": ("options_ticker_snapshot_1m_observed", "options_surface_1m_feature"),
    "options_instrument_ticker_snapshot_1m": (
        "options_instrument_ticker_snapshot_1m_observed",
        "options_surface_1m_feature",
    ),
    "perps_l2_snapshot_1m": ("perps_l2_snapshot_1m_observed", "perps_l2_1m_feature"),
    "options_l2_snapshot_1m": ("options_l2_snapshot_1m_observed", "options_l2_1m_feature"),
    "recent_trade_snapshot_1m": ("recent_trade_snapshot_1m_observed",),
    "instrument_metadata_snapshot_daily": ("instrument_metadata_snapshot_daily_observed",),
    "futures_instrument_metadata_snapshot_daily": ("futures_instrument_metadata_snapshot_daily_observed",),
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
    "gold.market.iv_rv.m1": GoldDatasetContract(
        dataset_id="gold.market.iv_rv.m1",
        requirements=(GoldSourceRequirement("iv_rv_1m_feature", "1m"),),
        include_l2=False,
    ),
    "gold.market.index_price.m1": GoldDatasetContract(
        dataset_id="gold.market.index_price.m1",
        requirements=(GoldSourceRequirement("index_price_1m_feature", "1m"),),
        include_l2=False,
    ),
    "gold.market.futures_summary.m1": GoldDatasetContract(
        dataset_id="gold.market.futures_summary.m1",
        requirements=(GoldSourceRequirement("futures_summary_1m_feature", "1m"),),
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
