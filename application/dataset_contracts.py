"""Typed Silver and Gold dataset contracts used by transformation services."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

MissingDataPolicy = Literal[
    "drop_invalid",
    "observed_only",
    "observed_plus_confirmed_empty",
    "derived_trailing",
    "forward_fill",
    "asof_join",
    "none",
]
TimestampSemantics = Literal["event_open_time", "observed_timestamp", "minute_grid", "trade_time"]
QuantitativeUnit = Literal[
    "decimal_return",
    "decimal_volatility",
    "percentage_points",
    "price",
    "boolean",
    "dimensionless",
    "minutes",
]


@dataclass(frozen=True)
class SilverDatasetContract:
    """Schema and time-alignment contract for one Silver output dataset."""

    dataset_type: str
    timeframe: str
    timestamp_column: str
    timestamp_semantics: TimestampSemantics
    missing_data_policy: MissingDataPolicy
    output_columns: tuple[str, ...]
    feature_semantics: dict[str, QuantitativeFeatureSemantics] = field(default_factory=dict)


@dataclass(frozen=True)
class QuantitativeFeatureSemantics:
    """Machine-readable quantitative meaning for a feature column.

    The metadata is intentionally compact: it covers the units and construction
    choices that decide whether two columns may be compared directly, and it records
    the lookback/source policy needed by partitioned builders and manifests.
    """

    unit: QuantitativeUnit
    horizon: str | None
    annualized: bool
    annualization_basis_days: int | None
    estimator: str
    required_lookback_days: int
    source_selection_policy: str
    null_policy: str

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation for manifests and tests."""

        return {
            "unit": self.unit,
            "horizon": self.horizon,
            "annualized": self.annualized,
            "annualization_basis_days": self.annualization_basis_days,
            "estimator": self.estimator,
            "required_lookback_days": self.required_lookback_days,
            "source_selection_policy": self.source_selection_policy,
            "null_policy": self.null_policy,
        }


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
    optional_requirements: tuple[GoldSourceRequirement, ...] = ()
    timestamp_column: str = "timestamp_m1"
    timestamp_semantics: TimestampSemantics = "minute_grid"
    missing_data_policy: MissingDataPolicy = "asof_join"

    def legacy_spec(self) -> dict[str, object]:
        """Return the previous dict shape to keep public service constants compatible."""

        return {
            "requirements": [requirement.as_tuple() for requirement in self.requirements],
            "optional_requirements": [requirement.as_tuple() for requirement in self.optional_requirements],
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
SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "historical_prediction_spot_log_return_1m",
    "historical_prediction_perps_log_return_1m",
    "historical_prediction_spot_rv_15m",
    "historical_prediction_spot_rv_1h",
    "historical_prediction_spot_rv_1d",
    "historical_prediction_perps_rv_15m",
    "historical_prediction_perps_rv_1h",
    "historical_prediction_perps_rv_1d",
    "historical_prediction_spot_perp_basis",
    "historical_prediction_basis_change_1m",
    "historical_prediction_basis_zscore_1h",
    "historical_prediction_open_interest_delta_1m",
    "historical_prediction_open_interest_pct_change_1m",
    "historical_prediction_open_interest_zscore_1h",
    "historical_prediction_funding_rate_change_1m",
    "historical_prediction_funding_rate_zscore_1d",
    "historical_prediction_funding_basis_divergence",
    "historical_prediction_perps_trade_imbalance",
    "historical_prediction_perps_trade_count_zscore_1h",
    "historical_prediction_perps_quote_volume_zscore_1h",
    "historical_prediction_perps_price_impact_1m",
    "historical_prediction_options_trade_imbalance",
    "historical_prediction_options_trade_count_zscore_1h",
    "historical_prediction_options_quote_volume_zscore_1h",
    "historical_prediction_leverage_build_up_signal",
    "historical_prediction_short_stress_signal",
    "historical_prediction_flow_volatility_pressure",
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
    # QC-01: Deribit's DVOL-style volatility index is already an annualized, 30-day
    # implied-volatility measure in percentage points. This alias makes that unit and
    # horizon explicit so downstream IV/RV comparisons never need to guess the
    # semantics of `iv_close`.
    "iv_30d_annualized_pct",
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
    "canonical_rv_source",
    "canonical_rv_source_available",
    "rv_5m",
    "rv_15m",
    "rv_1h",
    "rv_4h",
    "rv_1d",
    # QC-01: raw RV windows above are non-annualized sqrt(sum(log_return^2));
    # the *_annualized_pct siblings below are the unit-safe, annualized-percentage-
    # point equivalents (365-day annualization basis) suitable for direct comparison
    # against `iv_close` / `iv_30d_annualized_pct`.
    "rv_5m_annualized_pct",
    "rv_15m_annualized_pct",
    "rv_1h_annualized_pct",
    "rv_4h_annualized_pct",
    "rv_1d_annualized_pct",
    "rv_30d",
    "rv_30d_annualized_pct",
    "spot_log_return",
    "spot_rv_5m",
    "spot_rv_15m",
    "spot_rv_1h",
    "spot_rv_4h",
    "spot_rv_1d",
    "spot_rv_30d",
    "spot_rv_5m_annualized_pct",
    "spot_rv_15m_annualized_pct",
    "spot_rv_1h_annualized_pct",
    "spot_rv_4h_annualized_pct",
    "spot_rv_1d_annualized_pct",
    "spot_rv_30d_annualized_pct",
    "perps_log_return",
    "perps_rv_5m",
    "perps_rv_15m",
    "perps_rv_1h",
    "perps_rv_4h",
    "perps_rv_1d",
    "perps_rv_30d",
    "perps_rv_5m_annualized_pct",
    "perps_rv_15m_annualized_pct",
    "perps_rv_1h_annualized_pct",
    "perps_rv_4h_annualized_pct",
    "perps_rv_1d_annualized_pct",
    "perps_rv_30d_annualized_pct",
    "parkinson_rv_1h",
    "jump_proxy",
    "spot_available",
    "perps_available",
    "spot_perps_basis_available",
]
SILVER_IV_RV_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "canonical_rv_source",
    # Deprecated (QC-01): these mix an annualized IV percentage-point index with a
    # non-annualized, sub-30-day-horizon RV estimate. Units and horizons are
    # incompatible; kept unchanged for backward compatibility with existing
    # persisted artifacts. Prefer `iv_rv_spread_30d_pct` / `iv_rv_ratio_30d`.
    "iv_minus_rv_1h",
    "iv_minus_rv_1d",
    "iv_rv_ratio_1h",
    "iv_rv_ratio_1d",
    # QC-01: unit- and horizon-compatible comparison, both sides annualized
    # volatility percentage points over a 30-day horizon.
    "iv_rv_spread_30d_pct",
    "iv_rv_ratio_30d",
    "iv_rv_zscore_1d",
    "iv_rv_percentile_30d",
    "minutes_since_iv_observation",
    "minutes_since_rv_observation",
    "iv_available",
    "rv_available",
]


def _volatility_feature_semantics() -> dict[str, QuantitativeFeatureSemantics]:
    return {
        "iv_close": QuantitativeFeatureSemantics(
            unit="percentage_points",
            horizon="30d",
            annualized=True,
            annualization_basis_days=365,
            estimator="deribit_volatility_index_close",
            required_lookback_days=30,
            source_selection_policy="historical_observed_with_snapshot_override_by_timestamp",
            null_policy="observed_only",
        ),
        "iv_30d_annualized_pct": QuantitativeFeatureSemantics(
            unit="percentage_points",
            horizon="30d",
            annualized=True,
            annualization_basis_days=365,
            estimator="deribit_volatility_index_close_alias",
            required_lookback_days=30,
            source_selection_policy="historical_observed_with_snapshot_override_by_timestamp",
            null_policy="observed_only",
        ),
        "iv_return_1m": QuantitativeFeatureSemantics(
            unit="decimal_return",
            horizon="1m",
            annualized=False,
            annualization_basis_days=None,
            estimator="log_return",
            required_lookback_days=30,
            source_selection_policy="same_iv_source_stream",
            null_policy="null_without_previous_positive_close",
        ),
        "iv_percentile_30d": QuantitativeFeatureSemantics(
            unit="dimensionless",
            horizon="30d",
            annualized=False,
            annualization_basis_days=None,
            estimator="closed_trailing_empirical_percentile",
            required_lookback_days=30,
            source_selection_policy="same_iv_source_stream",
            null_policy="null_without_numeric_observation",
        ),
    }


def _rv_feature_semantics() -> dict[str, QuantitativeFeatureSemantics]:
    semantics: dict[str, QuantitativeFeatureSemantics] = {
        "canonical_rv_source": QuantitativeFeatureSemantics(
            unit="dimensionless",
            horizon=None,
            annualized=False,
            annualization_basis_days=None,
            estimator="source_identity",
            required_lookback_days=30,
            source_selection_policy="perps_if_symbol_has_perps_else_spot_no_rowwise_fallback",
            null_policy="never_null_for_processed_rows",
        ),
        "canonical_rv_source_available": QuantitativeFeatureSemantics(
            unit="boolean",
            horizon="1m",
            annualized=False,
            annualization_basis_days=None,
            estimator="selected_source_close_non_null",
            required_lookback_days=30,
            source_selection_policy="perps_if_symbol_has_perps_else_spot_no_rowwise_fallback",
            null_policy="false_when_selected_source_missing",
        ),
        "spot_log_return": QuantitativeFeatureSemantics(
            unit="decimal_return",
            horizon="1m",
            annualized=False,
            annualization_basis_days=None,
            estimator="log_return_on_spot_close",
            required_lookback_days=30,
            source_selection_policy="spot_only",
            null_policy="null_without_previous_positive_spot_close",
        ),
        "perps_log_return": QuantitativeFeatureSemantics(
            unit="decimal_return",
            horizon="1m",
            annualized=False,
            annualization_basis_days=None,
            estimator="log_return_on_perpetual_close",
            required_lookback_days=30,
            source_selection_policy="perps_only",
            null_policy="null_without_previous_positive_perps_close",
        ),
        "parkinson_rv_1h": QuantitativeFeatureSemantics(
            unit="decimal_volatility",
            horizon="1h",
            annualized=False,
            annualization_basis_days=None,
            estimator="parkinson_range_volatility",
            required_lookback_days=30,
            source_selection_policy="canonical_rv_source",
            null_policy="null_when_canonical_ohlc_missing",
        ),
        "jump_proxy": QuantitativeFeatureSemantics(
            unit="dimensionless",
            horizon="1d",
            annualized=False,
            annualization_basis_days=None,
            estimator="absolute_rolling_zscore_of_canonical_log_return",
            required_lookback_days=30,
            source_selection_policy="canonical_rv_source",
            null_policy="null_until_two_observations_or_zero_std",
        ),
    }
    for prefix, source_policy in (("", "canonical_rv_source"), ("spot_", "spot_only"), ("perps_", "perps_only")):
        for raw_column, horizon in (
            ("rv_5m", "5m"),
            ("rv_15m", "15m"),
            ("rv_1h", "1h"),
            ("rv_4h", "4h"),
            ("rv_1d", "1d"),
            ("rv_30d", "30d"),
        ):
            column = f"{prefix}{raw_column}"
            semantics[column] = QuantitativeFeatureSemantics(
                unit="decimal_volatility",
                horizon=horizon,
                annualized=False,
                annualization_basis_days=None,
                estimator="sqrt_sum_squared_log_returns",
                required_lookback_days=30,
                source_selection_policy=source_policy,
                null_policy="null_without_positive_current_and_previous_close",
            )
            semantics[f"{column}_annualized_pct"] = QuantitativeFeatureSemantics(
                unit="percentage_points",
                horizon=horizon,
                annualized=True,
                annualization_basis_days=365,
                estimator="sqrt_sum_squared_log_returns_scaled_to_365d_pct",
                required_lookback_days=30,
                source_selection_policy=source_policy,
                null_policy="null_when_raw_window_null",
            )
    return semantics


def _iv_rv_feature_semantics() -> dict[str, QuantitativeFeatureSemantics]:
    return {
        "canonical_rv_source": QuantitativeFeatureSemantics(
            unit="dimensionless",
            horizon=None,
            annualized=False,
            annualization_basis_days=None,
            estimator="source_identity_from_realized_volatility",
            required_lookback_days=30,
            source_selection_policy="inherited_from_realized_volatility_1m_feature",
            null_policy="null_when_rv_missing",
        ),
        "iv_rv_spread_30d_pct": QuantitativeFeatureSemantics(
            unit="percentage_points",
            horizon="30d",
            annualized=True,
            annualization_basis_days=365,
            estimator="iv_30d_annualized_pct_minus_rv_30d_annualized_pct",
            required_lookback_days=30,
            source_selection_policy="iv_source_and_canonical_rv_source",
            null_policy="null_when_iv_or_rv_null",
        ),
        "iv_rv_ratio_30d": QuantitativeFeatureSemantics(
            unit="dimensionless",
            horizon="30d",
            annualized=True,
            annualization_basis_days=365,
            estimator="iv_30d_annualized_pct_divided_by_rv_30d_annualized_pct",
            required_lookback_days=30,
            source_selection_policy="iv_source_and_canonical_rv_source",
            null_policy="null_when_iv_or_rv_null_or_rv_non_positive",
        ),
        "iv_rv_zscore_1d": QuantitativeFeatureSemantics(
            unit="dimensionless",
            horizon="1d",
            annualized=False,
            annualization_basis_days=None,
            estimator="rolling_zscore_of_legacy_iv_minus_rv_1d",
            required_lookback_days=30,
            source_selection_policy="legacy_iv_rv_1d",
            null_policy="null_until_two_observations_or_zero_std",
        ),
        "iv_rv_percentile_30d": QuantitativeFeatureSemantics(
            unit="dimensionless",
            horizon="30d",
            annualized=False,
            annualization_basis_days=None,
            estimator="closed_trailing_percentile_of_legacy_iv_minus_rv_1d",
            required_lookback_days=30,
            source_selection_policy="legacy_iv_rv_1d",
            null_policy="null_without_numeric_observation",
        ),
    }


def silver_feature_semantics(dataset_type: str) -> dict[str, dict[str, object]]:
    """Return JSON-serializable quantitative feature semantics for a Silver dataset."""

    contract = SILVER_DATASET_CONTRACTS.get(dataset_type)
    if contract is None:
        return {}
    return {column: semantics.as_dict() for column, semantics in contract.feature_semantics.items()}


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
        missing_data_policy="observed_plus_confirmed_empty",
        output_columns=tuple(SILVER_TRADES_M1_FEATURE_COLUMNS),
    ),
    "options_trades_1m_feature": SilverDatasetContract(
        dataset_type="options_trades_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="observed_plus_confirmed_empty",
        output_columns=tuple(SILVER_TRADES_M1_FEATURE_COLUMNS),
    ),
    "historical_prediction_1m_feature": SilverDatasetContract(
        dataset_type="historical_prediction_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="derived_trailing",
        output_columns=tuple(SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS),
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
        feature_semantics=_volatility_feature_semantics(),
    ),
    "realized_volatility_1m_feature": SilverDatasetContract(
        dataset_type="realized_volatility_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS),
        feature_semantics=_rv_feature_semantics(),
    ),
    "iv_rv_1m_feature": SilverDatasetContract(
        dataset_type="iv_rv_1m_feature",
        timeframe="1m",
        timestamp_column="timestamp_m1",
        timestamp_semantics="minute_grid",
        missing_data_policy="observed_only",
        output_columns=tuple(SILVER_IV_RV_FEATURE_COLUMNS),
        feature_semantics=_iv_rv_feature_semantics(),
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

SILVER_LIVE_ORIGIN_BUILD_DATASETS: dict[str, tuple[str, ...]] = {
    "volatility_index_snapshot_1m": ("volatility_index_snapshot_1m_observed", "volatility_index_1m_feature"),
    "realized_volatility": ("realized_volatility_1m_feature",),
    "iv_rv": ("iv_rv_1m_feature",),
    "historical_prediction": ("historical_prediction_1m_feature",),
    "index_price_snapshot_1m": ("index_price_1m_feature",),
    "futures_summary_snapshot_1m": ("futures_summary_1m_feature",),
    "options_ticker_snapshot_1m": ("options_surface_1m_feature",),
    "options_instrument_ticker_snapshot_1m": ("options_surface_1m_feature",),
    "options_surface_1m_feature": ("options_surface_1m_feature",),
    "perps_l2_snapshot_1m": ("perps_l2_1m_feature",),
    "options_l2_snapshot_1m": ("options_l2_1m_feature",),
    "recent_trade_snapshot_1m": ("recent_trade_snapshot_1m_observed",),
    "instrument_metadata_snapshot_daily": ("instrument_metadata_snapshot_daily_observed",),
    "futures_instrument_metadata_snapshot_daily": ("futures_instrument_metadata_snapshot_daily_observed",),
}


def supported_bronze_backed_silver_build_ids() -> tuple[str, ...]:
    """Return stable Silver build IDs that read directly from Bronze datasets."""

    return tuple(sorted(BRONZE_TO_SILVER_DATASETS))


def supported_live_origin_silver_build_ids() -> tuple[str, ...]:
    """Return stable Silver build IDs that derive from live or existing Silver feature sources."""

    return tuple(sorted(SILVER_LIVE_ORIGIN_BUILD_DATASETS))


def supported_silver_build_ids() -> tuple[str, ...]:
    """Return every supported ``silver-build`` dataset choice in stable order."""

    return tuple(sorted({*BRONZE_TO_SILVER_DATASETS, *SILVER_LIVE_ORIGIN_BUILD_DATASETS}))


FULL_MARKET_GOLD_REQUIREMENTS = (
    GoldSourceRequirement("spot_ohlcv", "1m"),
    GoldSourceRequirement("perps_ohlcv", "1m"),
    GoldSourceRequirement("open_interest_1m_feature", "1m"),
    GoldSourceRequirement("funding_1m_feature", "1m"),
    GoldSourceRequirement("perps_trades_1m_feature", "1m"),
    GoldSourceRequirement("options_trades_1m_feature", "1m"),
    GoldSourceRequirement("volatility_index_data_observed", "1m"),
)

REGIME_FEATURE_GOLD_REQUIREMENTS = (
    GoldSourceRequirement("spot_ohlcv", "1m"),
    GoldSourceRequirement("perps_ohlcv", "1m"),
    GoldSourceRequirement("funding_1m_feature", "1m"),
    GoldSourceRequirement("open_interest_1m_feature", "1m"),
    GoldSourceRequirement("realized_volatility_1m_feature", "1m"),
    GoldSourceRequirement("iv_rv_1m_feature", "1m"),
)

REGIME_FEATURE_GOLD_OPTIONAL_REQUIREMENTS = (
    GoldSourceRequirement("perps_l2_1m_feature", "1m"),
    GoldSourceRequirement("options_l2_1m_feature", "1m"),
    GoldSourceRequirement("options_surface_1m_feature", "1m"),
    GoldSourceRequirement("index_price_1m_feature", "1m"),
    GoldSourceRequirement("futures_summary_1m_feature", "1m"),
    GoldSourceRequirement("historical_volatility_observed", "1m"),
)

PREDICTION_TARGET_GOLD_REQUIREMENTS = (
    GoldSourceRequirement("perps_ohlcv", "1m"),
    GoldSourceRequirement("funding_1m_feature", "1m"),
    GoldSourceRequirement("realized_volatility_1m_feature", "1m"),
    GoldSourceRequirement("iv_rv_1m_feature", "1m"),
)

HISTORY_FULL_GOLD_REQUIREMENTS = (
    GoldSourceRequirement("spot_ohlcv", "1m"),
    GoldSourceRequirement("perps_ohlcv", "1m"),
    GoldSourceRequirement("funding_1m_feature", "1m"),
    GoldSourceRequirement("open_interest_1m_feature", "1m"),
    GoldSourceRequirement("perps_trades_1m_feature", "1m"),
    GoldSourceRequirement("options_trades_1m_feature", "1m"),
)

EXTENDED_HISTORY_FULL_GOLD_REQUIREMENTS = HISTORY_FULL_GOLD_REQUIREMENTS + (
    GoldSourceRequirement("historical_prediction_1m_feature", "1m"),
)

LIVE_VOLATILITY_GOLD_REQUIREMENTS = (GoldSourceRequirement("volatility_index_1m_feature", "1m"),)

LIVE_MICROSTRUCTURE_GOLD_REQUIREMENTS = (
    GoldSourceRequirement("perps_l2_1m_feature", "1m"),
    GoldSourceRequirement("options_l2_1m_feature", "1m"),
)

LIVE_FULL_GOLD_REQUIREMENTS = (
    GoldSourceRequirement("volatility_index_snapshot_1m_observed", "1m"),
    GoldSourceRequirement("index_price_snapshot_1m_observed", "1m"),
    GoldSourceRequirement("futures_summary_snapshot_1m_observed", "1m"),
    GoldSourceRequirement("options_ticker_snapshot_1m_observed", "1m"),
    GoldSourceRequirement("options_instrument_ticker_snapshot_1m_observed", "1m"),
    GoldSourceRequirement("perps_l2_snapshot_1m_observed", "1m"),
    GoldSourceRequirement("options_l2_snapshot_1m_observed", "1m"),
    GoldSourceRequirement("recent_trade_snapshot_1m_observed", "1m"),
    GoldSourceRequirement("instrument_metadata_snapshot_daily_observed", "1d"),
    GoldSourceRequirement("futures_instrument_metadata_snapshot_daily_observed", "1d"),
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
        requirements=(
            GoldSourceRequirement("spot_ohlcv", "1m"),
            GoldSourceRequirement("perps_ohlcv", "1m"),
            GoldSourceRequirement("funding_1m_feature", "1m"),
            GoldSourceRequirement("open_interest_1m_feature", "1m"),
            GoldSourceRequirement("realized_volatility_1m_feature", "1m"),
            GoldSourceRequirement("iv_rv_1m_feature", "1m"),
        ),
        optional_requirements=(GoldSourceRequirement("historical_volatility_observed", "1m"),),
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
    "gold.market.regime_features.m1": GoldDatasetContract(
        dataset_id="gold.market.regime_features.m1",
        requirements=REGIME_FEATURE_GOLD_REQUIREMENTS,
        optional_requirements=REGIME_FEATURE_GOLD_OPTIONAL_REQUIREMENTS,
        include_l2=False,
    ),
    "gold.market.prediction_targets.m1": GoldDatasetContract(
        dataset_id="gold.market.prediction_targets.m1",
        requirements=PREDICTION_TARGET_GOLD_REQUIREMENTS,
        include_l2=False,
    ),
    "gold.history.full.m1": GoldDatasetContract(
        dataset_id="gold.history.full.m1",
        requirements=HISTORY_FULL_GOLD_REQUIREMENTS,
        include_l2=False,
    ),
    "gold.history.full.m5": GoldDatasetContract(
        dataset_id="gold.history.full.m5",
        requirements=(),
        include_l2=False,
    ),
    "gold.history.full.m30": GoldDatasetContract(
        dataset_id="gold.history.full.m30",
        requirements=(),
        include_l2=False,
    ),
    "gold.history.full.h1": GoldDatasetContract(
        dataset_id="gold.history.full.h1",
        requirements=(),
        include_l2=False,
    ),
    "gold.history.extended.m1": GoldDatasetContract(
        dataset_id="gold.history.extended.m1",
        requirements=EXTENDED_HISTORY_FULL_GOLD_REQUIREMENTS,
        include_l2=False,
    ),
    "gold.history.extended.m5": GoldDatasetContract(
        dataset_id="gold.history.extended.m5",
        requirements=(),
        include_l2=False,
    ),
    "gold.history.extended.m30": GoldDatasetContract(
        dataset_id="gold.history.extended.m30",
        requirements=(),
        include_l2=False,
    ),
    "gold.history.extended.h1": GoldDatasetContract(
        dataset_id="gold.history.extended.h1",
        requirements=(),
        include_l2=False,
    ),
    "gold.history.extended_full.m1": GoldDatasetContract(
        dataset_id="gold.history.extended_full.m1",
        requirements=EXTENDED_HISTORY_FULL_GOLD_REQUIREMENTS,
        include_l2=False,
    ),
    "gold.live.volatility_features.m1": GoldDatasetContract(
        dataset_id="gold.live.volatility_features.m1",
        requirements=LIVE_VOLATILITY_GOLD_REQUIREMENTS,
        include_l2=False,
        missing_data_policy="observed_only",
    ),
    "gold.live.microstructure_features.m1": GoldDatasetContract(
        dataset_id="gold.live.microstructure_features.m1",
        requirements=LIVE_MICROSTRUCTURE_GOLD_REQUIREMENTS,
        include_l2=False,
        missing_data_policy="observed_only",
    ),
    "gold.live.full.m1": GoldDatasetContract(
        dataset_id="gold.live.full.m1",
        requirements=LIVE_FULL_GOLD_REQUIREMENTS,
        include_l2=False,
        missing_data_policy="observed_only",
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


def supported_gold_dataset_ids() -> tuple[str, ...]:
    """Return every supported ``gold-build`` dataset ID in stable order."""

    supported = {
        "gold.history.full.m1",
        "gold.history.full.m5",
        "gold.history.full.m30",
        "gold.history.full.h1",
        "gold.history.extended.m1",
        "gold.history.extended.m5",
        "gold.history.extended.m30",
        "gold.history.extended.h1",
        "gold.history.extended_full.m1",
        "gold.live.full.m1",
    }
    return tuple(sorted(dataset_id for dataset_id in GOLD_DATASET_CONTRACTS if dataset_id in supported))


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
