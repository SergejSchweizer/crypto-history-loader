"""Tests for explicit Silver and Gold dataset transformation contracts."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from application.dataset_contracts import (
    BRONZE_TO_SILVER_DATASETS,
    GOLD_DATASET_CONTRACTS,
    SILVER_DATASET_CONTRACTS,
    SILVER_LIVE_ORIGIN_BUILD_DATASETS,
    gold_dataset_contract,
    silver_dataset_contract,
    silver_feature_semantics,
    supported_bronze_backed_silver_build_ids,
    supported_gold_dataset_ids,
    supported_live_origin_silver_build_ids,
    supported_silver_build_ids,
)
from application.services import gold_service, silver_service


def test_silver_contracts_cover_service_output_columns() -> None:
    """Silver services should use the same output columns declared by contracts."""

    expected_columns = {
        "spot_ohlcv": silver_service.SILVER_OHLCV_COLUMNS,
        "perps_ohlcv": silver_service.SILVER_OHLCV_COLUMNS,
        "funding_observed": silver_service.SILVER_FUNDING_OBSERVED_COLUMNS,
        "funding_1m_feature": silver_service.SILVER_FUNDING_FEATURE_COLUMNS,
        "open_interest_observed": silver_service.SILVER_OPEN_INTEREST_OBSERVED_COLUMNS,
        "open_interest_1m_feature": silver_service.SILVER_OPEN_INTEREST_M1_FEATURE_COLUMNS,
        "perps_trades_observed": silver_service.SILVER_TRADES_OBSERVED_COLUMNS,
        "options_trades_observed": silver_service.SILVER_TRADES_OBSERVED_COLUMNS,
        "perps_trades_1m_feature": silver_service.SILVER_TRADES_M1_FEATURE_COLUMNS,
        "options_trades_1m_feature": silver_service.SILVER_TRADES_M1_FEATURE_COLUMNS,
        "historical_prediction_1m_feature": silver_service.SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS,
        "volatility_index_data_observed": silver_service.SILVER_VOLATILITY_OBSERVED_COLUMNS,
        "volatility_index_snapshot_1m_observed": silver_service.SILVER_VOLATILITY_OBSERVED_COLUMNS,
        "volatility_index_1m_feature": silver_service.SILVER_VOLATILITY_FEATURE_COLUMNS,
        "realized_volatility_1m_feature": silver_service.SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS,
        "iv_rv_1m_feature": silver_service.SILVER_IV_RV_FEATURE_COLUMNS,
        "index_price_snapshot_1m_observed": silver_service.SILVER_INDEX_PRICE_OBSERVED_COLUMNS,
        "index_price_1m_feature": silver_service.SILVER_INDEX_PRICE_FEATURE_COLUMNS,
        "futures_summary_snapshot_1m_observed": silver_service.SILVER_FUTURES_SUMMARY_OBSERVED_COLUMNS,
        "futures_summary_1m_feature": silver_service.SILVER_FUTURES_SUMMARY_FEATURE_COLUMNS,
        "options_ticker_snapshot_1m_observed": silver_service.SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS,
        "options_instrument_ticker_snapshot_1m_observed": silver_service.SILVER_OPTIONS_TICKER_OBSERVED_COLUMNS,
        "options_surface_1m_feature": silver_service.SILVER_OPTION_SURFACE_FEATURE_COLUMNS,
        "perps_l2_snapshot_1m_observed": silver_service.SILVER_L2_OBSERVED_COLUMNS,
        "perps_l2_1m_feature": silver_service.SILVER_L2_FEATURE_COLUMNS,
        "options_l2_snapshot_1m_observed": silver_service.SILVER_L2_OBSERVED_COLUMNS,
        "options_l2_1m_feature": silver_service.SILVER_L2_FEATURE_COLUMNS,
        "recent_trade_snapshot_1m_observed": silver_service.SILVER_RECENT_TRADE_SNAPSHOT_OBSERVED_COLUMNS,
        "instrument_metadata_snapshot_daily_observed": silver_service.SILVER_INSTRUMENT_METADATA_OBSERVED_COLUMNS,
        "futures_instrument_metadata_snapshot_daily_observed": (
            silver_service.SILVER_INSTRUMENT_METADATA_OBSERVED_COLUMNS
        ),
        "historical_volatility_observed": silver_service.SILVER_HISTORICAL_VOLATILITY_OBSERVED_COLUMNS,
    }

    for dataset_type, columns in expected_columns.items():
        contract = silver_dataset_contract(dataset_type)
        assert contract.dataset_type == dataset_type
        assert contract.output_columns == tuple(columns)
        assert contract.timestamp_column in contract.output_columns


def test_backlog_bronze_datasets_have_silver_destinations() -> None:
    """Every local Bronze dataset listed in the backlog should have planned Silver outputs."""

    backlog = (Path(__file__).resolve().parents[1] / "BACKLOG.md").read_text(encoding="utf-8")
    start_marker = "Bronze dataset types present locally:\n\n```text\n"
    end_marker = "\n```"
    start = backlog.index(start_marker) + len(start_marker)
    end = backlog.index(end_marker, start)
    bronze_datasets = {line.strip() for line in backlog[start:end].splitlines() if line.strip()}

    missing = sorted(bronze_datasets.difference(BRONZE_TO_SILVER_DATASETS))
    assert not missing

    for bronze_dataset in bronze_datasets:
        destinations = BRONZE_TO_SILVER_DATASETS[bronze_dataset]
        assert destinations
        for silver_dataset in destinations:
            assert silver_dataset in SILVER_DATASET_CONTRACTS
            contract = silver_dataset_contract(silver_dataset)
            assert contract.timestamp_column in contract.output_columns


def test_gold_contracts_are_service_compatible() -> None:
    """Gold contracts should preserve the legacy service spec surface."""

    expected_supported = {
        "gold.history.full.m1",
        "gold.history.full.m5",
        "gold.history.full.m30",
        "gold.history.full.h1",
        "gold.history.extended_full.m1",
    }
    assert gold_service.SUPPORTED_GOLD_DATASET_IDS == expected_supported
    for dataset_id in expected_supported:
        contract = GOLD_DATASET_CONTRACTS[dataset_id]
        assert gold_service._dataset_requirements(dataset_id) == [
            requirement.as_tuple() for requirement in contract.requirements
        ]
        assert gold_service._dataset_includes_l2(dataset_id) is contract.include_l2
        assert gold_service.GOLD_DATASET_SPECS[dataset_id] == contract.legacy_spec()


def test_supported_dataset_helpers_are_contract_driven_and_stable() -> None:
    """Canonical build-choice helpers should be sorted and backed by dataset contracts."""

    assert supported_bronze_backed_silver_build_ids() == tuple(sorted(BRONZE_TO_SILVER_DATASETS))
    assert supported_live_origin_silver_build_ids() == tuple(sorted(SILVER_LIVE_ORIGIN_BUILD_DATASETS))
    assert supported_silver_build_ids() == tuple(
        sorted({*BRONZE_TO_SILVER_DATASETS, *SILVER_LIVE_ORIGIN_BUILD_DATASETS})
    )
    assert supported_gold_dataset_ids() == (
        "gold.history.extended_full.m1",
        "gold.history.full.h1",
        "gold.history.full.m1",
        "gold.history.full.m30",
        "gold.history.full.m5",
    )

    for outputs in (*BRONZE_TO_SILVER_DATASETS.values(), *SILVER_LIVE_ORIGIN_BUILD_DATASETS.values()):
        assert outputs
        assert set(outputs) <= set(SILVER_DATASET_CONTRACTS)


def test_gold_dataset_ids_follow_canonical_or_extended_grain_naming() -> None:
    """Gold dataset names should stay explicit about family and grain."""

    pattern = re.compile(r"^gold\.(history|market|live|hybrid)\.[a-z0-9_]+\.(m1|m5|m30|h1)$")

    for dataset_id in GOLD_DATASET_CONTRACTS:
        assert pattern.fullmatch(dataset_id), dataset_id


def test_contract_lookup_rejects_unknown_dataset_names() -> None:
    """Contract readers should fail loudly for unsupported datasets."""

    with pytest.raises(ValueError, match="Unsupported silver dataset_type"):
        silver_dataset_contract("unknown")

    with pytest.raises(ValueError, match="Unsupported dataset_id"):
        gold_dataset_contract("gold.unknown")


def test_iv_rv_contract_semantics_are_unit_and_horizon_compatible() -> None:
    """QC-04: unit-safe IV/RV comparisons must declare matching semantics."""

    iv_semantics = silver_feature_semantics("volatility_index_1m_feature")
    rv_semantics = silver_feature_semantics("realized_volatility_1m_feature")
    iv_rv_semantics = silver_feature_semantics("iv_rv_1m_feature")

    iv_30d = iv_semantics["iv_30d_annualized_pct"]
    rv_30d = rv_semantics["rv_30d_annualized_pct"]
    spread_30d = iv_rv_semantics["iv_rv_spread_30d_pct"]

    for key in ("unit", "horizon", "annualized", "annualization_basis_days"):
        assert iv_30d[key] == rv_30d[key]
        assert spread_30d[key] == iv_30d[key]

    assert rv_semantics["rv_1h"]["unit"] == "decimal_volatility"
    assert rv_semantics["rv_1h"]["annualized"] is False
    assert rv_semantics["rv_1h"]["horizon"] == "1h"
    assert rv_semantics["rv_30d_annualized_pct"]["source_selection_policy"] == "canonical_rv_source"


def test_realized_volatility_contract_declares_source_specific_semantics() -> None:
    """QC-03/QC-04: source-specific RV fields must not inherit row-wise fallback semantics."""

    rv_semantics = silver_feature_semantics("realized_volatility_1m_feature")

    assert rv_semantics["canonical_rv_source"]["source_selection_policy"] == (
        "perps_if_symbol_has_perps_else_spot_no_rowwise_fallback"
    )
    assert rv_semantics["spot_rv_1h"]["source_selection_policy"] == "spot_only"
    assert rv_semantics["perps_rv_1h"]["source_selection_policy"] == "perps_only"
    assert rv_semantics["spot_rv_1h"]["unit"] == rv_semantics["perps_rv_1h"]["unit"] == "decimal_volatility"
    assert rv_semantics["spot_rv_1h_annualized_pct"]["unit"] == "percentage_points"
    assert rv_semantics["spot_rv_1h_annualized_pct"]["annualized"] is True
