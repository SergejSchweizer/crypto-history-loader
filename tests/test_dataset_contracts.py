"""Tests for explicit Silver and Gold dataset transformation contracts."""

from __future__ import annotations

import pytest

from application.dataset_contracts import (
    GOLD_DATASET_CONTRACTS,
    SILVER_DATASET_CONTRACTS,
    gold_dataset_contract,
    silver_dataset_contract,
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
        "volatility_index_data_observed": silver_service.SILVER_VOLATILITY_OBSERVED_COLUMNS,
    }

    assert set(SILVER_DATASET_CONTRACTS) == set(expected_columns)
    for dataset_type, columns in expected_columns.items():
        contract = silver_dataset_contract(dataset_type)
        assert contract.dataset_type == dataset_type
        assert contract.output_columns == tuple(columns)
        assert contract.timestamp_column in contract.output_columns


def test_gold_contracts_are_service_compatible() -> None:
    """Gold contracts should preserve the legacy service spec surface."""

    assert set(GOLD_DATASET_CONTRACTS) == gold_service.SUPPORTED_GOLD_DATASET_IDS
    for dataset_id, contract in GOLD_DATASET_CONTRACTS.items():
        assert gold_service._dataset_requirements(dataset_id) == [
            requirement.as_tuple() for requirement in contract.requirements
        ]
        assert gold_service._dataset_includes_l2(dataset_id) is contract.include_l2
        assert gold_service.GOLD_DATASET_SPECS[dataset_id] == contract.legacy_spec()


def test_contract_lookup_rejects_unknown_dataset_names() -> None:
    """Contract readers should fail loudly for unsupported datasets."""

    with pytest.raises(ValueError, match="Unsupported silver dataset_type"):
        silver_dataset_contract("unknown")

    with pytest.raises(ValueError, match="Unsupported dataset_id"):
        gold_dataset_contract("gold.unknown")
