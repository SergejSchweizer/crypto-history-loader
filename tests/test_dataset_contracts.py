"""Tests for explicit Silver and Gold dataset transformation contracts."""

from __future__ import annotations

from pathlib import Path

import pytest

from application.dataset_contracts import (
    BRONZE_TO_SILVER_DATASETS,
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
