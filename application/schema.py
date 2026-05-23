"""Canonical dataset/instrument schema contracts for market data storage."""

from __future__ import annotations

from dataclasses import dataclass

from application.datasets import CliDataType, DatasetType, InstrumentType, dataset_spec


@dataclass(frozen=True)
class DatasetContract:
    """Mapping contract between CLI data type and storage partition semantics."""

    cli_data_type: CliDataType
    dataset_type: DatasetType
    instrument_type: InstrumentType


def dataset_contract(cli_data_type: CliDataType) -> DatasetContract:
    """Return canonical storage contract for one CLI data type."""

    spec = dataset_spec(cli_data_type)
    return DatasetContract(
        cli_data_type=spec.cli_data_type,
        dataset_type=spec.dataset_type,
        instrument_type=spec.instrument_type,
    )
