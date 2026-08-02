"""Tests for loader DTO default-value isolation."""

from __future__ import annotations

import pytest

from application import dto

FetchResult = (
    dto.CandleFetchResultDTO
    | dto.OpenInterestFetchResultDTO
    | dto.FundingFetchResultDTO
    | dto.VolatilityFetchResultDTO
    | dto.TradeFetchResultDTO
)


@pytest.mark.parametrize(
    "result_type",
    [
        dto.CandleFetchResultDTO,
        dto.OpenInterestFetchResultDTO,
        dto.FundingFetchResultDTO,
        dto.VolatilityFetchResultDTO,
        dto.TradeFetchResultDTO,
    ],
)
def test_fetch_result_dtos_do_not_share_mutable_defaults(result_type: type[FetchResult]) -> None:
    """Each result object owns independent rows and errors for concurrent task collection."""

    first = result_type()
    second = result_type()

    assert first.rows == second.rows == {}
    assert first.errors == second.errors == {}
    assert first.rows is not second.rows
    assert first.errors is not second.errors


def test_loader_storage_and_persist_result_defaults_are_independent() -> None:
    """Storage and output DTOs never leak state between CLI invocations."""

    first_storage = dto.LoaderStorageDTO()
    second_storage = dto.LoaderStorageDTO()
    first_storage.candles["spot_ohlcv"] = {}
    assert second_storage.candles == {}

    assert dto.PersistResultDTO().to_output_dict() == {}
    assert dto.PersistResultDTO(parquet_files=["lake/bronze/data.parquet"]).to_output_dict() == {
        "_parquet_files": ["lake/bronze/data.parquet"]
    }
