from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from application.dataset_contracts import supported_gold_dataset_ids
from application.postgres_sync import (
    GOLD_ROW_KEY_COLUMNS,
    POSTGRES_CONSUMER_SCHEMA,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_ROLE,
    POSTGRES_ROW_HASH_TABLE,
    POSTGRES_SESSION_TIMEZONE,
    POSTGRES_SYNC_SCHEMA,
    POSTGRES_SYNC_STATE_TABLE,
    POSTGRES_TIMESTAMP_TYPE,
    SOURCE_TIMESTAMP_TIMEZONE,
    SOURCE_TIMESTAMP_UNIT,
    GoldDeltaPlan,
    GoldLineage,
    GoldRowDigest,
    GoldRowKey,
    GoldSourceSnapshot,
    GoldSyncResult,
    GoldSyncState,
    consumer_table_name,
    validate_gold_key_columns,
    validate_unique_table_names,
)


def test_exact_postgres_identity_constants() -> None:
    assert POSTGRES_HOST == "10.10.1.3"
    assert POSTGRES_PORT == 54321
    assert POSTGRES_ROLE == "crypto-loader"
    assert POSTGRES_CONSUMER_SCHEMA == "crypto_loader"
    assert POSTGRES_SYNC_SCHEMA == "crypto_loader_sync"
    assert POSTGRES_SYNC_STATE_TABLE == "gold_sync_state"
    assert POSTGRES_ROW_HASH_TABLE == "gold_row_hashes"
    assert POSTGRES_TIMESTAMP_TYPE == "TIMESTAMPTZ(6)"
    assert POSTGRES_SESSION_TIMEZONE == "UTC"
    assert SOURCE_TIMESTAMP_UNIT == "us"
    assert SOURCE_TIMESTAMP_TIMEZONE == "UTC"
    assert GOLD_ROW_KEY_COLUMNS == ("exchange", "symbol", "timestamp_m1")


def test_all_supported_gold_ids_map_uniquely() -> None:
    dataset_ids = supported_gold_dataset_ids()
    mapping = validate_unique_table_names(dataset_ids)
    assert set(mapping) == set(dataset_ids)
    assert len(set(mapping.values())) == len(dataset_ids)
    assert mapping["gold.history.full.m1"] == "gold_history_full_m1"


def test_invalid_dataset_identifiers_fail() -> None:
    with pytest.raises(ValueError):
        consumer_table_name("gold.bad-name.m1")
    with pytest.raises(ValueError):
        consumer_table_name("x" * 64)


def test_gold_key_contract_validation() -> None:
    validate_gold_key_columns(["timestamp_m1", "exchange", "symbol", "value"])
    with pytest.raises(ValueError):
        validate_gold_key_columns(["timestamp_m1", "exchange"])


def test_timestamp_contract_rejects_naive_and_non_utc_values() -> None:
    lineage = GoldLineage("gold.history.full.m1", "deribit", "BTC")
    aware = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
    key = GoldRowKey("deribit", "BTC", aware)
    assert key.timestamp_m1 == aware
    assert key.timestamp_m1.microsecond == 123456

    with pytest.raises(ValueError):
        GoldRowKey("deribit", "BTC", aware.replace(tzinfo=None))
    with pytest.raises(ValueError):
        GoldRowKey("deribit", "BTC", aware.astimezone(timezone(timedelta(hours=1))))

    snapshot = GoldSourceSnapshot(
        lineage=lineage,
        artifact_path=Path("gold.parquet"),
        source_fingerprint="fingerprint",
        schema_signature="schema",
        row_count=1,
        min_timestamp=aware,
        max_timestamp=aware,
        source_version="1.0.0",
    )
    state = GoldSyncState(
        lineage=lineage,
        source_fingerprint=snapshot.source_fingerprint,
        schema_signature=snapshot.schema_signature,
        row_count=1,
        min_timestamp=aware,
        max_timestamp=aware,
        synced_at_utc=aware,
        source_version="1.0.0",
    )
    assert state.synced_at_utc.tzinfo is UTC


def test_immutable_contracts_and_count_semantics() -> None:
    key = GoldRowKey("deribit", "BTC", datetime(2026, 1, 1, tzinfo=UTC))
    digest = GoldRowDigest(key, "a" * 64)
    plan = GoldDeltaPlan((), (), (), (key,), (digest,))
    assert plan.inserted_count == 0
    assert plan.updated_count == 0
    assert plan.deleted_count == 0
    assert plan.unchanged_count == 1
    result = GoldSyncResult(())
    assert result.inserted == result.updated == result.deleted == result.unchanged == 0

    with pytest.raises((AttributeError, TypeError)):
        key.symbol = "ETH"  # type: ignore[misc]
