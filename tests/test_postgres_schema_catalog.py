from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from application.postgres_sync import GoldLineage, GoldSourceSnapshot
from application.postgres_sync.schema import (
    PostgresCatalogTable,
    bootstrap_migration,
    build_postgres_table_schema,
)
from infra.postgres.gold_repository import (
    PostgresConnectionSettings,
    PostgresGoldSyncRepository,
    PostgresSchemaMismatchError,
)


class CatalogCursor:
    def __init__(
        self,
        trace: list[tuple[str, object]],
        column_rows: list[tuple[object, ...]],
        primary_key_rows: list[tuple[object, ...]],
        state_row: tuple[object, ...] | None = None,
    ) -> None:
        self._trace = trace
        self._results: list[list[tuple[object, ...]]] = [column_rows, primary_key_rows]
        self._state_row = state_row

    def execute(self, query: str, params: object = None) -> None:
        self._trace.append((query, params))

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._results.pop(0)

    def fetchone(self) -> tuple[object, ...] | None:
        return self._state_row

    def close(self) -> None:
        return None


class CatalogConnection:
    def __init__(
        self,
        column_rows: list[tuple[object, ...]],
        primary_key_rows: list[tuple[object, ...]],
        state_row: tuple[object, ...] | None = None,
    ) -> None:
        self.trace: list[tuple[str, object]] = []
        self._cursor = CatalogCursor(self.trace, column_rows, primary_key_rows, state_row)

    def cursor(self) -> CatalogCursor:
        return self._cursor

    def commit(self) -> None:
        self.trace.append(("COMMIT", None))

    def rollback(self) -> None:
        self.trace.append(("ROLLBACK", None))

    def close(self) -> None:
        self.trace.append(("CLOSE", None))


def _schema() -> Any:
    return build_postgres_table_schema(
        "gold.history.full.m1",
        {
            "exchange": pl.String,
            "symbol": pl.String,
            "timestamp_m1": pl.Datetime("us", "UTC"),
            "value": pl.Decimal(20, 8),
        },
    )


def _snapshot(signature: str) -> GoldSourceSnapshot:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    return GoldSourceSnapshot(
        GoldLineage("gold.history.full.m1", "deribit", "BTC"),
        Path("gold.parquet"),
        "fingerprint",
        signature,
        1,
        timestamp,
        timestamp,
    )


def _catalog_rows(
    tables: tuple[PostgresCatalogTable, ...],
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]]]:
    column_rows: list[tuple[object, ...]] = []
    primary_key_rows: list[tuple[object, ...]] = []
    for table in sorted(tables, key=lambda item: (item.schema_name, item.table_name)):
        for ordinal, column in enumerate(table.columns, start=1):
            column_rows.append(
                (
                    table.schema_name,
                    table.table_name,
                    ordinal,
                    column.name,
                    column.data_type,
                    "YES" if column.nullable else "NO",
                    column.character_maximum_length,
                    column.numeric_precision,
                    column.numeric_scale,
                    column.datetime_precision,
                )
            )
        for ordinal, column_name in enumerate(table.primary_key, start=1):
            primary_key_rows.append((table.schema_name, table.table_name, column_name, ordinal))
    return column_rows, primary_key_rows


def _repository(connection: CatalogConnection) -> PostgresGoldSyncRepository:
    settings = PostgresConnectionSettings("10.10.1.3", 54321, "crypto-loader", "market_data", "secret")
    return PostgresGoldSyncRepository(settings, connection_factory=lambda _: connection)


def test_bootstrap_metadata_is_versioned_deterministic_and_idempotent() -> None:
    schema = _schema()
    first = bootstrap_migration(schema)
    second = bootstrap_migration(schema)
    assert first == second
    assert first.version == 1
    assert len(first.tables) == 3
    assert all(statement.startswith("CREATE ") and "IF NOT EXISTS" in statement for statement in first.statements)


def test_compatible_actual_catalog_passes_without_normal_sync_ddl() -> None:
    schema = _schema()
    migration = bootstrap_migration(schema)
    connection = CatalogConnection(*_catalog_rows(migration.tables))
    _repository(connection).ensure_lineage(_snapshot(schema.signature), schema.ddl, schema.signature)
    sql = "\n".join(query for query, _ in connection.trace)
    assert "information_schema.columns" in sql
    assert "pg_catalog.pg_index" in sql
    assert "CREATE " not in sql
    assert ("COMMIT", None) in connection.trace


@pytest.mark.parametrize("drift", ["type", "precision", "nullability", "primary_key", "extra", "missing"])
def test_actual_catalog_drift_requires_migration_before_dml(drift: str) -> None:
    schema = _schema()
    migration = bootstrap_migration(schema)
    column_rows, primary_key_rows = _catalog_rows(migration.tables)
    consumer_name = schema.table_name
    consumer_indexes = [
        index for index, row in enumerate(column_rows) if row[0:2] == (schema.schema_name, consumer_name)
    ]
    if drift == "type":
        index = consumer_indexes[-1]
        column_rows[index] = (*column_rows[index][:4], "text", *column_rows[index][5:])
    elif drift == "precision":
        index = consumer_indexes[2]
        column_rows[index] = (*column_rows[index][:9], 3)
    elif drift == "nullability":
        index = consumer_indexes[-1]
        column_rows[index] = (*column_rows[index][:5], "NO", *column_rows[index][6:])
    elif drift == "primary_key":
        primary_key_rows[-2], primary_key_rows[-1] = primary_key_rows[-1], primary_key_rows[-2]
        primary_key_rows[-2] = (*primary_key_rows[-2][:3], 1)
        primary_key_rows[-1] = (*primary_key_rows[-1][:3], 2)
    elif drift == "extra":
        ordinal = len(consumer_indexes) + 1
        extra_column = (schema.schema_name, consumer_name, ordinal, "extra", "text", "YES", None, None, None, None)
        column_rows.insert(consumer_indexes[-1] + 1, extra_column)
    else:
        column_rows.pop(consumer_indexes[-1])

    connection = CatalogConnection(column_rows, primary_key_rows)
    with pytest.raises(PostgresSchemaMismatchError, match="migration required"):
        _repository(connection).ensure_lineage(_snapshot(schema.signature), schema.ddl, schema.signature)
    sql = "\n".join(query for query, _ in connection.trace)
    assert "CREATE " not in sql
    assert 'INSERT INTO "crypto_loader"' not in sql
    assert 'UPDATE "crypto_loader"' not in sql
    assert 'DELETE FROM "crypto_loader"' not in sql
    assert ("ROLLBACK", None) in connection.trace


@pytest.mark.parametrize("has_saved_state", [False, True])
def test_saved_sync_state_does_not_hide_incompatible_catalog(has_saved_state: bool) -> None:
    schema = _schema()
    migration = bootstrap_migration(schema)
    column_rows, primary_key_rows = _catalog_rows(migration.tables)
    column_rows[0] = (*column_rows[0][:4], "bigint", *column_rows[0][5:7], 64, 0, None)
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    saved_state = (
        "gold.history.full.m1",
        "deribit",
        "BTC",
        "fingerprint",
        schema.signature,
        1,
        timestamp,
        timestamp,
        timestamp,
        None,
        None,
    )
    connection = CatalogConnection(column_rows, primary_key_rows, state_row=saved_state if has_saved_state else None)
    with pytest.raises(PostgresSchemaMismatchError, match="migration required"):
        _repository(connection).ensure_lineage(_snapshot(schema.signature), schema.ddl, schema.signature)
    state_reads = (
        "gold_sync_state" in query and query.startswith("SELECT dataset_id") for query, _ in connection.trace
    )
    assert not any(state_reads)
