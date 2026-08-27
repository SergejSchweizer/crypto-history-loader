"""Pure deterministic mapping from validated Polars Gold schemas to PostgreSQL DDL."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass

import polars as pl

from application.postgres_sync.contracts import (
    GOLD_ROW_KEY_COLUMNS,
    POSTGRES_CONSUMER_SCHEMA,
    POSTGRES_ROW_HASH_TABLE,
    POSTGRES_SYNC_SCHEMA,
    POSTGRES_SYNC_STATE_TABLE,
    POSTGRES_TIMESTAMP_TYPE,
    consumer_table_name,
    validate_gold_key_columns,
)

_DECIMAL_RE = re.compile(r"Decimal\(precision=(\d+), scale=(\d+)\)")


def quote_identifier(identifier: str) -> str:
    """Safely quote a PostgreSQL identifier."""

    return '"' + identifier.replace('"', '""') + '"'


def postgres_type_for_polars(dtype: pl.DataType) -> str:
    """Map one supported source dtype to an exact PostgreSQL type."""

    if dtype == pl.Datetime("us", "UTC"):
        return POSTGRES_TIMESTAMP_TYPE
    if str(dtype).startswith("Datetime"):
        raise TypeError("Gold datetime columns must be Datetime(us, UTC)")
    if dtype == pl.Date:
        return "DATE"
    if dtype in (pl.String, pl.Categorical) or str(dtype).startswith("Enum"):
        return "TEXT"
    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype == pl.UInt64:
        return "NUMERIC(20,0)"
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32):
        return "BIGINT"
    if dtype in (pl.Float32, pl.Float64):
        return "DOUBLE PRECISION"
    if dtype == pl.Binary:
        return "BYTEA"
    dtype_text = str(dtype)
    if dtype_text.startswith("Decimal"):
        match = _DECIMAL_RE.fullmatch(dtype_text)
        if match is None:
            return "NUMERIC"
        precision, scale = match.groups()
        return f"NUMERIC({precision},{scale})"
    if dtype_text.startswith(("List", "Array", "Struct")):
        return "JSONB"
    raise TypeError(f"unsupported Gold dtype for PostgreSQL: {dtype_text}")


@dataclass(frozen=True, slots=True)
class PostgresColumn:
    """One normalized consumer column."""

    name: str
    source_type: str
    postgres_type: str
    nullable: bool


@dataclass(frozen=True, slots=True)
class PostgresTableSchema:
    """Deterministic table definition and compatibility signature."""

    dataset_id: str
    schema_name: str
    table_name: str
    columns: tuple[PostgresColumn, ...]
    primary_key: tuple[str, ...]
    ddl: str
    signature: str


@dataclass(frozen=True, slots=True)
class PostgresCatalogColumn:
    """One exact column shape returned by PostgreSQL catalog introspection."""

    name: str
    data_type: str
    nullable: bool
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    datetime_precision: int | None = None


@dataclass(frozen=True, slots=True)
class PostgresCatalogTable:
    """Exact ordered catalog contract for one owned PostgreSQL table."""

    schema_name: str
    table_name: str
    columns: tuple[PostgresCatalogColumn, ...]
    primary_key: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PostgresBootstrapMigration:
    """Source-controlled bootstrap metadata for one schema contract version."""

    version: int
    tables: tuple[PostgresCatalogTable, ...]
    statements: tuple[str, ...]


POSTGRES_SCHEMA_VERSION = 1


def _catalog_column(name: str, postgres_type: str, nullable: bool) -> PostgresCatalogColumn:
    type_name = postgres_type.upper()
    if type_name == "TEXT":
        return PostgresCatalogColumn(name, "text", nullable)
    if type_name == "BOOLEAN":
        return PostgresCatalogColumn(name, "boolean", nullable)
    if type_name == "BIGINT":
        return PostgresCatalogColumn(name, "bigint", nullable, numeric_precision=64, numeric_scale=0)
    if type_name == "DOUBLE PRECISION":
        return PostgresCatalogColumn(name, "double precision", nullable, numeric_precision=53)
    if type_name == "BYTEA":
        return PostgresCatalogColumn(name, "bytea", nullable)
    if type_name == "DATE":
        return PostgresCatalogColumn(name, "date", nullable)
    if type_name == "JSONB":
        return PostgresCatalogColumn(name, "jsonb", nullable)
    if type_name == "TIMESTAMPTZ(6)":
        return PostgresCatalogColumn(name, "timestamp with time zone", nullable, datetime_precision=6)
    char_match = re.fullmatch(r"CHAR\((\d+)\)", type_name)
    if char_match is not None:
        return PostgresCatalogColumn(name, "character", nullable, character_maximum_length=int(char_match.group(1)))
    numeric_match = re.fullmatch(r"NUMERIC(?:\((\d+),(\d+)\))?", type_name)
    if numeric_match is not None:
        precision, scale = numeric_match.groups()
        return PostgresCatalogColumn(
            name,
            "numeric",
            nullable,
            numeric_precision=None if precision is None else int(precision),
            numeric_scale=None if scale is None else int(scale),
        )
    raise ValueError(f"unsupported canonical PostgreSQL type: {postgres_type}")


def catalog_table_for_schema(schema: PostgresTableSchema) -> PostgresCatalogTable:
    """Convert a mapped Gold schema to its exact PostgreSQL catalog contract."""

    return PostgresCatalogTable(
        schema_name=schema.schema_name,
        table_name=schema.table_name,
        columns=tuple(_catalog_column(column.name, column.postgres_type, column.nullable) for column in schema.columns),
        primary_key=schema.primary_key,
    )


def _sync_catalog_tables() -> tuple[PostgresCatalogTable, PostgresCatalogTable]:
    state_columns = (
        _catalog_column("dataset_id", "TEXT", False),
        _catalog_column("exchange", "TEXT", False),
        _catalog_column("symbol", "TEXT", False),
        _catalog_column("source_fingerprint", "TEXT", False),
        _catalog_column("schema_signature", "CHAR(64)", False),
        _catalog_column("row_count", "BIGINT", False),
        _catalog_column("min_timestamp", POSTGRES_TIMESTAMP_TYPE, True),
        _catalog_column("max_timestamp", POSTGRES_TIMESTAMP_TYPE, True),
        _catalog_column("synced_at_utc", POSTGRES_TIMESTAMP_TYPE, False),
        _catalog_column("source_version", "TEXT", True),
        _catalog_column("build_id", "TEXT", True),
    )
    digest_columns = (
        _catalog_column("dataset_id", "TEXT", False),
        _catalog_column("exchange", "TEXT", False),
        _catalog_column("symbol", "TEXT", False),
        _catalog_column("timestamp_m1", POSTGRES_TIMESTAMP_TYPE, False),
        _catalog_column("row_sha256", "CHAR(64)", False),
    )
    return (
        PostgresCatalogTable(
            POSTGRES_SYNC_SCHEMA,
            POSTGRES_SYNC_STATE_TABLE,
            state_columns,
            ("dataset_id", "exchange", "symbol"),
        ),
        PostgresCatalogTable(
            POSTGRES_SYNC_SCHEMA,
            POSTGRES_ROW_HASH_TABLE,
            digest_columns,
            ("dataset_id", "exchange", "symbol", "timestamp_m1"),
        ),
    )


def _create_table_statement(table: PostgresCatalogTable) -> str:
    definitions: list[str] = []
    for column in table.columns:
        postgres_type = column.data_type.upper()
        if column.data_type == "timestamp with time zone":
            postgres_type = f"TIMESTAMPTZ({column.datetime_precision})"
        elif column.data_type == "character":
            postgres_type = f"CHAR({column.character_maximum_length})"
        elif column.data_type == "numeric" and column.numeric_precision is not None:
            postgres_type = f"NUMERIC({column.numeric_precision},{column.numeric_scale})"
        null_sql = "" if column.nullable else " NOT NULL"
        definitions.append(f"{quote_identifier(column.name)} {postgres_type}{null_sql}")
    definitions.append("PRIMARY KEY (" + ", ".join(quote_identifier(name) for name in table.primary_key) + ")")
    return (
        f"CREATE TABLE IF NOT EXISTS {quote_identifier(table.schema_name)}.{quote_identifier(table.table_name)} (\n"
        f"    {',\n    '.join(definitions)}\n)"
    )


def bootstrap_migration(schema: PostgresTableSchema) -> PostgresBootstrapMigration:
    """Return deterministic idempotent bootstrap SQL, never used by normal sync."""

    tables = (*_sync_catalog_tables(), catalog_table_for_schema(schema))
    statements = (
        f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(POSTGRES_CONSUMER_SCHEMA)}",
        f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(POSTGRES_SYNC_SCHEMA)}",
        *(_create_table_statement(table) for table in tables),
    )
    return PostgresBootstrapMigration(POSTGRES_SCHEMA_VERSION, tables, statements)


def catalog_table_from_generated_ddl(ddl: str) -> PostgresCatalogTable:
    """Parse only the deterministic CREATE TABLE form emitted by this module."""

    match = re.search(
        r'CREATE TABLE IF NOT EXISTS "([^"]+)"\."([^"]+)" \(\n    (.*)\n\)',
        ddl,
        flags=re.DOTALL,
    )
    if match is None:
        raise ValueError("Gold consumer DDL is not in the canonical generated form")
    schema_name, table_name, definitions_text = match.groups()
    definitions = definitions_text.split(",\n    ")
    primary_definition = definitions.pop()
    primary_match = re.fullmatch(r"PRIMARY KEY \((.*)\)", primary_definition)
    if primary_match is None:
        raise ValueError("Gold consumer DDL is missing its canonical primary key")
    primary_key = tuple(part.removeprefix('"').removesuffix('"') for part in primary_match.group(1).split(", "))
    columns: list[PostgresCatalogColumn] = []
    for definition in definitions:
        column_match = re.fullmatch(r'"([^"]+)" (.+?)( NOT NULL)?', definition)
        if column_match is None:
            raise ValueError("Gold consumer DDL contains a non-canonical column definition")
        name, postgres_type, not_null = column_match.groups()
        columns.append(_catalog_column(name, postgres_type, not_null is None))
    return PostgresCatalogTable(schema_name, table_name, tuple(columns), primary_key)


def owned_catalog_tables(consumer_ddl: str) -> tuple[PostgresCatalogTable, ...]:
    """Return all sync and consumer tables owned by one normal sync operation."""

    return (*_sync_catalog_tables(), catalog_table_from_generated_ddl(consumer_ddl))


def _schema_signature(columns: tuple[PostgresColumn, ...], primary_key: tuple[str, ...]) -> str:
    payload = {
        "columns": [[column.name, column.source_type, column.postgres_type, column.nullable] for column in columns],
        "primary_key": list(primary_key),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_postgres_table_schema(
    dataset_id: str,
    source_schema: Mapping[str, pl.DataType],
    *,
    nullable: Mapping[str, bool] | None = None,
) -> PostgresTableSchema:
    """Build idempotent non-destructive DDL from source column order."""

    source_columns = tuple(source_schema)
    validate_gold_key_columns(source_columns)
    nullability = {} if nullable is None else dict(nullable)
    for key in GOLD_ROW_KEY_COLUMNS:
        if nullability.get(key, False):
            raise ValueError(f"logical key column {key!r} must be non-nullable")
    if source_schema["timestamp_m1"] != pl.Datetime("us", "UTC"):
        raise TypeError("timestamp_m1 must be Datetime(us, UTC)")

    columns: list[PostgresColumn] = []
    definitions: list[str] = []
    for name, dtype in source_schema.items():
        is_nullable = nullability.get(name, name not in GOLD_ROW_KEY_COLUMNS)
        pg_type = postgres_type_for_polars(dtype)
        column = PostgresColumn(name=name, source_type=str(dtype), postgres_type=pg_type, nullable=is_nullable)
        columns.append(column)
        null_sql = "" if is_nullable else " NOT NULL"
        definitions.append(f"{quote_identifier(name)} {pg_type}{null_sql}")

    primary_key = tuple(GOLD_ROW_KEY_COLUMNS)
    definitions.append("PRIMARY KEY (" + ", ".join(quote_identifier(column) for column in primary_key) + ")")
    table_name = consumer_table_name(dataset_id)
    qualified = f"{quote_identifier(POSTGRES_CONSUMER_SCHEMA)}.{quote_identifier(table_name)}"
    ddl = (
        f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(POSTGRES_CONSUMER_SCHEMA)};\n"
        f"CREATE TABLE IF NOT EXISTS {qualified} (\n    " + ",\n    ".join(definitions) + "\n);"
    )
    normalized = tuple(columns)
    return PostgresTableSchema(
        dataset_id=dataset_id,
        schema_name=POSTGRES_CONSUMER_SCHEMA,
        table_name=table_name,
        columns=normalized,
        primary_key=primary_key,
        ddl=ddl,
        signature=_schema_signature(normalized, primary_key),
    )
