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


def _schema_signature(columns: tuple[PostgresColumn, ...], primary_key: tuple[str, ...]) -> str:
    payload = {
        "columns": [
            [column.name, column.source_type, column.postgres_type, column.nullable] for column in columns
        ],
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
        f"CREATE TABLE IF NOT EXISTS {qualified} (\n    "
        + ",\n    ".join(definitions)
        + "\n);"
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
