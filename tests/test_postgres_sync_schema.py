from __future__ import annotations

from collections import OrderedDict

import polars as pl
import pytest

from application.postgres_sync.schema import build_postgres_table_schema, postgres_type_for_polars, quote_identifier


def _base_schema() -> OrderedDict[str, pl.DataType]:
    return OrderedDict(
        [
            ("exchange", pl.String),
            ("symbol", pl.String),
            ("timestamp_m1", pl.Datetime("us", "UTC")),
            ("value", pl.Float64),
        ]
    )


def test_exact_table_order_key_and_timestamp_type() -> None:
    mapped = build_postgres_table_schema("gold.history.full.m1", _base_schema())
    assert mapped.schema_name == "crypto_loader"
    assert mapped.table_name == "gold_history_full_m1"
    assert [column.name for column in mapped.columns] == ["exchange", "symbol", "timestamp_m1", "value"]
    assert mapped.primary_key == ("exchange", "symbol", "timestamp_m1")
    assert '"timestamp_m1" TIMESTAMPTZ(6) NOT NULL' in mapped.ddl
    assert 'PRIMARY KEY ("exchange", "symbol", "timestamp_m1")' in mapped.ddl
    assert "DROP" not in mapped.ddl.upper()
    assert "TRUNCATE" not in mapped.ddl.upper()
    assert "ALTER TABLE" not in mapped.ddl.upper()


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (pl.Datetime("us", "UTC"), "TIMESTAMPTZ(6)"),
        (pl.Date, "DATE"),
        (pl.String, "TEXT"),
        (pl.Boolean, "BOOLEAN"),
        (pl.Int64, "BIGINT"),
        (pl.UInt64, "NUMERIC(20,0)"),
        (pl.Float64, "DOUBLE PRECISION"),
        (pl.Binary, "BYTEA"),
        (pl.List(pl.Float64), "JSONB"),
        (pl.Struct({"x": pl.Int64}), "JSONB"),
    ],
)
def test_dtype_mapping(dtype: pl.DataType, expected: str) -> None:
    assert postgres_type_for_polars(dtype) == expected


def test_naive_or_wrong_precision_datetime_fails() -> None:
    with pytest.raises(TypeError):
        postgres_type_for_polars(pl.Datetime("us"))
    with pytest.raises(TypeError):
        postgres_type_for_polars(pl.Datetime("ms", "UTC"))


def test_unknown_dtype_fails() -> None:
    with pytest.raises(TypeError):
        postgres_type_for_polars(pl.Time)


def test_signature_changes_for_type_nullability_or_order() -> None:
    base = build_postgres_table_schema("gold.history.full.m1", _base_schema())
    changed_schema = _base_schema()
    changed_schema["value"] = pl.Int64
    changed_type = build_postgres_table_schema("gold.history.full.m1", changed_schema)
    nullable = build_postgres_table_schema("gold.history.full.m1", _base_schema(), nullable={"value": False})
    reordered = OrderedDict(reversed(tuple(_base_schema().items())))
    reordered = OrderedDict(
        [
            ("exchange", reordered["exchange"]),
            ("symbol", reordered["symbol"]),
            ("timestamp_m1", reordered["timestamp_m1"]),
            ("value", reordered["value"]),
        ]
    )
    assert base.signature != changed_type.signature
    assert base.signature != nullable.signature


def test_nullable_logical_key_fails() -> None:
    with pytest.raises(ValueError):
        build_postgres_table_schema("gold.history.full.m1", _base_schema(), nullable={"exchange": True})


def test_identifier_quoting_is_safe() -> None:
    assert quote_identifier('x"; DROP TABLE y;--') == '"x""; DROP TABLE y;--"'
