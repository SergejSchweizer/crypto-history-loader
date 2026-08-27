from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from application.postgres_sync.schema import bootstrap_migration, build_postgres_table_schema
from scripts.provision_postgres_sync_role import (
    APP_ROLE,
    CONSUMER_SCHEMA,
    POSTGRES_HOST,
    POSTGRES_PORT,
    SYNC_SCHEMA,
    ProvisioningConfig,
    _apply_migrations,
    _ensure_schema,
    _grant_runtime_table_privileges,
    _migration_tables,
)


class _Cursor:
    def __init__(self, role_exists: bool) -> None:
        self.role_exists = role_exists
        self.queries: list[str] = []

    def execute(self, query: object, params: object = None) -> None:
        self.queries.append(query.as_string(None) if hasattr(query, "as_string") else str(query))

    def fetchone(self) -> tuple[object, ...] | None:
        return (1,) if self.role_exists else None

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


def _valid_env() -> dict[str, str]:
    return {
        "PGHOST": POSTGRES_HOST,
        "PGPORT": str(POSTGRES_PORT),
        "PGDATABASE": "market_data",
        "PGADMINUSER": "postgres-admin",
        "GOLD_ROOT": "/test/gold",
        "PGADMINPASSWORD": "fake-admin-secret",
        "PGPASSWORD": "fake-app-secret",
    }


def test_provisioning_config_exact_identity_and_redaction() -> None:
    config = ProvisioningConfig.from_mapping(_valid_env())
    assert config.host == "10.10.1.3"
    assert config.port == 54321
    assert config.database == "market_data"
    assert APP_ROLE == "crypto-loader"
    assert CONSUMER_SCHEMA == "crypto_loader"
    assert SYNC_SCHEMA == "crypto_loader_sync"
    rendered = repr(config)
    assert "fake-admin-secret" not in rendered
    assert "fake-app-secret" not in rendered


@pytest.mark.parametrize("key", ["PGDATABASE", "PGADMINUSER", "PGADMINPASSWORD", "PGPASSWORD", "GOLD_ROOT"])
def test_required_operator_inputs_fail_closed(key: str) -> None:
    values = _valid_env()
    values[key] = ""
    with pytest.raises(ValueError):
        ProvisioningConfig.from_mapping(values)


def test_wrong_endpoint_is_rejected() -> None:
    values = _valid_env()
    values["PGHOST"] = "localhost"
    with pytest.raises(ValueError):
        ProvisioningConfig.from_mapping(values)


def test_static_sql_contains_only_least_privilege_contract() -> None:
    sql_text = (Path(__file__).resolve().parents[1] / "infra/postgres/provisioning.sql").read_text(encoding="utf-8")
    assert '"crypto-loader"' in sql_text
    assert '"crypto_loader"' in sql_text
    assert '"crypto_loader_sync"' in sql_text
    for token in ("NOINHERIT", "NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE", "NOREPLICATION", "NOBYPASSRLS"):
        assert token in sql_text
    assert "fake-admin-secret" not in sql_text
    assert "fake-app-secret" not in sql_text


def test_role_password_is_escaped_into_ddl_without_driver_parameter_placeholder() -> None:
    cursor = _Cursor(role_exists=False)

    from scripts.provision_postgres_sync_role import _ensure_role

    _ensure_role(cursor, "secret'with-quote")

    assert len(cursor.queries) == 2
    assert "$1" not in cursor.queries[-1]
    assert "secret''with-quote" in cursor.queries[-1]


def test_existing_role_is_hardened_to_exact_attributes() -> None:
    cursor = _Cursor(role_exists=True)

    from scripts.provision_postgres_sync_role import _ensure_role

    _ensure_role(cursor, "replacement-secret")

    assert len(cursor.queries) == 2
    assert cursor.queries[-1].startswith('ALTER ROLE "crypto-loader"')
    for token in ("LOGIN", "NOINHERIT", "NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE", "NOREPLICATION", "NOBYPASSRLS"):
        assert token in cursor.queries[-1]


class _ProvisioningCursor:
    def __init__(self, owners: list[str | None]) -> None:
        self._owners = owners
        self.queries: list[str] = []

    def execute(self, query: object, params: object = None) -> None:
        rendered = query.as_string(None) if hasattr(query, "as_string") else str(query)
        self.queries.append(rendered)

    def fetchone(self) -> tuple[object, ...] | None:
        owner = self._owners.pop(0)
        return None if owner is None else (owner,)

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


def _migration() -> Any:
    schema = build_postgres_table_schema(
        "gold.history.full.m1",
        {
            "exchange": pl.String,
            "symbol": pl.String,
            "timestamp_m1": pl.Datetime("us", "UTC"),
            "value": pl.Float64,
        },
    )
    return bootstrap_migration(schema)


def test_admin_bootstrap_owns_ddl_and_runtime_schema_grants_exclude_create() -> None:
    cursor = _ProvisioningCursor([APP_ROLE, "postgres-admin"])
    _ensure_schema(cursor, CONSUMER_SCHEMA, "postgres-admin")
    _ensure_schema(cursor, SYNC_SCHEMA, "postgres-admin")
    _apply_migrations(cursor, (_migration(),))

    trace = "\n".join(cursor.queries)
    assert 'ALTER SCHEMA "crypto_loader" OWNER TO "postgres-admin"' in trace
    assert 'REVOKE ALL ON SCHEMA "crypto_loader" FROM "crypto-loader"' in trace
    assert 'GRANT USAGE ON SCHEMA "crypto_loader" TO "crypto-loader"' in trace
    assert "GRANT USAGE, CREATE" not in trace
    assert "CREATE TABLE IF NOT EXISTS" in trace


def test_runtime_table_grants_match_consumer_digest_and_state_operations() -> None:
    migration = _migration()
    cursor = _ProvisioningCursor([])
    tables = _migration_tables((migration,))
    _grant_runtime_table_privileges(cursor, tables)
    trace = "\n".join(cursor.queries)

    assert 'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "crypto_loader"."gold_history_full_m1"' in trace
    assert 'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "crypto_loader_sync"."gold_row_hashes"' in trace
    assert 'GRANT SELECT, INSERT, UPDATE ON TABLE "crypto_loader_sync"."gold_sync_state"' in trace
    state_grant = next(query for query in cursor.queries if "GRANT" in query and "gold_sync_state" in query)
    assert "DELETE" not in state_grant
