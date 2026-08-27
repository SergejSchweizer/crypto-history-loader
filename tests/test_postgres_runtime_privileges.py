"""Real PostgreSQL probes for the runtime role's DML-only contract."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
import pytest
from psycopg import sql

from scripts.provision_postgres_sync_role import (
    APP_ROLE,
    CONSUMER_SCHEMA,
    SYNC_SCHEMA,
)


def _required_test_dsn(name: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    if os.getenv("CI"):
        pytest.fail(f"required CI PostgreSQL credential {name} is not configured")
    pytest.skip(f"{name} is required for local real-PostgreSQL privilege tests")


@contextmanager
def _admin_probe_table(admin_dsn: str, table_name: str) -> Iterator[None]:
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        with connection.cursor() as cursor:
            table = sql.Identifier(CONSUMER_SCHEMA, table_name)
            cursor.execute(
                sql.SQL(
                    "CREATE TABLE {} (exchange TEXT NOT NULL, symbol TEXT NOT NULL, "
                    "timestamp_m1 TIMESTAMPTZ(6) NOT NULL, value BIGINT, "
                    "PRIMARY KEY (exchange, symbol, timestamp_m1))"
                ).format(table)
            )
            cursor.execute(
                sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {} TO {}").format(
                    table, sql.Identifier(APP_ROLE)
                )
            )
        try:
            yield
        finally:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(CONSUMER_SCHEMA, table_name)))


def _assert_runtime_ddl_denied(runtime_dsn: str, statement: sql.Composable) -> None:
    with psycopg.connect(runtime_dsn, autocommit=False) as connection:
        with connection.cursor() as cursor:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                cursor.execute(statement)
        connection.rollback()


def test_real_runtime_catalog_and_exact_grants() -> None:
    """Read back role attributes, schema ownership, and every direct table grant."""

    admin_dsn = _required_test_dsn("TEST_POSTGRES_ADMIN_DSN")
    with psycopg.connect(admin_dsn, autocommit=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolcanlogin, rolinherit "
                "FROM pg_roles WHERE rolname = %s",
                (APP_ROLE,),
            )
            assert cursor.fetchone() == (False, False, False, False, False, True, False)
            cursor.execute(
                "SELECT nspname, pg_get_userbyid(nspowner), "
                "has_schema_privilege(%s, nspname, 'USAGE'), has_schema_privilege(%s, nspname, 'CREATE') "
                "FROM pg_namespace WHERE nspname IN (%s, %s) ORDER BY nspname",
                (APP_ROLE, APP_ROLE, CONSUMER_SCHEMA, SYNC_SCHEMA),
            )
            schema_rows = cursor.fetchall()
            assert [row[0] for row in schema_rows] == [CONSUMER_SCHEMA, SYNC_SCHEMA]
            assert all(row[1] != APP_ROLE and row[2:] == (True, False) for row in schema_rows)
            cursor.execute(
                "SELECT table_schema, table_name, privilege_type FROM information_schema.role_table_grants "
                "WHERE grantee = %s AND table_schema IN (%s, %s)",
                (APP_ROLE, CONSUMER_SCHEMA, SYNC_SCHEMA),
            )
            grants = {(str(row[0]), str(row[1]), str(row[2])) for row in cursor.fetchall()}
            cursor.execute(
                "SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN (%s, %s)",
                (CONSUMER_SCHEMA, SYNC_SCHEMA),
            )
            tables = {(str(row[0]), str(row[1])) for row in cursor.fetchall()}

    expected: set[tuple[str, str, str]] = set()
    for schema_name, table_name in tables:
        privileges = {"SELECT", "INSERT", "UPDATE"}
        if table_name != "gold_sync_state":
            privileges.add("DELETE")
        expected.update((schema_name, table_name, privilege) for privilege in privileges)
    assert grants == expected


def test_real_runtime_allows_dml_and_denies_create_alter_drop() -> None:
    """Exercise normal consumer mutations and prohibited DDL as the actual runtime role."""

    admin_dsn = _required_test_dsn("TEST_POSTGRES_ADMIN_DSN")
    runtime_dsn = _required_test_dsn("TEST_POSTGRES_RUNTIME_DSN")
    table_name = f"pr86_permission_probe_{uuid.uuid4().hex}"
    table = sql.Identifier(CONSUMER_SCHEMA, table_name)
    with _admin_probe_table(admin_dsn, table_name):
        with psycopg.connect(runtime_dsn, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_user")
                assert cursor.fetchone() == (APP_ROLE,)
                cursor.execute(
                    sql.SQL("INSERT INTO {} VALUES ('test', 'BTC', '2026-01-01T00:00:00Z', 1)").format(table)
                )
                cursor.execute(sql.SQL("UPDATE {} SET value = 2 WHERE symbol = 'BTC'").format(table))
                cursor.execute(sql.SQL("SELECT value FROM {} WHERE symbol = 'BTC'").format(table))
                assert cursor.fetchone() == (2,)
                cursor.execute(sql.SQL("DELETE FROM {} WHERE symbol = 'BTC'").format(table))
            connection.commit()

        _assert_runtime_ddl_denied(
            runtime_dsn,
            sql.SQL("CREATE TABLE {} (id BIGINT)").format(sql.Identifier(CONSUMER_SCHEMA, f"{table_name}_create")),
        )
        _assert_runtime_ddl_denied(runtime_dsn, sql.SQL("ALTER TABLE {} ADD COLUMN forbidden BIGINT").format(table))
        _assert_runtime_ddl_denied(runtime_dsn, sql.SQL("DROP TABLE {}").format(table))
