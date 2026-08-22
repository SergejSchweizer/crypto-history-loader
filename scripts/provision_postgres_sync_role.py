#!/usr/bin/env python3
"""Provision the dedicated PostgreSQL Gold-sync role with least privilege."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import cast

import psycopg
from psycopg import sql

POSTGRES_HOST = "10.10.1.3"
POSTGRES_PORT = 54321
APP_ROLE = "crypto-history-loader"
CONSUMER_SCHEMA = "crypto_history_gold"
SYNC_SCHEMA = "crypto_history_sync"
_REQUIRED_ROLE_ATTRIBUTES = {
    "rolsuper": False,
    "rolcreatedb": False,
    "rolcreaterole": False,
    "rolreplication": False,
    "rolbypassrls": False,
    "rolcanlogin": True,
}


@dataclass(frozen=True, slots=True)
class ProvisioningConfig:
    """Operator-only provisioning settings; secrets are excluded from repr."""

    host: str
    port: int
    database: str
    admin_user: str
    admin_password: str = field(repr=False)
    app_password: str = field(repr=False)

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> ProvisioningConfig:
        """Resolve protected operator settings from environment-style input."""

        host = values.get("PGHOST", POSTGRES_HOST).strip()
        raw_port = values.get("PGPORT", str(POSTGRES_PORT)).strip()
        database = values.get("PGDATABASE", "").strip()
        admin_user = values.get("PGADMINUSER", "").strip()
        admin_password = values.get("PGADMINPASSWORD", "")
        app_password = values.get("PGPASSWORD", "")
        try:
            port = int(raw_port)
        except ValueError as exc:
            raise ValueError("PGPORT must be an integer") from exc
        if host != POSTGRES_HOST or port != POSTGRES_PORT:
            raise ValueError(f"provisioning endpoint must be {POSTGRES_HOST}:{POSTGRES_PORT}")
        if not database:
            raise ValueError("PGDATABASE is required")
        if not admin_user:
            raise ValueError("PGADMINUSER is required")
        if not admin_password:
            raise ValueError("PGADMINPASSWORD is required")
        if not app_password:
            raise ValueError("PGPASSWORD is required for the application role")
        return cls(host, port, database, admin_user, admin_password, app_password)

    @classmethod
    def from_env(cls) -> ProvisioningConfig:
        return cls.from_mapping(os.environ)


def _validate_role(cursor: psycopg.Cursor[tuple[object, ...]]) -> bool:
    """Return whether the role exists and fail if its attributes are incompatible."""

    cursor.execute(
        "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolcanlogin "
        "FROM pg_roles WHERE rolname = %s",
        (APP_ROLE,),
    )
    row = cursor.fetchone()
    if row is None:
        return False
    names = tuple(_REQUIRED_ROLE_ATTRIBUTES)
    actual = dict(zip(names, (bool(value) for value in row), strict=True))
    if actual != _REQUIRED_ROLE_ATTRIBUTES:
        raise RuntimeError("existing PostgreSQL application role has incompatible privileges")
    return True


def _ensure_role(cursor: psycopg.Cursor[tuple[object, ...]], app_password: str) -> None:
    """Create or validate the application role and set its protected runtime password."""

    exists = _validate_role(cursor)
    role_identifier = sql.Identifier(APP_ROLE)
    if not exists:
        cursor.execute(
            sql.SQL(
                "CREATE ROLE {} WITH LOGIN PASSWORD %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
            ).format(role_identifier),
            (app_password,),
        )
    else:
        cursor.execute(
            sql.SQL("ALTER ROLE {} WITH PASSWORD %s").format(role_identifier),
            (app_password,),
        )


def _schema_owner(cursor: psycopg.Cursor[tuple[object, ...]], schema_name: str) -> str | None:
    cursor.execute(
        "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = %s",
        (schema_name,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    owner = row[0]
    if not isinstance(owner, str):
        raise RuntimeError("unexpected PostgreSQL schema owner type")
    return owner


def _ensure_schema(cursor: psycopg.Cursor[tuple[object, ...]], schema_name: str) -> None:
    """Create an application-owned schema or validate compatible existing ownership."""

    owner = _schema_owner(cursor, schema_name)
    if owner is None:
        cursor.execute(
            sql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(sql.Identifier(schema_name), sql.Identifier(APP_ROLE))
        )
    elif owner != APP_ROLE:
        raise RuntimeError(f"existing PostgreSQL schema {schema_name!r} has incompatible ownership")
    cursor.execute(
        sql.SQL("GRANT USAGE, CREATE ON SCHEMA {} TO {}").format(
            sql.Identifier(schema_name),
            sql.Identifier(APP_ROLE),
        )
    )


def provision(config: ProvisioningConfig) -> None:
    """Provision role and schemas idempotently using administrator credentials only here."""

    connection = psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.admin_user,
        password=config.admin_password,
        autocommit=False,
    )
    try:
        cursor = cast(psycopg.Cursor[tuple[object, ...]], connection.cursor())
        try:
            _ensure_role(cursor, config.app_password)
            _ensure_schema(cursor, CONSUMER_SCHEMA)
            _ensure_schema(cursor, SYNC_SCHEMA)
        finally:
            cursor.close()
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def main() -> int:
    """Operator entrypoint. No password is accepted as a process argument."""

    try:
        config = ProvisioningConfig.from_env()
        provision(config)
    except Exception as exc:
        # Error categories are intentionally sanitized. Connection exceptions may
        # contain host/database metadata, but never the protected passwords because
        # we do not interpolate configuration values into this message.
        print(f"PostgreSQL role provisioning failed: {type(exc).__name__}")
        return 1
    print(
        f"PostgreSQL role provisioned: endpoint={POSTGRES_HOST}:{POSTGRES_PORT} "
        f"role={APP_ROLE} schemas={CONSUMER_SCHEMA},{SYNC_SCHEMA}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
