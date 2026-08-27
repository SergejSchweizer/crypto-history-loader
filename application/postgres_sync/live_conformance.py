"""Read-only conformance verification for the production PostgreSQL serving plane."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import psycopg
from psycopg import sql

from application.postgres_sync.config import PostgresSyncConfig
from application.postgres_sync.contracts import (
    POSTGRES_CONSUMER_SCHEMA,
    POSTGRES_ROLE,
    POSTGRES_SESSION_TIMEZONE,
    POSTGRES_SYNC_SCHEMA,
    GoldLineage,
    consumer_table_name,
)
from application.postgres_sync.delta import canonical_row_hash
from application.postgres_sync.inventory import discover_current_gold_lineages
from application.postgres_sync.schema import (
    PostgresCatalogTable,
    bootstrap_migration,
    build_postgres_table_schema,
)

_REPORT_VERSION = "postgres-live-conformance-v2"
_EXPECTED_ROLE_FLAGS = {
    "rolsuper": False,
    "rolcreatedb": False,
    "rolcreaterole": False,
    "rolreplication": False,
    "rolbypassrls": False,
    "rolcanlogin": True,
}
_REQUIRED_TIMEOUTS = ("statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout")


@dataclass(frozen=True, slots=True)
class ConformanceCheck:
    """One sanitized, stable conformance result."""

    name: str
    passed: bool
    category: str
    detail: str


@dataclass(frozen=True, slots=True)
class LiveConformanceReport:
    """Serializable evidence that never includes connection secrets or market rows."""

    report_version: str
    status: str
    generated_at_utc: str
    endpoint: dict[str, object]
    checks: tuple[ConformanceCheck, ...]
    lineage_count: int

    def payload(self) -> dict[str, object]:
        return {
            "report_version": self.report_version,
            "status": self.status,
            "generated_at_utc": self.generated_at_utc,
            "endpoint": self.endpoint,
            "lineage_count": self.lineage_count,
            "checks": [asdict(check) for check in self.checks],
        }


def _check(checks: list[ConformanceCheck], name: str, passed: bool, category: str, detail: str) -> None:
    checks.append(ConformanceCheck(name, passed, category, detail))


def _quoted_table(schema_name: str, table_name: str) -> sql.Composed:
    return sql.SQL("{}.").format(sql.Identifier(schema_name)) + sql.Identifier(table_name)


def _utc(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("database timestamp is not aware UTC")
    return value.astimezone(UTC)


def _expected_catalog(gold_root: Path) -> tuple[tuple[Any, ...], tuple[PostgresCatalogTable, ...]]:
    snapshots = discover_current_gold_lineages(gold_root)
    if not snapshots:
        raise ValueError("no current certified Gold lineages were found")
    expected: dict[tuple[str, str], PostgresCatalogTable] = {}
    for snapshot in snapshots:
        schema = build_postgres_table_schema(
            snapshot.lineage.dataset_id,
            dict(pl.read_parquet_schema(snapshot.artifact_path)),
        )
        for table in bootstrap_migration(schema).tables:
            key = (table.schema_name, table.table_name)
            previous = expected.get(key)
            if previous is not None and previous != table:
                raise ValueError("current Gold schemas disagree for one consumer table")
            expected[key] = table
    return snapshots, tuple(expected[key] for key in sorted(expected))


def _actual_catalog(
    cursor: psycopg.Cursor[tuple[object, ...]], expected: tuple[PostgresCatalogTable, ...]
) -> tuple[bool, str]:
    expected_keys = {(table.schema_name, table.table_name) for table in expected}
    cursor.execute(
        """SELECT table_schema, table_name FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' AND table_schema IN (%s, %s)
        ORDER BY table_schema, table_name""",
        (POSTGRES_CONSUMER_SCHEMA, POSTGRES_SYNC_SCHEMA),
    )
    actual_keys = {(str(schema), str(table)) for schema, table in cursor.fetchall()}
    if actual_keys != expected_keys:
        return False, "owned table set differs from the certified Gold contract"
    for table in expected:
        cursor.execute(
            """SELECT column_name, data_type, is_nullable, character_maximum_length,
                      numeric_precision, numeric_scale, datetime_precision
               FROM information_schema.columns
               WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""",
            (table.schema_name, table.table_name),
        )
        actual_columns = tuple(cursor.fetchall())
        expected_columns = tuple(
            (
                column.name,
                column.data_type,
                "YES" if column.nullable else "NO",
                column.character_maximum_length,
                column.numeric_precision,
                column.numeric_scale,
                column.datetime_precision,
            )
            for column in table.columns
        )
        if actual_columns != expected_columns:
            return False, f"catalog column contract differs for {table.schema_name}.{table.table_name}"
        cursor.execute(
            """SELECT attribute.attname FROM pg_catalog.pg_index AS index_definition
               JOIN pg_catalog.pg_class AS relation ON relation.oid = index_definition.indrelid
               JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
               JOIN LATERAL unnest(index_definition.indkey) WITH ORDINALITY AS key_column(attnum, ordinality) ON TRUE
               JOIN pg_catalog.pg_attribute AS attribute
                 ON attribute.attrelid = relation.oid AND attribute.attnum = key_column.attnum
               WHERE index_definition.indisprimary AND namespace.nspname = %s AND relation.relname = %s
               ORDER BY key_column.ordinality""",
            (table.schema_name, table.table_name),
        )
        if tuple(str(row[0]) for row in cursor.fetchall()) != table.primary_key:
            return False, f"primary-key contract differs for {table.schema_name}.{table.table_name}"
    return True, "owned catalog exactly matches the certified Gold contract"


def _source_digest_map(snapshot: Any) -> dict[tuple[str, str, datetime], str]:
    frame = pl.read_parquet(snapshot.artifact_path)
    result: dict[tuple[str, str, datetime], str] = {}
    for row in frame.to_dicts():
        timestamp = _utc(row["timestamp_m1"])
        if timestamp is None:
            raise ValueError("Gold source timestamp cannot be null")
        key = (str(row["exchange"]), str(row["symbol"]), timestamp)
        if key in result:
            raise ValueError("Gold source contains duplicate logical keys")
        result[key] = canonical_row_hash(row)
    return result


def _target_digest_map(
    cursor: psycopg.Cursor[tuple[object, ...]], snapshot: Any
) -> tuple[dict[tuple[str, str, datetime], str], dict[tuple[str, str, datetime], str]]:
    lineage: GoldLineage = snapshot.lineage
    table = _quoted_table(POSTGRES_CONSUMER_SCHEMA, consumer_table_name(lineage.dataset_id))
    cursor.execute(
        sql.SQL("SELECT * FROM {} WHERE exchange = %s AND symbol = %s ORDER BY timestamp_m1").format(table),
        (lineage.exchange, lineage.symbol),
    )
    names = tuple(column.name for column in cursor.description or ())
    rows = cursor.fetchall()
    target: dict[tuple[str, str, datetime], str] = {}
    for values in rows:
        row = dict(zip(names, values, strict=True))
        timestamp = _utc(row["timestamp_m1"])
        if timestamp is None:
            raise ValueError("consumer timestamp cannot be null")
        key = (str(row["exchange"]), str(row["symbol"]), timestamp)
        target[key] = canonical_row_hash(row)
    cursor.execute(
        """SELECT timestamp_m1, row_sha256 FROM crypto_loader_sync.gold_row_hashes
           WHERE dataset_id = %s AND exchange = %s AND symbol = %s ORDER BY timestamp_m1""",
        (lineage.dataset_id, lineage.exchange, lineage.symbol),
    )
    persisted: dict[tuple[str, str, datetime], str] = {}
    for digest_row in cursor.fetchall():
        if len(digest_row) != 2:
            raise ValueError("digest table row has unexpected width")
        parsed = _utc(digest_row[0])
        digest = digest_row[1]
        if parsed is None or not isinstance(digest, str):
            raise ValueError("digest table contains malformed value")
        persisted[(lineage.exchange, lineage.symbol, parsed)] = digest
    return target, persisted


def _verify_lineage(cursor: psycopg.Cursor[tuple[object, ...]], snapshot: Any) -> tuple[bool, str]:
    source = _source_digest_map(snapshot)
    target, persisted = _target_digest_map(cursor, snapshot)
    if source.keys() != target.keys():
        return False, "source and consumer logical-key sets differ"
    if source != target:
        return False, "source and consumer row digests differ"
    if source != persisted:
        return False, "source and persisted digest-table contents differ"
    lineage: GoldLineage = snapshot.lineage
    cursor.execute(
        """SELECT source_fingerprint, schema_signature, row_count, min_timestamp, max_timestamp,
                  source_version, build_id
           FROM crypto_loader_sync.gold_sync_state
           WHERE dataset_id = %s AND exchange = %s AND symbol = %s""",
        (lineage.dataset_id, lineage.exchange, lineage.symbol),
    )
    state = cursor.fetchone()
    if state is None:
        return False, "certified lineage has no sync checkpoint"
    fingerprint, signature, row_count, minimum, maximum, version, build_id = state
    expected = (
        snapshot.source_fingerprint,
        snapshot.schema_signature,
        snapshot.row_count,
        snapshot.min_timestamp,
        snapshot.max_timestamp,
        snapshot.source_version,
        snapshot.build_id,
    )
    actual = (fingerprint, signature, row_count, _utc(minimum), _utc(maximum), version, build_id)
    if actual != expected:
        return False, "sync checkpoint fingerprint, schema, bounds, or version differs"
    return True, "source, consumer, digests, and checkpoint are exactly equivalent"


def _runtime_contract(cursor: psycopg.Cursor[tuple[object, ...]]) -> tuple[bool, str]:
    cursor.execute(
        """SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolcanlogin
           FROM pg_roles WHERE rolname = current_user"""
    )
    row = cursor.fetchone()
    if row is None:
        return False, "runtime role was not found"
    actual = dict(zip(_EXPECTED_ROLE_FLAGS, (bool(value) for value in row), strict=True))
    if actual != _EXPECTED_ROLE_FLAGS:
        return False, "runtime role attributes violate least privilege"
    cursor.execute("SHOW TIME ZONE")
    timezone = cursor.fetchone()
    if timezone != (POSTGRES_SESSION_TIMEZONE,):
        return False, "runtime session timezone is not UTC"
    for setting in _REQUIRED_TIMEOUTS:
        cursor.execute(sql.SQL("SHOW {}").format(sql.Identifier(setting)))
        value = cursor.fetchone()
        if value is None or str(value[0]).lower() in {"0", "0ms", "0s"}:
            return False, f"runtime timeout {setting} is disabled"
    cursor.execute(
        """SELECT nspname, pg_get_userbyid(nspowner) FROM pg_namespace
           WHERE nspname IN (%s, %s) ORDER BY nspname""",
        (POSTGRES_CONSUMER_SCHEMA, POSTGRES_SYNC_SCHEMA),
    )
    owners = cursor.fetchall()
    if len(owners) != 2 or any(owner == POSTGRES_ROLE for _, owner in owners):
        return False, "runtime role owns an application schema and can perform DDL"
    for schema_name in (POSTGRES_CONSUMER_SCHEMA, POSTGRES_SYNC_SCHEMA):
        cursor.execute("SELECT has_schema_privilege(current_user, %s, 'USAGE')", (schema_name,))
        if cursor.fetchone() != (True,):
            return False, f"runtime role lacks USAGE privilege on {schema_name}"
        cursor.execute("SELECT has_schema_privilege(current_user, %s, 'CREATE')", (schema_name,))
        if cursor.fetchone() != (False,):
            return False, f"runtime role retains CREATE privilege on {schema_name}"
    return True, "runtime role, UTC session, timeouts, and schema privileges conform"


def _rollback_probes(cursor: psycopg.Cursor[tuple[object, ...]], snapshots: tuple[Any, ...]) -> tuple[bool, str]:
    probe = datetime(2026, 10, 25, 1, 59, 59, 654321, tzinfo=UTC)
    cursor.execute("SELECT %s::timestamptz(6)", (probe,))
    if cursor.fetchone() != (probe,):
        return False, "microsecond UTC timestamp did not round-trip exactly"
    if not snapshots:
        return True, "no certified lineages require DML permission probing"
    lineage: GoldLineage = snapshots[0].lineage
    table = _quoted_table(POSTGRES_CONSUMER_SCHEMA, consumer_table_name(lineage.dataset_id))
    cursor.execute(sql.SQL("UPDATE {} SET exchange = exchange WHERE FALSE").format(table))
    try:
        cursor.execute(
            sql.SQL("CREATE TABLE {}.conformance_forbidden_probe (id integer)").format(
                sql.Identifier(POSTGRES_CONSUMER_SCHEMA)
            )
        )
    except psycopg.errors.InsufficientPrivilege:
        return True, "rollback-only DML is allowed and runtime DDL is denied"
    return False, "runtime role unexpectedly has application-schema DDL permission"


def verify_live_postgres(*, gold_root: Path, config: PostgresSyncConfig, report_path: Path) -> LiveConformanceReport:
    """Inspect the configured target and write a secret-free PASS/FAIL evidence report."""

    checks: list[ConformanceCheck] = []
    snapshots: tuple[Any, ...] = ()
    try:
        snapshots, expected = _expected_catalog(gold_root)
        _check(checks, "certified-current-gold", True, "source", "current Gold artifacts are attested")
    except Exception:
        _check(checks, "certified-current-gold", False, "source", "current Gold artifacts cannot be certified")
        expected = ()
    if expected:
        try:
            with psycopg.connect(
                host=config.host,
                port=config.port,
                user=config.user,
                dbname=config.database,
                password=config.password,
                autocommit=False,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SET TIME ZONE 'UTC'")
                    passed, detail = _actual_catalog(cursor, expected)
                    _check(checks, "owned-catalog", passed, "catalog", detail)
                    passed, detail = _runtime_contract(cursor)
                    _check(checks, "runtime-contract", passed, "role", detail)
                    for snapshot in snapshots:
                        passed, detail = _verify_lineage(cursor, snapshot)
                        name = (
                            f"lineage:{snapshot.lineage.dataset_id}:"
                            f"{snapshot.lineage.exchange}:{snapshot.lineage.symbol}"
                        )
                        _check(checks, name, passed, "data", detail)
                    passed, detail = _rollback_probes(cursor, snapshots)
                    _check(checks, "rollback-only-temporal-permission-probes", passed, "temporal-permission", detail)
                connection.rollback()
        except Exception as exc:
            _check(
                checks,
                "postgres-connection",
                False,
                "postgresql",
                f"{type(exc).__name__} during read-only verification",
            )
    status = "PASS" if checks and all(check.passed for check in checks) else "FAIL"
    report = LiveConformanceReport(
        report_version=_REPORT_VERSION,
        status=status,
        generated_at_utc=datetime.now(UTC).isoformat(),
        endpoint={"host": config.host, "port": config.port, "database": config.database, "user": config.user},
        checks=tuple(checks),
        lineage_count=len(snapshots),
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report.payload(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report
