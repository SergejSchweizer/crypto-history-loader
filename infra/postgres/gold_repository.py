"""Psycopg repository adapter for the rebuildable Gold serving-plane replica."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol, cast

import psycopg

from application.postgres_sync import (
    POSTGRES_CONSUMER_SCHEMA,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_ROLE,
    POSTGRES_ROW_HASH_TABLE,
    POSTGRES_SESSION_TIMEZONE,
    POSTGRES_SYNC_SCHEMA,
    POSTGRES_SYNC_STATE_TABLE,
    POSTGRES_TIMESTAMP_TYPE,
    GoldDeltaPlan,
    GoldLineage,
    GoldRowDigest,
    GoldRowKey,
    GoldSourceSnapshot,
    GoldSyncState,
    GoldTargetSummary,
    consumer_table_name,
)
from application.postgres_sync.schema import quote_identifier


class PostgresGoldRepositoryError(RuntimeError):
    """Sanitized PostgreSQL adapter failure."""


class PostgresSchemaMismatchError(PostgresGoldRepositoryError):
    """Existing target schema/checkpoint is incompatible with the current source."""


class CursorPort(Protocol):
    def execute(self, query: str, params: Sequence[object] | None = None) -> object: ...

    def fetchone(self) -> tuple[object, ...] | None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...

    def close(self) -> None: ...


class ConnectionPort(Protocol):
    def cursor(self) -> CursorPort: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PostgresConnectionSettings:
    """Validated application connection settings with password redacted from repr."""

    host: str
    port: int
    user: str
    database: str
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        if self.host != POSTGRES_HOST or self.port != POSTGRES_PORT or self.user != POSTGRES_ROLE:
            raise ValueError("PostgreSQL Gold repository endpoint/user identity is invalid")
        if not self.database.strip():
            raise ValueError("PostgreSQL database must not be empty")
        if not self.password:
            raise ValueError("PostgreSQL password must not be empty")


ConnectionFactory = Callable[[PostgresConnectionSettings], ConnectionPort]


def _default_connection(settings: PostgresConnectionSettings) -> ConnectionPort:
    connection = psycopg.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        dbname=settings.database,
        password=settings.password,
        autocommit=False,
    )
    return cast(ConnectionPort, connection)


def _qualified(schema_name: str, table_name: str) -> str:
    return f"{quote_identifier(schema_name)}.{quote_identifier(table_name)}"


_STATE = _qualified(POSTGRES_SYNC_SCHEMA, POSTGRES_SYNC_STATE_TABLE)
_DIGESTS = _qualified(POSTGRES_SYNC_SCHEMA, POSTGRES_ROW_HASH_TABLE)
_STATE_DDL = f"""CREATE TABLE IF NOT EXISTS {_STATE} (
    dataset_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    schema_signature CHAR(64) NOT NULL,
    row_count BIGINT NOT NULL,
    min_timestamp {POSTGRES_TIMESTAMP_TYPE} NULL,
    max_timestamp {POSTGRES_TIMESTAMP_TYPE} NULL,
    synced_at_utc {POSTGRES_TIMESTAMP_TYPE} NOT NULL,
    source_version TEXT NULL,
    build_id TEXT NULL,
    PRIMARY KEY (dataset_id, exchange, symbol)
)"""
_DIGEST_DDL = f"""CREATE TABLE IF NOT EXISTS {_DIGESTS} (
    dataset_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timestamp_m1 {POSTGRES_TIMESTAMP_TYPE} NOT NULL,
    row_sha256 CHAR(64) NOT NULL,
    PRIMARY KEY (dataset_id, exchange, symbol, timestamp_m1)
)"""
_SELECT_STATE = f"""SELECT dataset_id, exchange, symbol, source_fingerprint, schema_signature, row_count,
min_timestamp, max_timestamp, synced_at_utc, source_version, build_id
FROM {_STATE} WHERE dataset_id = %s AND exchange = %s AND symbol = %s"""
_SELECT_DIGESTS = f"""SELECT timestamp_m1, row_sha256 FROM {_DIGESTS}
WHERE dataset_id = %s AND exchange = %s AND symbol = %s ORDER BY timestamp_m1"""
_UPSERT_STATE = f"""INSERT INTO {_STATE} (
    dataset_id, exchange, symbol, source_fingerprint, schema_signature, row_count,
    min_timestamp, max_timestamp, synced_at_utc, source_version, build_id
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (dataset_id, exchange, symbol) DO UPDATE SET
    source_fingerprint = EXCLUDED.source_fingerprint,
    schema_signature = EXCLUDED.schema_signature,
    row_count = EXCLUDED.row_count,
    min_timestamp = EXCLUDED.min_timestamp,
    max_timestamp = EXCLUDED.max_timestamp,
    synced_at_utc = EXCLUDED.synced_at_utc,
    source_version = EXCLUDED.source_version,
    build_id = EXCLUDED.build_id"""
_UPSERT_DIGEST = f"""INSERT INTO {_DIGESTS} (dataset_id, exchange, symbol, timestamp_m1, row_sha256)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (dataset_id, exchange, symbol, timestamp_m1)
DO UPDATE SET row_sha256 = EXCLUDED.row_sha256"""
_DELETE_DIGEST = f"DELETE FROM {_DIGESTS} WHERE dataset_id = %s AND exchange = %s AND symbol = %s AND timestamp_m1 = %s"


def _lineage_params(lineage: GoldLineage) -> tuple[object, ...]:
    return (lineage.dataset_id, lineage.exchange, lineage.symbol)


def _as_text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"PostgreSQL {name} must be text")
    return value


def _as_optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _as_text(value, name)


def _as_int(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"PostgreSQL {name} must be integer")
    return value


def _as_datetime(value: object, name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"PostgreSQL {name} must be datetime")
    return value


def _state_from_row(row: tuple[object, ...]) -> GoldSyncState:
    if len(row) != 11:
        raise ValueError("PostgreSQL Gold sync state row has unexpected width")
    synced = _as_datetime(row[8], "synced_at_utc")
    if synced is None:
        raise ValueError("PostgreSQL synced_at_utc cannot be null")
    return GoldSyncState(
        lineage=GoldLineage(_as_text(row[0], "dataset_id"), _as_text(row[1], "exchange"), _as_text(row[2], "symbol")),
        source_fingerprint=_as_text(row[3], "source_fingerprint"),
        schema_signature=_as_text(row[4], "schema_signature"),
        row_count=_as_int(row[5], "row_count"),
        min_timestamp=_as_datetime(row[6], "min_timestamp"),
        max_timestamp=_as_datetime(row[7], "max_timestamp"),
        synced_at_utc=synced,
        source_version=_as_optional_text(row[9], "source_version"),
        build_id=_as_optional_text(row[10], "build_id"),
    )


def _summary_from_row(row: tuple[object, ...]) -> GoldTargetSummary:
    if len(row) != 3:
        raise ValueError("PostgreSQL Gold target summary row has unexpected width")
    return GoldTargetSummary(
        row_count=_as_int(row[0], "row_count"),
        min_timestamp=_as_datetime(row[1], "min_timestamp"),
        max_timestamp=_as_datetime(row[2], "max_timestamp"),
    )


def _state_params(state: GoldSyncState) -> tuple[object, ...]:
    lineage = state.lineage
    return (
        lineage.dataset_id,
        lineage.exchange,
        lineage.symbol,
        state.source_fingerprint,
        state.schema_signature,
        state.row_count,
        state.min_timestamp,
        state.max_timestamp,
        state.synced_at_utc,
        state.source_version,
        state.build_id,
    )


def _safe_table_ddl(ddl: str) -> str:
    upper = ddl.upper()
    for forbidden in ("DROP ", "TRUNCATE ", "ALTER TABLE", "DELETE FROM"):
        if forbidden in upper:
            raise ValueError(f"forbidden normal-sync DDL token: {forbidden.strip()}")
    statements = [statement.strip() for statement in ddl.split(";") if statement.strip()]
    table_statements = [statement for statement in statements if statement.upper().startswith("CREATE TABLE")]
    if len(table_statements) != 1:
        raise ValueError("Gold consumer DDL must contain exactly one CREATE TABLE statement")
    return table_statements[0]


class PostgresGoldSyncRepository:
    """Transactional repository adapter implementing one-lineage atomic reconciliation."""

    def __init__(
        self,
        settings: PostgresConnectionSettings,
        *,
        connection_factory: ConnectionFactory = _default_connection,
    ) -> None:
        self._settings = settings
        self._connection_factory = connection_factory

    def _open(self) -> ConnectionPort:
        try:
            connection = self._connection_factory(self._settings)
            cursor = connection.cursor()
            try:
                cursor.execute(f"SET TIME ZONE '{POSTGRES_SESSION_TIMEZONE}'")
            finally:
                cursor.close()
            return connection
        except Exception:
            raise PostgresGoldRepositoryError("PostgreSQL connection initialization failed") from None

    def ensure_lineage(self, snapshot: GoldSourceSnapshot, ddl: str, schema_signature: str) -> None:
        """Create/validate target tables before any consumer-row mutation."""

        table_ddl = _safe_table_ddl(ddl)
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(_STATE_DDL)
                cursor.execute(_DIGEST_DDL)
                cursor.execute(table_ddl)
                cursor.execute(_SELECT_STATE, _lineage_params(snapshot.lineage))
                row = cursor.fetchone()
                if row is not None:
                    existing = _state_from_row(row)
                    if existing.schema_signature != schema_signature:
                        raise PostgresSchemaMismatchError("PostgreSQL Gold schema migration required")
            finally:
                cursor.close()
            connection.commit()
        except PostgresSchemaMismatchError:
            connection.rollback()
            raise
        except Exception:
            connection.rollback()
            raise PostgresGoldRepositoryError("PostgreSQL Gold schema initialization failed") from None
        finally:
            connection.close()

    def read_state(self, lineage: GoldLineage) -> GoldSyncState | None:
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(_SELECT_STATE, _lineage_params(lineage))
                row = cursor.fetchone()
            finally:
                cursor.close()
            return None if row is None else _state_from_row(row)
        except Exception:
            raise PostgresGoldRepositoryError("PostgreSQL Gold sync-state read failed") from None
        finally:
            connection.close()

    def read_digests(self, lineage: GoldLineage) -> tuple[GoldRowDigest, ...]:
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(_SELECT_DIGESTS, _lineage_params(lineage))
                rows = cursor.fetchall()
            finally:
                cursor.close()
            result: list[GoldRowDigest] = []
            for row in rows:
                if len(row) != 2:
                    raise ValueError("PostgreSQL Gold digest row has unexpected width")
                timestamp = _as_datetime(row[0], "timestamp_m1")
                if timestamp is None:
                    raise ValueError("PostgreSQL Gold digest timestamp cannot be null")
                result.append(
                    GoldRowDigest(
                        GoldRowKey(lineage.exchange, lineage.symbol, timestamp),
                        _as_text(row[1], "row_sha256"),
                    )
                )
            return tuple(result)
        except Exception:
            raise PostgresGoldRepositoryError("PostgreSQL Gold digest read failed") from None
        finally:
            connection.close()

    def _summary_sql(self, lineage: GoldLineage) -> tuple[str, tuple[object, ...]]:
        table = _qualified(POSTGRES_CONSUMER_SCHEMA, consumer_table_name(lineage.dataset_id))
        query = (
            f"SELECT COUNT(*), MIN({quote_identifier('timestamp_m1')}), MAX({quote_identifier('timestamp_m1')}) "
            f"FROM {table} WHERE {quote_identifier('exchange')} = %s AND {quote_identifier('symbol')} = %s"
        )
        return query, (lineage.exchange, lineage.symbol)

    def summary(self, lineage: GoldLineage) -> GoldTargetSummary:
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                query, params = self._summary_sql(lineage)
                cursor.execute(query, params)
                row = cursor.fetchone()
            finally:
                cursor.close()
            if row is None:
                raise ValueError("PostgreSQL Gold summary query returned no row")
            return _summary_from_row(row)
        except Exception:
            raise PostgresGoldRepositoryError("PostgreSQL Gold summary read failed") from None
        finally:
            connection.close()

    @staticmethod
    def _digest_map(plan: GoldDeltaPlan) -> dict[GoldRowKey, str]:
        result: dict[GoldRowKey, str] = {}
        for digest in plan.source_digests:
            if digest.key in result:
                raise ValueError("duplicate source digest in Gold delta plan")
            result[digest.key] = digest.row_sha256
        return result

    def _insert_row(self, cursor: CursorPort, lineage: GoldLineage, values: tuple[tuple[str, object], ...]) -> None:
        table = _qualified(POSTGRES_CONSUMER_SCHEMA, consumer_table_name(lineage.dataset_id))
        columns = [name for name, _ in values]
        query = (
            f"INSERT INTO {table} ({', '.join(quote_identifier(name) for name in columns)}) VALUES "
            f"({', '.join('%s' for _ in columns)})"
        )
        cursor.execute(query, tuple(value for _, value in values))

    def _update_row(self, cursor: CursorPort, lineage: GoldLineage, values: tuple[tuple[str, object], ...]) -> None:
        table = _qualified(POSTGRES_CONSUMER_SCHEMA, consumer_table_name(lineage.dataset_id))
        mapping = dict(values)
        mutable = [(name, value) for name, value in values if name not in {"exchange", "symbol", "timestamp_m1"}]
        if not mutable:
            raise ValueError("Gold update contains no mutable payload columns")
        query = (
            f"UPDATE {table} SET {', '.join(f'{quote_identifier(name)} = %s' for name, _ in mutable)} "
            f"WHERE {quote_identifier('exchange')} = %s AND {quote_identifier('symbol')} = %s "
            f"AND {quote_identifier('timestamp_m1')} = %s"
        )
        params = tuple(value for _, value in mutable) + (
            mapping["exchange"],
            mapping["symbol"],
            mapping["timestamp_m1"],
        )
        cursor.execute(query, params)

    def _delete_row(self, cursor: CursorPort, lineage: GoldLineage, key: GoldRowKey) -> None:
        table = _qualified(POSTGRES_CONSUMER_SCHEMA, consumer_table_name(lineage.dataset_id))
        query = (
            f"DELETE FROM {table} WHERE {quote_identifier('exchange')} = %s "
            f"AND {quote_identifier('symbol')} = %s AND {quote_identifier('timestamp_m1')} = %s"
        )
        cursor.execute(query, (key.exchange, key.symbol, key.timestamp_m1))

    def apply_delta(self, lineage: GoldLineage, plan: GoldDeltaPlan, state: GoldSyncState) -> None:
        """Apply exactly one supplied lineage delta atomically under an advisory lock."""

        if state.lineage != lineage:
            raise ValueError("Gold sync state lineage does not match requested lineage")
        digest_map = self._digest_map(plan)
        for payload in (*plan.inserts, *plan.updates):
            if payload.key not in digest_map:
                raise ValueError("Gold delta mutation is missing its source digest")

        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                lock_key = f"{lineage.dataset_id}|{lineage.exchange}|{lineage.symbol}"
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_key,))

                for payload in plan.inserts:
                    self._insert_row(cursor, lineage, payload.values)
                for payload in plan.updates:
                    self._update_row(cursor, lineage, payload.values)
                for key in plan.deletes:
                    self._delete_row(cursor, lineage, key)

                for payload in (*plan.inserts, *plan.updates):
                    cursor.execute(
                        _UPSERT_DIGEST,
                        (
                            lineage.dataset_id,
                            lineage.exchange,
                            lineage.symbol,
                            payload.key.timestamp_m1,
                            digest_map[payload.key],
                        ),
                    )
                for key in plan.deletes:
                    cursor.execute(
                        _DELETE_DIGEST,
                        (lineage.dataset_id, lineage.exchange, lineage.symbol, key.timestamp_m1),
                    )

                cursor.execute(_UPSERT_STATE, _state_params(state))
                summary_query, summary_params = self._summary_sql(lineage)
                cursor.execute(summary_query, summary_params)
                summary_row = cursor.fetchone()
                if summary_row is None:
                    raise ValueError("PostgreSQL Gold verification query returned no row")
                actual = _summary_from_row(summary_row)
                expected = GoldTargetSummary(state.row_count, state.min_timestamp, state.max_timestamp)
                if actual != expected:
                    raise ValueError("PostgreSQL Gold post-write summary does not match source")
            finally:
                cursor.close()
            connection.commit()
        except Exception:
            connection.rollback()
            raise PostgresGoldRepositoryError("PostgreSQL Gold delta transaction failed") from None
        finally:
            connection.close()
