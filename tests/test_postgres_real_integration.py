"""Real PostgreSQL integration checks for required CI jobs."""

from __future__ import annotations

import os
import threading
import uuid
from datetime import UTC, datetime

import psycopg
import pytest
from psycopg import sql


def _test_dsn() -> str:
    dsn = os.getenv("TEST_POSTGRES_DSN", "").strip()
    if dsn:
        return dsn
    if os.getenv("CI"):
        pytest.fail("required CI PostgreSQL service is not configured")
    pytest.skip("TEST_POSTGRES_DSN is required for local real-PostgreSQL tests")


def test_real_postgres_temporal_transaction_lock_and_sync_operations() -> None:
    """Prove server behavior that connection fakes cannot model."""

    dsn = _test_dsn()
    timestamp = datetime(2026, 3, 29, 1, 59, 59, 654321, tzinfo=UTC)
    with psycopg.connect(dsn, autocommit=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.execute("SHOW TIME ZONE")
            assert cursor.fetchone() == ("UTC",)
            cursor.execute("CREATE SCHEMA IF NOT EXISTS crypto_loader_test")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS crypto_loader_sync_test")
            cursor.execute("DROP TABLE IF EXISTS crypto_loader_test.temporal_probe")
            cursor.execute("DROP TABLE IF EXISTS crypto_loader_sync_test.gold_sync_state")
            cursor.execute(
                """CREATE TABLE crypto_loader_test.temporal_probe (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timestamp_m1 TIMESTAMPTZ(6) NOT NULL,
                    row_sha256 CHAR(64) NOT NULL,
                    PRIMARY KEY (exchange, symbol, timestamp_m1)
                )"""
            )
            cursor.execute(
                "INSERT INTO crypto_loader_test.temporal_probe VALUES (%s, %s, %s, %s)",
                ("deribit", "BTC", timestamp, "a" * 64),
            )
            cursor.execute(
                "SELECT timestamp_m1, row_sha256 FROM crypto_loader_test.temporal_probe WHERE symbol = %s",
                ("BTC",),
            )
            assert cursor.fetchone() == (timestamp, "a" * 64)
            cursor.execute(
                """SELECT data_type, datetime_precision
                FROM information_schema.columns
                WHERE table_schema = 'crypto_loader_test'
                  AND table_name = 'temporal_probe'
                  AND column_name = 'timestamp_m1'"""
            )
            assert cursor.fetchone() == ("timestamp with time zone", 6)
            cursor.execute(
                """CREATE TABLE crypto_loader_sync_test.gold_sync_state (
                    dataset_id TEXT PRIMARY KEY,
                    synced_at_utc TIMESTAMPTZ(6) NOT NULL
                )"""
            )
            cursor.execute(
                "INSERT INTO crypto_loader_sync_test.gold_sync_state VALUES (%s, %s)",
                ("gold.history.full.m1", timestamp),
            )
            cursor.execute("SELECT synced_at_utc FROM crypto_loader_sync_test.gold_sync_state")
            assert cursor.fetchone() == (timestamp,)
            cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", (987654321,))
            assert cursor.fetchone() == (True,)
            with psycopg.connect(dsn, autocommit=False) as competing_connection:
                with competing_connection.cursor() as competing_cursor:
                    competing_cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", (987654321,))
                    assert competing_cursor.fetchone() == (False,)
                competing_connection.rollback()
        connection.commit()

    with psycopg.connect(dsn, autocommit=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO crypto_loader_test.temporal_probe VALUES (%s, %s, %s, %s)",
                ("deribit", "ETH", timestamp, "b" * 64),
            )
        connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM crypto_loader_test.temporal_probe WHERE symbol = 'ETH'")
            assert cursor.fetchone() == (0,)


def test_two_writers_replan_after_lock_and_rollback_target_with_checkpoint() -> None:
    """Prove a waiting writer reads committed state only after acquiring the lineage lock."""

    dsn = _test_dsn()
    table_name = f"reconcile_probe_{uuid.uuid4().hex}"
    table = sql.Identifier("crypto_loader_sync_test", table_name)
    lock_key = f"pr84|{table_name}"
    with psycopg.connect(dsn, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS crypto_loader_sync_test")
            cursor.execute(
                sql.SQL("CREATE TABLE {} (id INTEGER PRIMARY KEY, target_value INTEGER, checkpoint INTEGER)").format(
                    table
                )
            )
            cursor.execute(sql.SQL("INSERT INTO {} VALUES (1, 0, 0)").format(table))

    first_read = threading.Event()
    second_started = threading.Event()
    release_first = threading.Event()
    observations: list[tuple[str, int]] = []
    failures: list[BaseException] = []

    def writer(name: str) -> None:
        try:
            with psycopg.connect(dsn, autocommit=False) as connection:
                with connection.cursor() as cursor:
                    if name == "second":
                        second_started.set()
                    cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_key,))
                    cursor.execute(sql.SQL("SELECT target_value FROM {} WHERE id = 1").format(table))
                    row = cursor.fetchone()
                    assert row is not None
                    current = row[0]
                    assert isinstance(current, int)
                    observations.append((name, current))
                    if name == "first":
                        first_read.set()
                        assert release_first.wait(timeout=10)
                    next_value = current + 1
                    cursor.execute(
                        sql.SQL("UPDATE {} SET target_value = %s, checkpoint = %s WHERE id = 1").format(table),
                        (next_value, next_value),
                    )
                connection.commit()
        except BaseException as exc:
            failures.append(exc)

    first = threading.Thread(target=writer, args=("first",))
    second = threading.Thread(target=writer, args=("second",))
    first.start()
    assert first_read.wait(timeout=10)
    second.start()
    assert second_started.wait(timeout=10)
    release_first.set()
    first.join(timeout=10)
    second.join(timeout=10)

    try:
        assert not first.is_alive() and not second.is_alive()
        assert failures == []
        assert observations == [("first", 0), ("second", 1)]

        with psycopg.connect(dsn, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_key,))
                cursor.execute(sql.SQL("UPDATE {} SET target_value = 99, checkpoint = 99 WHERE id = 1").format(table))
            connection.rollback()
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("SELECT target_value, checkpoint FROM {} WHERE id = 1").format(table))
                assert cursor.fetchone() == (2, 2)
    finally:
        with psycopg.connect(dsn, autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table))
