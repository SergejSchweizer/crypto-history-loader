"""Real PostgreSQL integration checks for required CI jobs."""

from __future__ import annotations

import os
from datetime import UTC, datetime

import psycopg
import pytest


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
