"""Focused safety tests for the production PostgreSQL conformance verifier."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

import api.commands.postgres_conformance as postgres_conformance_cmd
from application.postgres_sync import live_conformance as conformance
from application.postgres_sync.config import PostgresSyncConfig
from application.postgres_sync.schema import PostgresCatalogColumn


class _Cursor:
    """Minimal recorded cursor for deterministic verifier helper tests."""

    def __init__(self, responses: list[list[tuple[object, ...]]]) -> None:
        self.responses = iter(responses)
        self.description: tuple[SimpleNamespace, ...] | None = None

    def execute(self, _: object, __: object = None) -> None:
        return None

    def fetchall(self) -> list[tuple[object, ...]]:
        return next(self.responses)

    def fetchone(self) -> tuple[object, ...] | None:
        rows = self.fetchall()
        return rows[0] if rows else None

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_: object) -> bool:
        return False


class _Connection:
    """Context-managed fake connection that records the required rollback."""

    def __init__(self, cursor: _Cursor) -> None:
        self.cursor_instance = cursor
        self.rollback_calls = 0

    def __enter__(self) -> _Connection:
        return self

    def __exit__(self, *_: object) -> bool:
        return False

    def cursor(self) -> _Cursor:
        return self.cursor_instance

    def rollback(self) -> None:
        self.rollback_calls += 1


class _ProbeCursor:
    """Minimal cursor that controls timestamp and DDL-probe outcomes."""

    def __init__(self, *, timestamp: datetime, deny_ddl: bool) -> None:
        self.timestamp = timestamp
        self.deny_ddl = deny_ddl
        self.statements: list[str] = []

    def execute(self, statement: object, _: object = None) -> None:
        rendered = str(statement)
        self.statements.append(rendered)
        if self.deny_ddl and "CREATE TABLE" in rendered:
            raise psycopg.errors.InsufficientPrivilege()

    def fetchone(self) -> tuple[object, ...]:
        return (self.timestamp,)


def _config() -> PostgresSyncConfig:
    return PostgresSyncConfig(
        host="10.10.1.3",
        port=54321,
        user="crypto-loader",
        database="crypto_loader_test",
        password="never-write-this-secret",
    )


@pytest.mark.parametrize(("status", "expected_return_code"), [("PASS", 0), ("FAIL", 1)])
def test_run_postgres_live_conformance_returns_status_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    status: str,
    expected_return_code: int,
) -> None:
    """The CLI adapter maps only a passing conformance report to success."""

    report = SimpleNamespace(
        status=status,
        lineage_count=0,
        payload=lambda: {"lineage_count": 0, "status": status},
    )
    monkeypatch.setattr(postgres_conformance_cmd.PostgresSyncConfig, "from_env", lambda: _config())
    monkeypatch.setattr(postgres_conformance_cmd, "verify_live_postgres", lambda **_: report)

    return_code = postgres_conformance_cmd.run_postgres_live_conformance(
        argparse.Namespace(gold_root="lake/gold", report_file="artifacts/evidence.json"),
        logging.getLogger("test"),
    )

    assert return_code == expected_return_code
    assert json.loads(capsys.readouterr().out) == {"lineage_count": 0, "status": status}


def test_run_postgres_live_conformance_sanitizes_initialization_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Config initialization failures expose no credentials or internal detail."""

    monkeypatch.setattr(
        postgres_conformance_cmd.PostgresSyncConfig,
        "from_env",
        lambda: (_ for _ in ()).throw(ValueError("PGPASSWORD=secret-value")),
    )

    with pytest.raises(RuntimeError) as exc_info:
        postgres_conformance_cmd.run_postgres_live_conformance(
            argparse.Namespace(gold_root="lake/gold", report_file="artifacts/evidence.json"),
            logging.getLogger("test"),
        )

    assert str(exc_info.value) == "postgres-live-conformance could not initialize"
    assert "secret-value" not in str(exc_info.value)


def test_verifier_writes_sanitized_fail_report_when_gold_cannot_be_certified(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Source-certification failures are evidence, not an unsafe partial verification."""

    def fail_expected_catalog(_: Path) -> tuple[tuple[object, ...], tuple[object, ...]]:
        raise ValueError("malformed source artifact")

    monkeypatch.setattr(conformance, "_expected_catalog", fail_expected_catalog)
    report_path = tmp_path / "evidence.json"

    report = conformance.verify_live_postgres(
        gold_root=tmp_path / "gold",
        config=_config(),
        report_path=report_path,
    )

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    serialized = json.dumps(payload, sort_keys=True)
    assert report.status == "FAIL"
    assert payload["status"] == "FAIL"
    assert payload["endpoint"]["user"] == "crypto-loader"
    assert "never-write-this-secret" not in serialized
    assert "malformed source artifact" not in serialized
    assert payload["checks"] == [
        {
            "category": "source",
            "detail": "current Gold artifacts cannot be certified",
            "name": "certified-current-gold",
            "passed": False,
        }
    ]


def test_utc_rejects_naive_and_non_utc_values() -> None:
    """The live verifier preserves the strict database UTC boundary."""

    with pytest.raises(ValueError, match="aware UTC"):
        conformance._utc(__import__("datetime").datetime(2026, 1, 1))


def test_actual_catalog_accepts_exact_owned_table_contract() -> None:
    """Catalog verification accepts matching tables, columns, and primary keys."""

    table = conformance.PostgresCatalogTable(
        schema_name="crypto_loader",
        table_name="gold_spot",
        columns=(PostgresCatalogColumn("exchange", "text", False),),
        primary_key=("exchange",),
    )
    cursor = _Cursor(
        [
            [("crypto_loader", "gold_spot")],
            [("exchange", "text", "NO", None, None, None, None)],
            [("exchange",)],
        ]
    )

    assert conformance._actual_catalog(cursor, (table,)) == (
        True,
        "owned catalog exactly matches the certified Gold contract",
    )


def test_actual_catalog_rejects_unexpected_owned_table() -> None:
    """Catalog verification fails closed when an owned table is missing or extra."""

    cursor = _Cursor([[("crypto_loader", "unexpected")]])

    assert conformance._actual_catalog(cursor, ()) == (
        False,
        "owned table set differs from the certified Gold contract",
    )


def test_verify_lineage_requires_matching_source_target_digests_and_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lineage verification accepts only an exact source, consumer, and state match."""

    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    snapshot = SimpleNamespace(
        lineage=conformance.GoldLineage("gold.live.full.m1", "deribit", "BTC"),
        source_fingerprint="source",
        schema_signature="schema",
        row_count=1,
        min_timestamp=timestamp,
        max_timestamp=timestamp,
        source_version="v1",
        build_id="build",
    )
    expected = {("deribit", "BTC", timestamp): "digest"}
    monkeypatch.setattr(conformance, "_source_digest_map", lambda _: expected)
    monkeypatch.setattr(conformance, "_target_digest_map", lambda _cursor, _: (expected, expected))
    cursor = _Cursor([[("source", "schema", 1, timestamp, timestamp, "v1", "build")]])

    assert conformance._verify_lineage(cursor, snapshot) == (
        True,
        "source, consumer, digests, and checkpoint are exactly equivalent",
    )


def test_verify_lineage_rejects_same_count_payload_tampering(monkeypatch: pytest.MonkeyPatch) -> None:
    """A changed digest cannot pass merely because the logical key set is unchanged."""

    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    snapshot = SimpleNamespace(lineage=conformance.GoldLineage("gold.live.full.m1", "deribit", "BTC"))
    key = ("deribit", "BTC", timestamp)
    monkeypatch.setattr(conformance, "_source_digest_map", lambda _: {key: "expected"})
    monkeypatch.setattr(conformance, "_target_digest_map", lambda _cursor, _: ({key: "tampered"}, {key: "expected"}))

    assert conformance._verify_lineage(_Cursor([]), snapshot) == (False, "source and consumer row digests differ")


def test_target_digest_map_reads_consumer_and_persisted_hashes() -> None:
    """The verifier compares consumer rows against the persisted digest index."""

    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    cursor = _Cursor(
        [
            [("deribit", "BTC", timestamp)],
            [(timestamp, "persisted-digest")],
        ]
    )
    cursor.description = tuple(SimpleNamespace(name=name) for name in ("exchange", "symbol", "timestamp_m1"))
    snapshot = SimpleNamespace(lineage=conformance.GoldLineage("gold.live.full.m1", "deribit", "BTC"))

    target, persisted = conformance._target_digest_map(cursor, snapshot)

    key = ("deribit", "BTC", timestamp)
    assert target.keys() == {key}
    assert persisted == {key: "persisted-digest"}


def test_runtime_contract_accepts_dml_only_utc_runtime_role() -> None:
    """A runtime role must have bounded sessions and no schema-creation privilege."""

    cursor = _Cursor(
        [
            [(False, False, False, False, False, True)],
            [("UTC",)],
            [("30s",)],
            [("5s",)],
            [("1min",)],
            [("crypto_loader", "postgres"), ("crypto_loader_sync", "postgres")],
            [(True,)],
            [(False,)],
            [(True,)],
            [(False,)],
        ]
    )

    assert conformance._runtime_contract(cursor) == (
        True,
        "runtime role, UTC session, timeouts, and schema privileges conform",
    )


def test_verify_live_postgres_writes_pass_report_and_rolls_back(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A fully conforming target produces complete PASS evidence after rollback."""

    snapshot = SimpleNamespace(lineage=conformance.GoldLineage("gold.live.full.m1", "deribit", "BTC"))
    cursor = _Cursor([])
    connection = _Connection(cursor)
    monkeypatch.setattr(conformance, "_expected_catalog", lambda _: ((snapshot,), (object(),)))
    monkeypatch.setattr(conformance.psycopg, "connect", lambda **_: connection)
    monkeypatch.setattr(conformance, "_actual_catalog", lambda *_: (True, "catalog conforms"))
    monkeypatch.setattr(conformance, "_runtime_contract", lambda _: (True, "role conforms"))
    monkeypatch.setattr(conformance, "_verify_lineage", lambda *_: (True, "lineage conforms"))
    monkeypatch.setattr(conformance, "_rollback_probes", lambda *_: (True, "probes conform"))

    report = conformance.verify_live_postgres(
        gold_root=tmp_path / "gold", config=_config(), report_path=tmp_path / "evidence.json"
    )

    assert report.status == "PASS"
    assert connection.rollback_calls == 1
    assert [(check.name, check.passed) for check in report.checks] == [
        ("certified-current-gold", True),
        ("owned-catalog", True),
        ("runtime-contract", True),
        ("lineage:gold.live.full.m1:deribit:BTC", True),
        ("rollback-only-temporal-permission-probes", True),
    ]


def test_verify_live_postgres_writes_sanitized_connection_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Connection exceptions become a secret-free FAIL report instead of escaping."""

    monkeypatch.setattr(conformance, "_expected_catalog", lambda _: ((object(),), (object(),)))
    monkeypatch.setattr(
        conformance.psycopg,
        "connect",
        lambda **_: (_ for _ in ()).throw(ConnectionError("password=never-write-this-secret")),
    )

    report = conformance.verify_live_postgres(
        gold_root=tmp_path / "gold", config=_config(), report_path=tmp_path / "evidence.json"
    )

    assert report.status == "FAIL"
    assert [(check.name, check.detail) for check in report.checks] == [
        ("certified-current-gold", "current Gold artifacts are attested"),
        ("postgres-connection", "ConnectionError during read-only verification"),
    ]
    assert "never-write-this-secret" not in (tmp_path / "evidence.json").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("target", "check_name", "detail"),
    [
        ("_actual_catalog", "owned-catalog", "catalog differs"),
        ("_runtime_contract", "runtime-contract", "role differs"),
        ("_verify_lineage", "lineage:gold.live.full.m1:deribit:BTC", "lineage differs"),
        ("_rollback_probes", "rollback-only-temporal-permission-probes", "probe differs"),
    ],
)
def test_verify_live_postgres_records_component_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    target: str,
    check_name: str,
    detail: str,
) -> None:
    """Catalog, role, lineage, and probe failures each make the complete report fail."""

    snapshot = SimpleNamespace(lineage=conformance.GoldLineage("gold.live.full.m1", "deribit", "BTC"))
    connection = _Connection(_Cursor([]))
    monkeypatch.setattr(conformance, "_expected_catalog", lambda _: ((snapshot,), (object(),)))
    monkeypatch.setattr(conformance.psycopg, "connect", lambda **_: connection)
    monkeypatch.setattr(conformance, "_actual_catalog", lambda *_: (True, "catalog conforms"))
    monkeypatch.setattr(conformance, "_runtime_contract", lambda _: (True, "role conforms"))
    monkeypatch.setattr(conformance, "_verify_lineage", lambda *_: (True, "lineage conforms"))
    monkeypatch.setattr(conformance, "_rollback_probes", lambda *_: (True, "probes conform"))
    monkeypatch.setattr(conformance, target, lambda *_: (False, detail))

    report = conformance.verify_live_postgres(
        gold_root=tmp_path / "gold", config=_config(), report_path=tmp_path / "evidence.json"
    )

    assert report.status == "FAIL"
    assert next(check for check in report.checks if check.name == check_name) == conformance.ConformanceCheck(
        check_name, False, next(check.category for check in report.checks if check.name == check_name), detail
    )
    assert connection.rollback_calls == 1


@pytest.mark.parametrize(
    ("timestamp", "has_snapshot", "deny_ddl", "expected"),
    [
        (
            datetime(2026, 10, 25, 1, 59, 59, 654321, tzinfo=UTC),
            False,
            False,
            (True, "no certified lineages require DML permission probing"),
        ),
        (
            datetime(2026, 10, 25, 1, 59, 59, 654321, tzinfo=UTC),
            True,
            True,
            (True, "rollback-only DML is allowed and runtime DDL is denied"),
        ),
        (
            datetime(2026, 10, 25, 1, 59, 59, 654321, tzinfo=UTC),
            True,
            False,
            (False, "runtime role unexpectedly has application-schema DDL permission"),
        ),
        (
            datetime(2026, 10, 25, 1, 59, 59, 654320, tzinfo=UTC),
            False,
            False,
            (False, "microsecond UTC timestamp did not round-trip exactly"),
        ),
    ],
)
def test_rollback_probes_cover_timestamp_dml_and_ddl_outcomes(
    timestamp: datetime, has_snapshot: bool, deny_ddl: bool, expected: tuple[bool, str]
) -> None:
    """Rollback probes enforce precision and distinguish allowed DML from forbidden DDL."""

    cursor = _ProbeCursor(timestamp=timestamp, deny_ddl=deny_ddl)
    snapshots: tuple[object, ...] = ()
    if has_snapshot:
        snapshots = (SimpleNamespace(lineage=conformance.GoldLineage("gold.live.full.m1", "deribit", "BTC")),)

    assert conformance._rollback_probes(cursor, snapshots) == expected
