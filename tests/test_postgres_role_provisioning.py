from __future__ import annotations

from pathlib import Path

import pytest

from scripts.provision_postgres_sync_role import (
    APP_ROLE,
    CONSUMER_SCHEMA,
    POSTGRES_HOST,
    POSTGRES_PORT,
    SYNC_SCHEMA,
    ProvisioningConfig,
)


def _valid_env() -> dict[str, str]:
    return {
        "PGHOST": POSTGRES_HOST,
        "PGPORT": str(POSTGRES_PORT),
        "PGDATABASE": "market_data",
        "PGADMINUSER": "postgres-admin",
        "PGADMINPASSWORD": "fake-admin-secret",
        "PGPASSWORD": "fake-app-secret",
    }


def test_provisioning_config_exact_identity_and_redaction() -> None:
    config = ProvisioningConfig.from_mapping(_valid_env())
    assert config.host == "10.10.1.3"
    assert config.port == 54321
    assert config.database == "market_data"
    assert APP_ROLE == "crypto-history-loader"
    assert CONSUMER_SCHEMA == "crypto_history_gold"
    assert SYNC_SCHEMA == "crypto_history_sync"
    rendered = repr(config)
    assert "fake-admin-secret" not in rendered
    assert "fake-app-secret" not in rendered


@pytest.mark.parametrize("key", ["PGDATABASE", "PGADMINUSER", "PGADMINPASSWORD", "PGPASSWORD"])
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
    assert '"crypto-history-loader"' in sql_text
    assert '"crypto_history_gold"' in sql_text
    assert '"crypto_history_sync"' in sql_text
    for token in ("NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE", "NOREPLICATION", "NOBYPASSRLS"):
        assert token in sql_text
    assert "fake-admin-secret" not in sql_text
    assert "fake-app-secret" not in sql_text
