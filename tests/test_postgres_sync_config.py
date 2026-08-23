from __future__ import annotations

import pytest

from application.postgres_sync.config import PostgresSyncConfig


def _runtime() -> dict[str, object]:
    return {
        "PGHOST": "10.10.1.3",
        "PGPORT": "54321",
        "PGUSER": "crypto-loader",
        "PGDATABASE": "market_data",
        "PGPASSWORD": "fake-runtime-secret",
    }


def test_runtime_only_configuration() -> None:
    config = PostgresSyncConfig.from_sources(environment={}, runtime_values=_runtime())
    assert config.host == "10.10.1.3"
    assert config.port == 54321
    assert config.user == "crypto-loader"
    assert config.database == "market_data"
    assert config.as_env() == {key: str(value) for key, value in _runtime().items()}
    assert "fake-runtime-secret" not in repr(config)
    assert config.safe_dict()["password"] == "<redacted>"


def test_environment_overrides_runtime_values() -> None:
    runtime = _runtime()
    runtime["PGDATABASE"] = "runtime_db"
    config = PostgresSyncConfig.from_sources(environment={"PGDATABASE": "env_db"}, runtime_values=runtime)
    assert config.database == "env_db"


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("PGHOST", "localhost"),
        ("PGPORT", "5432"),
        ("PGUSER", "postgres"),
        ("PGDATABASE", ""),
        ("PGPASSWORD", ""),
    ],
)
def test_invalid_identity_or_missing_value_fails(key: str, value: str) -> None:
    runtime = _runtime()
    runtime[key] = value
    with pytest.raises(ValueError):
        PostgresSyncConfig.from_sources(environment={}, runtime_values=runtime)


def test_shell_special_password_is_not_rendered() -> None:
    runtime = _runtime()
    runtime["PGPASSWORD"] = "fake-$()'\" secret"
    config = PostgresSyncConfig.from_sources(environment={}, runtime_values=runtime)
    assert config.as_env()["PGPASSWORD"] == "fake-$()'\" secret"
    assert "fake-$()" not in repr(config)
