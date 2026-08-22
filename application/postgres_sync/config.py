"""Typed protected runtime configuration for PostgreSQL Gold synchronization."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

from application.postgres_sync.contracts import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_ROLE

_REQUIRED_KEYS = ("PGHOST", "PGPORT", "PGUSER", "PGDATABASE", "PGPASSWORD")


@dataclass(frozen=True, slots=True)
class PostgresSyncConfig:
    """Validated application connection settings with redacted password representation."""

    host: str
    port: int
    user: str
    database: str
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        if self.host != POSTGRES_HOST:
            raise ValueError(f"PGHOST must be {POSTGRES_HOST}")
        if self.port != POSTGRES_PORT:
            raise ValueError(f"PGPORT must be {POSTGRES_PORT}")
        if self.user != POSTGRES_ROLE:
            raise ValueError(f"PGUSER must be {POSTGRES_ROLE}")
        if not self.database.strip():
            raise ValueError("PGDATABASE must not be empty")
        if not self.password:
            raise ValueError("PGPASSWORD must not be empty")

    @classmethod
    def from_sources(
        cls,
        *,
        environment: Mapping[str, str] | None = None,
        runtime_values: Mapping[str, object] | None = None,
    ) -> PostgresSyncConfig:
        """Resolve runtime config with explicit environment values taking precedence."""

        env = os.environ if environment is None else environment
        runtime = {} if runtime_values is None else runtime_values
        resolved: dict[str, str] = {}
        for key in _REQUIRED_KEYS:
            env_value = env.get(key)
            if env_value is not None and str(env_value).strip():
                resolved[key] = str(env_value)
                continue
            runtime_value = runtime.get(key)
            if runtime_value is not None and str(runtime_value).strip():
                resolved[key] = str(runtime_value)
        missing = [key for key in _REQUIRED_KEYS if key not in resolved]
        if missing:
            raise ValueError(f"missing PostgreSQL runtime setting(s): {', '.join(missing)}")
        try:
            port = int(resolved["PGPORT"])
        except ValueError as exc:
            raise ValueError("PGPORT must be an integer") from exc
        return cls(
            host=resolved["PGHOST"].strip(),
            port=port,
            user=resolved["PGUSER"].strip(),
            database=resolved["PGDATABASE"].strip(),
            password=resolved["PGPASSWORD"],
        )

    @classmethod
    def from_env(cls) -> PostgresSyncConfig:
        """Resolve all settings from the current process environment."""

        return cls.from_sources(environment=os.environ)

    def as_env(self) -> dict[str, str]:
        """Return exactly the five standard application PG variables."""

        return {
            "PGHOST": self.host,
            "PGPORT": str(self.port),
            "PGUSER": self.user,
            "PGDATABASE": self.database,
            "PGPASSWORD": self.password,
        }

    def safe_dict(self) -> dict[str, object]:
        """Return a serialization-safe view that never includes the password."""

        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "database": self.database,
            "password": "<redacted>",
        }
