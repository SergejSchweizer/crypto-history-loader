"""Runtime policy readers for Deribit trade adapters."""

from __future__ import annotations

import os

from ingestion.exchanges.deribit_trade_common import env_float_non_negative, env_int_min

DEFAULT_INTER_REQUEST_SLEEP_S = 0.15
DEFAULT_ROUTE_RETRY_ATTEMPTS = 3
DEFAULT_ROUTE_RETRY_BACKOFF_BASE_S = 0.5
DEFAULT_MAX_PAGES_PER_RANGE = 5000


def env_first(*names: str, default: str) -> str:
    """Return the first configured environment value from an ordered allowlist."""

    for name in names:
        value = os.getenv(name)
        if value is not None:
            return value
    return default


def normalized_base_url(*names: str, default: str) -> str:
    """Return a normalized base URL from ordered environment override names."""

    return env_first(*names, default=default).strip().rstrip("/")


def non_negative_float(*names: str, default: float) -> float:
    """Return a non-negative float from ordered environment override names."""

    return env_float_non_negative(value=env_first(*names, default=str(default)), default=default)


def int_at_least(*names: str, default: int, minimum: int) -> int:
    """Return an integer bounded at ``minimum`` from ordered environment override names."""

    return env_int_min(value=env_first(*names, default=str(default)), default=default, minimum=minimum)


def page_size(*names: str, default: int, maximum: int) -> int:
    """Return a Deribit page size bounded to the exchange maximum."""

    return min(int_at_least(*names, default=default, minimum=1), maximum)


def max_pages_per_range(*names: str) -> int:
    """Return the page cap for one bounded fetch range.

    A value of ``0`` preserves the existing adapter convention of disabling the page cap.
    """

    return int_at_least(*names, default=DEFAULT_MAX_PAGES_PER_RANGE, minimum=0)
