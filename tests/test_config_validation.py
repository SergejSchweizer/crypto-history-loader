"""Tests for Pydantic runtime config validation."""

from __future__ import annotations

import pytest

from application.services.config_validation import validate_runtime_config


def test_validate_runtime_config_accepts_minimal_required_sections() -> None:
    payload: dict[str, object] = {
        "global": {"no_json_output": False},
        "env": {"DEPTH_HTTP_TIMEOUT_S": 8},
        "export-descriptive-stats": {
            "lake_root": "lake/bronze",
            "output_csv": "docs/out.csv",
            "start_time": "2026-01-01T00:00:00+00:00",
            "end_time": "2026-01-31T00:00:00+00:00",
            "exchanges": ["deribit"],
            "symbols": ["BTC"],
            "timeframes": ["1m"],
            "instrument_types": ["spot"],
        },
    }
    validate_runtime_config(payload)


def test_validate_runtime_config_rejects_invalid_export_types() -> None:
    payload: dict[str, object] = {
        "global": {},
        "env": {},
        "export-descriptive-stats": {
            "lake_root": "lake/bronze",
            "output_csv": "docs/out.csv",
            "start_time": "2026-01-01T00:00:00+00:00",
            "end_time": "2026-01-31T00:00:00+00:00",
            "exchanges": "deribit",
            "symbols": ["BTC"],
            "timeframes": ["1m"],
            "instrument_types": ["spot"],
        },
    }
    with pytest.raises(ValueError, match="Invalid config.yaml schema"):
        validate_runtime_config(payload)


def test_validate_runtime_config_accepts_fetch_runtime_env_policy() -> None:
    payload: dict[str, object] = {
        "global": {},
        "env": {
            "DEPTH_SYNC_LOG_DIR": ".logs",
            "DEPTH_FETCH_CONCURRENCY": 6,
            "DEPTH_FETCH_TASK_TIMEOUT_S": 900,
            "DEPTH_FETCH_HEARTBEAT_S": 30,
            "DEPTH_PERP_TRADES_WINDOW_MINUTES": 60,
            "DEPTH_OPTION_TRADES_WINDOW_MINUTES": 120,
            "DEPTH_DERIBIT_PERP_TRADES_PAGE_SIZE": 1000,
            "DEPTH_DERIBIT_OPTION_TRADES_PAGE_SIZE": 1000,
            "DEPTH_DERIBIT_PERP_TRADES_INTER_REQUEST_SLEEP_S": 0.02,
            "DEPTH_DERIBIT_OPTION_TRADES_INTER_REQUEST_SLEEP_S": 0.02,
        },
        "export-descriptive-stats": {
            "lake_root": "lake/bronze",
            "output_csv": "docs/out.csv",
            "start_time": "2026-01-01T00:00:00+00:00",
            "end_time": "2026-01-31T00:00:00+00:00",
            "exchanges": ["deribit"],
            "symbols": ["BTC"],
            "timeframes": ["1m"],
            "instrument_types": ["spot"],
        },
    }

    validate_runtime_config(payload)


def test_validate_runtime_config_rejects_out_of_bounds_fetch_runtime_env_policy() -> None:
    payload: dict[str, object] = {
        "global": {},
        "env": {
            "DEPTH_FETCH_CONCURRENCY": 0,
            "DEPTH_PERP_TRADES_WINDOW_MINUTES": 0,
            "DEPTH_OPTION_TRADES_WINDOW_MINUTES": 1441,
            "DEPTH_DERIBIT_PERP_TRADES_PAGE_SIZE": 1001,
            "DEPTH_DERIBIT_OPTION_TRADES_INTER_REQUEST_SLEEP_S": -0.1,
        },
        "export-descriptive-stats": {
            "lake_root": "lake/bronze",
            "output_csv": "docs/out.csv",
            "start_time": "2026-01-01T00:00:00+00:00",
            "end_time": "2026-01-31T00:00:00+00:00",
            "exchanges": ["deribit"],
            "symbols": ["BTC"],
            "timeframes": ["1m"],
            "instrument_types": ["spot"],
        },
    }

    with pytest.raises(ValueError, match="Invalid config.yaml schema"):
        validate_runtime_config(payload)
