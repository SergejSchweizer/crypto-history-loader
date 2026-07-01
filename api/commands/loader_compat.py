"""Private compatibility helpers for the Bronze loader command."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from api.commands.loader_planning import (
    build_bronze_fetch_plan as build_bronze_fetch_plan,
)
from api.commands.loader_planning import (
    canonical_symbol_key as canonical_symbol_key,
)
from api.commands.loader_planning import (
    parse_exchange_symbol_start_dates as parse_exchange_symbol_start_dates,
)
from api.commands.loader_planning import (
    parse_start_date_to_open_ms as parse_start_date_to_open_ms,
)
from api.commands.loader_planning import (
    parse_symbol_start_dates as parse_symbol_start_dates,
)
from api.commands.loader_planning import (
    resolved_symbol_groups as resolved_symbol_groups,
)
from api.commands.loader_planning import (
    sanitize_symbols as sanitize_symbols,
)
from application.dto import BronzeExecutionPolicyDTO, BronzeFetchPlanDTO
from application.services.bronze_runtime_service import (
    IntervalTaskKey,
)
from application.services.bronze_runtime_service import (
    bronze_checkpoint_fingerprint as _bronze_checkpoint_fingerprint,
)
from application.services.bronze_runtime_service import (
    bronze_checkpoint_path as _bronze_checkpoint_path,
)
from application.services.bronze_runtime_service import (
    build_bronze_execution_policy as _build_bronze_execution_policy,
)
from application.services.bronze_runtime_service import (
    dataset_task_key_maps as dataset_task_key_maps,
)
from application.services.bronze_runtime_service import (
    hydrate_checkpoint_aliases as hydrate_checkpoint_aliases,
)
from application.services.bronze_runtime_service import (
    load_bronze_checkpoint as _load_bronze_checkpoint,
)
from application.services.bronze_runtime_service import (
    task_key_tuple_to_string as _task_key_tuple_to_string,
)
from application.services.bronze_runtime_service import (
    volatility_task_key_map as _volatility_task_key_map,
)
from application.services.bronze_runtime_service import (
    write_bronze_checkpoint as _write_bronze_checkpoint,
)
from application.services.fetch_runtime_policy import fetch_concurrency


def build_bronze_execution_policy() -> BronzeExecutionPolicyDTO:
    """Build standardized Bronze execution policy from configured concurrency."""

    return _build_bronze_execution_policy(configured_concurrency=fetch_concurrency())


def task_key_tuple_to_string(parts: tuple[object, ...]) -> str:
    """Serialize tuple task key to stable checkpoint string."""

    return _task_key_tuple_to_string(parts)


def volatility_task_key_map(plan: BronzeFetchPlanDTO) -> dict[IntervalTaskKey, str]:
    """Return tuple-to-checkpoint-key mapping for volatility dataset tasks."""

    return _volatility_task_key_map(plan)


def bronze_checkpoint_fingerprint(args: argparse.Namespace, plan: BronzeFetchPlanDTO) -> str:
    """Build stable fingerprint for one Bronze invocation plan."""

    return _bronze_checkpoint_fingerprint(args=args, plan=plan)


def bronze_checkpoint_path() -> Path:
    """Return Bronze restart-checkpoint path."""

    return _bronze_checkpoint_path()


def load_bronze_checkpoint(path: Path, fingerprint: str, logger: logging.Logger) -> dict[str, set[str]]:
    """Load matching Bronze checkpoint completed-task sets."""

    return _load_bronze_checkpoint(path=path, fingerprint=fingerprint, logger=logger)


def write_bronze_checkpoint(path: Path, *, fingerprint: str, completed: dict[str, set[str]]) -> None:
    """Persist Bronze checkpoint atomically."""

    _write_bronze_checkpoint(path, fingerprint=fingerprint, completed=completed)
