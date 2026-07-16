"""Typed Bronze build request and result contracts.

These types provide a stable, testable seam between CLI argument parsing
(`api.commands.loader_parser`) and the Bronze build workflow
(`api.commands.loader_workflow`). They are additive: existing CLI flags,
defaults, JSON output shape, checkpoint keys, and Bronze write locations are
unchanged by introducing these contracts. A later refactor PR in the Bronze
stack (see ``BACKLOG.md`` PR-45/PR-46) routes execution through them.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import cast


@dataclass(frozen=True)
class BronzeDatasetSelection:
    """Deterministic Bronze dataset/symbol selection resolved from CLI args.

    Attributes:
        exchange: Primary ``--exchange`` value (parser default is ``deribit``).
        exchanges: Optional explicit ``--exchanges`` list; empty when omitted.
        data_types: Requested ``--dataset`` values in parser-provided order.
        symbols: Requested ``--symbols`` values in parser-provided order.
    """

    exchange: str
    exchanges: tuple[str, ...]
    data_types: tuple[str, ...]
    symbols: tuple[str, ...]


@dataclass(frozen=True)
class BronzeRuntimeContext:
    """Typed runtime bounds intent resolved for one Bronze build invocation.

    This mirrors the ``--tail-delta-only``/``--full-gap-fill``, ``--start-date``,
    ``--symbol-start-dates``, and ``--exchange-symbol-start-dates`` CLI flags
    without resolving them into millisecond bounds; bound resolution stays in
    `application.services.bronze_runtime_service` so this contract remains a
    pure, hashable snapshot of caller intent.

    Attributes:
        tail_delta_only: Whether the build should fetch only new tail data
            after the latest stored point (``--tail-delta-only``); defaults to
            full historical gap-fill mode (``False``) matching the parser.
        start_date: Inclusive UTC ``YYYY-MM-DD`` boundary from ``--start-date``,
            or ``None`` when not provided.
        symbol_start_dates: Raw ``SYMBOL=YYYY-MM-DD`` entries from
            ``--symbol-start-dates``, in parser-provided order.
        exchange_symbol_start_dates: Raw ``EXCHANGE:SYMBOL=YYYY-MM-DD`` entries
            from ``--exchange-symbol-start-dates``, in parser-provided order.
    """

    tail_delta_only: bool
    start_date: str | None
    symbol_start_dates: tuple[str, ...]
    exchange_symbol_start_dates: tuple[str, ...]


@dataclass(frozen=True)
class BronzeBuildRequest:
    """Deterministic, typed Bronze build request derived from CLI args and config.

    This is the immutable contract boundary between CLI argument parsing and
    the Bronze build workflow. Converting the same ``argparse.Namespace``
    twice always produces an equal `BronzeBuildRequest`, which lets tests and
    future workflow stages reason about build intent without re-parsing
    ``sys.argv`` or depending on ``api`` internals.

    Attributes:
        dataset_selection: Resolved dataset/symbol selection for this build.
        runtime_context: Resolved runtime bounds intent for this build.
        lake_root: Root directory for parquet lake files (``--lake-root``).
        save_parquet_lake: Whether fetched rows are persisted to the lake
            (``--save-parquet-lake``); also gates checkpoint read/write.
        no_json_output: Whether JSON summary output is suppressed
            (``--no-json-output``).
        debug: Whether debug-level logging was explicitly requested via
            ``--debug``. Bronze builds always log at debug level regardless of
            this flag (see `api.cli._DEBUG_BY_DEFAULT_COMMANDS`); this field
            only reflects the explicit CLI switch.
        invoked_via_cli: Whether this request originated from the ``main()``
            CLI entrypoint (``args._invoked_via_cli``), as opposed to a direct
            programmatic call used by tests.
    """

    dataset_selection: BronzeDatasetSelection
    runtime_context: BronzeRuntimeContext
    lake_root: str
    save_parquet_lake: bool
    no_json_output: bool
    debug: bool
    invoked_via_cli: bool


@dataclass(frozen=True)
class BronzeBuildResult:
    """Typed summary of one completed Bronze build invocation.

    This does not replace the existing JSON/report output written by
    `api.commands.loader_output.finalize_bronze_output`; it is an additive,
    typed view over the same summary counters so future workflow stages can
    depend on a stable contract instead of a loosely typed ``dict``.

    Attributes:
        data_types: Requested Bronze dataset types for this build, in
            schedule order.
        symbols: Requested symbols for this build, in schedule order.
        success_counts_by_dataset: Successful fetch-task counts keyed by
            dataset type.
        error_counts_by_dataset: Failed fetch-task counts keyed by dataset
            type.
        sidecars_written: Sidecar file paths written during this build.
    """

    data_types: tuple[str, ...]
    symbols: tuple[str, ...]
    success_counts_by_dataset: dict[str, int]
    error_counts_by_dataset: dict[str, int]
    sidecars_written: tuple[str, ...]


def bronze_build_request_from_args(args: argparse.Namespace) -> BronzeBuildRequest:
    """Convert a parsed ``bronze-build`` argparse.Namespace into a typed request.

    Every field is read defensively with ``getattr`` using the same defaults
    declared by the parser (`api.commands.loader_parser.add_ingest_parser`),
    so this conversion is safe to call for hand-built namespaces in tests
    without invoking the real parser.

    Args:
        args: Parsed CLI arguments for ``bronze-build`` (or a compatible
            ingest command sharing the same flag set).

    Returns:
        A frozen `BronzeBuildRequest` snapshot of ``args``.
    """

    exchanges_value = getattr(args, "exchanges", None)
    dataset_value = getattr(args, "dataset", None)
    symbols_value = getattr(args, "symbols", None)
    symbol_start_dates_value = getattr(args, "symbol_start_dates", None)
    exchange_symbol_start_dates_value = getattr(args, "exchange_symbol_start_dates", None)

    dataset_selection = BronzeDatasetSelection(
        exchange=str(getattr(args, "exchange", "deribit")),
        exchanges=tuple(exchanges_value) if exchanges_value else (),
        data_types=tuple(dataset_value) if dataset_value else ("spot_ohlcv",),
        symbols=tuple(symbols_value) if symbols_value else ("BTC", "ETH", "SOL"),
    )
    runtime_context = BronzeRuntimeContext(
        tail_delta_only=bool(getattr(args, "tail_delta_only", False)),
        start_date=cast(str | None, getattr(args, "start_date", None)),
        symbol_start_dates=tuple(symbol_start_dates_value) if symbol_start_dates_value else (),
        exchange_symbol_start_dates=(
            tuple(exchange_symbol_start_dates_value) if exchange_symbol_start_dates_value else ()
        ),
    )
    return BronzeBuildRequest(
        dataset_selection=dataset_selection,
        runtime_context=runtime_context,
        lake_root=str(getattr(args, "lake_root", "lake/bronze")),
        save_parquet_lake=bool(getattr(args, "save_parquet_lake", False)),
        no_json_output=bool(getattr(args, "no_json_output", False)),
        debug=bool(getattr(args, "debug", False)),
        invoked_via_cli=bool(getattr(args, "_invoked_via_cli", False)),
    )


def bronze_build_result_from_counters(
    *,
    data_types: list[str],
    symbols: list[str],
    success_counts_by_dataset: dict[str, int],
    error_counts_by_dataset: dict[str, int],
    sidecars_written: list[str],
) -> BronzeBuildResult:
    """Build a typed `BronzeBuildResult` from already-computed summary counters.

    Args:
        data_types: Requested Bronze dataset types for this build, in
            schedule order.
        symbols: Requested symbols for this build, in schedule order.
        success_counts_by_dataset: Successful fetch-task counts keyed by
            dataset type.
        error_counts_by_dataset: Failed fetch-task counts keyed by dataset
            type.
        sidecars_written: Sidecar file paths written during this build.

    Returns:
        A frozen `BronzeBuildResult` snapshot of the provided counters.
    """

    return BronzeBuildResult(
        data_types=tuple(data_types),
        symbols=tuple(symbols),
        success_counts_by_dataset=dict(success_counts_by_dataset),
        error_counts_by_dataset=dict(error_counts_by_dataset),
        sidecars_written=tuple(sidecars_written),
    )
