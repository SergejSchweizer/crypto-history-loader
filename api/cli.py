"""Command-line interface for data ingestion tasks."""

from __future__ import annotations

import argparse
import os
import stat
import sys
from pathlib import Path
from typing import Any, cast

from api.commands import loader as loader_cmd
from api.commands import stats as stats_cmd
from api.commands.gold import add_gold_build_parser, run_gold_build
from api.commands.inventory import add_dataset_inventory_parser, run_dataset_inventory
from api.commands.isin_modules import (
    add_isin_module_parsers,
    run_bivariate_statistics_command,
    run_fetch_all_isins,
    run_metadata_filter_command,
    run_univariate_filter_command,
    run_univariate_statistics_command,
)
from api.commands.loader import add_bronze_build_parser
from api.commands.silver import add_silver_build_parser, run_silver_build
from api.commands.stats import add_export_descriptive_stats_parser, run_export_descriptive_stats
from api.commands.timeframes import add_list_spot_ohlcv_timeframes_parser, run_list_spot_ohlcv_timeframes
from application.services.bronze_runtime_service import BronzeRuntimeBoundsContext
from application.services.config_validation import validate_runtime_config
from application.services.fetch_service import fetch_symbol_candles
from application.services.gapfill_service import _last_closed_open_ms, _missing_ranges_ms
from application.services.lake_query_service import (
    latest_open_time_in_lake,
    latest_open_time_in_lake_by_dataset,
    load_combined_ohlcv_dataframe,  # noqa: F401 - backward-compatible test monkeypatch surface
    open_times_in_lake,
    open_times_in_lake_by_dataset,
)
from application.services.runtime_service import (
    SingleInstanceError,
    SingleInstanceLock,
    configure_logging,
    fetch_concurrency,
)
from ingestion.funding import (
    fetch_funding_all_history,
    fetch_funding_range,
    funding_interval_to_milliseconds,
    normalize_funding_timeframe,
)
from ingestion.open_interest import (
    fetch_open_interest_all_history,
    fetch_open_interest_range,
    normalize_open_interest_timeframe,
    open_interest_interval_to_milliseconds,
)
from ingestion.spot_ohlcv import (
    Exchange,
    Market,
    SpotCandle,
    fetch_candles_all_history,
    fetch_candles_range,
    interval_to_milliseconds,
    normalize_storage_symbol,
)
from ingestion.trades import fetch_trades_all_history, fetch_trades_range

__all__ = ["SingleInstanceError", "SingleInstanceLock", "build_parser", "main"]
_TAIL_DELTA_ONLY = False
_BRONZE_START_OPEN_MS: int | None = None
_BRONZE_SYMBOL_START_OPEN_MS: dict[str, int] = {}
_BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS: dict[str, int] = {}
_BRONZE_CONFIG_ALIASES: tuple[str, ...] = ("bronze-build", "bronze-ingest", "loader")
_DEBUG_BY_DEFAULT_COMMANDS: frozenset[str] = frozenset({"bronze-build", "silver-build", "gold-build"})


# Backward-compatible wrappers used by tests.
def _fetch_symbol_candles(  # pyright: ignore[reportUnusedFunction]
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
) -> list[SpotCandle]:
    return fetch_symbol_candles(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_in_lake,
        symbol_normalizer=normalize_storage_symbol,
        interval_ms_resolver=interval_to_milliseconds,
        now_open_resolver=_last_closed_open_ms,
        ranges_builder=_missing_ranges_ms,
        history_fetcher=fetch_candles_all_history,
        range_fetcher=fetch_candles_range,
        latest_open_time_reader=latest_open_time_in_lake,
        tail_delta_only=_TAIL_DELTA_ONLY,
        start_open_ms_bound=_BRONZE_START_OPEN_MS,
    )


def _sync_loader_runtime_overrides() -> None:
    """Mirror runtime symbols into loader module to preserve monkeypatch behavior."""

    loader_any = cast(Any, loader_cmd)
    loader_any.SingleInstanceLock = SingleInstanceLock
    loader_any.SingleInstanceError = SingleInstanceError
    loader_any.fetch_concurrency = fetch_concurrency
    loader_any._last_closed_open_ms = _last_closed_open_ms
    loader_any._missing_ranges_ms = _missing_ranges_ms
    loader_any.open_times_in_lake = open_times_in_lake
    loader_any.open_times_in_lake_by_dataset = open_times_in_lake_by_dataset
    loader_any.latest_open_time_in_lake = latest_open_time_in_lake
    loader_any.latest_open_time_in_lake_by_dataset = latest_open_time_in_lake_by_dataset
    loader_any._RUNTIME_BOUNDS_CONTEXT = BronzeRuntimeBoundsContext(
        tail_delta_only=_TAIL_DELTA_ONLY,
        global_start_open_ms=_BRONZE_START_OPEN_MS,
        symbol_start_open_ms=_BRONZE_SYMBOL_START_OPEN_MS,
        exchange_symbol_start_open_ms=_BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS,
    )
    loader_any.normalize_storage_symbol = normalize_storage_symbol
    loader_any.interval_to_milliseconds = interval_to_milliseconds
    loader_any.open_interest_interval_to_milliseconds = open_interest_interval_to_milliseconds
    loader_any.funding_interval_to_milliseconds = funding_interval_to_milliseconds
    loader_any.normalize_open_interest_timeframe = normalize_open_interest_timeframe
    loader_any.normalize_funding_timeframe = normalize_funding_timeframe
    loader_any.fetch_candles_all_history = fetch_candles_all_history
    loader_any.fetch_candles_range = fetch_candles_range
    loader_any.fetch_open_interest_all_history = fetch_open_interest_all_history
    loader_any.fetch_open_interest_range = fetch_open_interest_range
    loader_any.fetch_funding_all_history = fetch_funding_all_history
    loader_any.fetch_funding_range = fetch_funding_range
    loader_any.fetch_trades_all_history = fetch_trades_all_history
    loader_any.fetch_trades_range = fetch_trades_range


def build_parser() -> argparse.ArgumentParser:
    """Create top-level CLI parser."""

    parser = argparse.ArgumentParser(description="crypto-history-loader CLI")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML configuration file for command defaults",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug-level logging")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_bronze_build_parser(subparsers)
    add_silver_build_parser(subparsers)
    add_gold_build_parser(subparsers)
    add_dataset_inventory_parser(subparsers)
    add_isin_module_parsers(subparsers)
    add_list_spot_ohlcv_timeframes_parser(subparsers)
    add_export_descriptive_stats_parser(subparsers)

    return parser


def _load_yaml_config(path: str) -> dict[str, object]:
    """Load and validate mandatory YAML config file."""

    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Required config file '{config_path}' is missing. Create config.yaml before running commands."
        )
    if not config_path.is_file():
        raise ValueError(f"Config path '{config_path}' must be a regular file")

    file_mode = stat.S_IMODE(config_path.stat().st_mode)
    # Block world-writable/executable config files; allow world-readable files
    # so default repository checkouts remain usable across environments.
    if file_mode & 0o003:
        raise PermissionError(
            f"Insecure permissions on '{config_path}' ({oct(file_mode)}). "
            "Remove write/execute permissions for 'others' (recommended: chmod 644 or 640 config.yaml)."
        )
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError("PyYAML is required to parse config.yaml. Install project dependencies.") from exc
    with config_path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    if loaded is None:
        raise ValueError("config.yaml is empty")
    if not isinstance(loaded, dict):
        raise ValueError("config file must contain a top-level mapping")
    config = cast(dict[str, object], loaded)

    required_sections = {"env", "export-descriptive-stats"}
    missing = [section for section in required_sections if section not in config]
    if missing:
        raise ValueError(f"config.yaml missing required section(s): {', '.join(sorted(missing))}")
    if not isinstance(config.get("env"), dict):
        raise ValueError("config.yaml section 'env' must be a mapping")
    validate_runtime_config(config)

    return config


def _subparser_for_command(parser: argparse.ArgumentParser, command: str) -> argparse.ArgumentParser | None:
    """Return subparser object for selected command."""

    for action in parser._actions:
        choices = cast(dict[str, argparse.ArgumentParser] | None, getattr(action, "choices", None))
        if not isinstance(choices, dict):
            continue
        candidate = choices.get(command)
        if isinstance(candidate, argparse.ArgumentParser):
            return candidate
    return None


def _collect_explicit_cli_dests(command_parser: argparse.ArgumentParser, argv: list[str]) -> set[str]:
    """Collect argparse destination names explicitly provided via CLI flags."""

    provided: set[str] = set()
    option_to_dest: dict[str, str] = {}
    for action in command_parser._actions:
        for option in action.option_strings:
            option_to_dest[option] = action.dest
    for token in argv:
        if token == "--":
            break
        if token.startswith("--"):
            option_name = token.split("=", 1)[0]
            dest = option_to_dest.get(option_name)
            if dest:
                provided.add(dest)
    return provided


def _apply_yaml_defaults(
    args: argparse.Namespace,
    command: str,
    config: dict[str, object],
    explicit_dests: set[str],
) -> None:
    """Apply global and command-level YAML defaults unless overridden by CLI."""

    def _apply_mapping_defaults(section: object) -> None:
        if not isinstance(section, dict):
            return
        for raw_key, value in cast(dict[str, object], section).items():
            if raw_key == "debug":
                # Keep debug as an explicit CLI-only switch.
                continue
            if raw_key == "save_parquet_lake":
                # Keep Bronze parquet writes as an explicit CLI-only switch.
                continue
            if raw_key in explicit_dests or not hasattr(args, raw_key):
                continue
            setattr(args, raw_key, value)

    _apply_mapping_defaults(config.get("global"))
    _apply_mapping_defaults(_resolve_command_config(command=command, config=config))


def _resolve_command_config(command: str, config: dict[str, object]) -> object:
    """Resolve command config section, including compatibility aliases."""

    if command != "bronze-build":
        section = config.get(command)
        return cast(dict[str, object], section) if isinstance(section, dict) else None
    for candidate in _BRONZE_CONFIG_ALIASES:
        section = config.get(candidate)
        if isinstance(section, dict):
            return cast(dict[str, object], section)
    return None


def _apply_env_from_config(config: dict[str, object]) -> None:
    """Load ``env`` mapping from YAML config into process environment."""

    env_config = config.get("env")
    if not isinstance(env_config, dict):
        return
    for raw_key, value in cast(dict[str, object], env_config).items():
        if value is None:
            continue
        os.environ[raw_key] = str(value)


def _is_debug_logging_enabled(args: argparse.Namespace) -> bool:
    """Return whether command logging should use debug verbosity."""

    command = str(getattr(args, "command", ""))
    return bool(getattr(args, "debug", False)) or command in _DEBUG_BY_DEFAULT_COMMANDS


def main() -> None:
    """CLI entrypoint."""

    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", default="config.yaml")
    pre_parser.add_argument("command", nargs="?")
    pre_args, _ = pre_parser.parse_known_args(sys.argv[1:])
    config_data = _load_yaml_config(pre_args.config)
    _apply_env_from_config(config_data)

    parser = build_parser()
    args = parser.parse_args()
    command = cast(str, args.command)
    if command == "bronze-build" and not any(alias in config_data for alias in _BRONZE_CONFIG_ALIASES):
        raise ValueError("config.yaml missing required section: bronze-build")
    command_parser = _subparser_for_command(parser, command)
    if command_parser is not None:
        explicit = _collect_explicit_cli_dests(command_parser, sys.argv[1:])
        _apply_yaml_defaults(args=args, command=command, config=config_data, explicit_dests=explicit)
    logger = configure_logging(module_name=str(args.command), debug=_is_debug_logging_enabled(args))
    logger.info("Command start: %s", args.command)

    if args.command == "bronze-build":
        args._invoked_via_cli = True
        _sync_loader_runtime_overrides()
        loader_cmd.run_bronze_build(args=args, logger=logger)
    elif args.command == "fetch-all-isins":
        exit_code = run_fetch_all_isins(args=args, logger=logger)
        if exit_code:
            raise SystemExit(exit_code)
    elif args.command == "metadata-filter":
        run_metadata_filter_command(args=args, logger=logger)
    elif args.command == "univariate-statistics":
        run_univariate_statistics_command(args=args, logger=logger)
    elif args.command == "univariate-filter":
        run_univariate_filter_command(args=args, logger=logger)
    elif args.command == "bivariate-statistics":
        run_bivariate_statistics_command(args=args, logger=logger)
    elif args.command == "silver-build":
        run_silver_build(args=args, logger=logger)
    elif args.command == "gold-build":
        run_gold_build(args=args, logger=logger)
    elif args.command == "dataset-inventory":
        run_dataset_inventory(args=args, logger=logger)
    elif args.command == "list-spot_ohlcv-timeframes":
        run_list_spot_ohlcv_timeframes(args=args, logger=logger)
    elif args.command == "export-descriptive-stats":
        cast(Any, stats_cmd).load_combined_ohlcv_dataframe = load_combined_ohlcv_dataframe
        run_export_descriptive_stats(args=args, logger=logger)


if __name__ == "__main__":
    main()
