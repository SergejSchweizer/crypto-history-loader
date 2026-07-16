"""CLI commands for the five-module ISIN research architecture."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from application.services.isin_selection_service import (
    run_bivariate_statistics,
    run_metadata_filter,
    run_univariate_filter,
    run_univariate_statistics,
)
from scripts import fetch_eodhd_isins


def add_isin_module_parsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the five ISIN architecture module commands."""

    fetch_parser = subparsers.add_parser("fetch-all-isins", help="Refresh the canonical all_isins source from EODHD")
    fetch_parser.add_argument("--output-csv", help="Output CSV path")
    fetch_parser.add_argument("--manifest-json", help="Output manifest JSON path")
    fetch_parser.add_argument("--exchange-codes", nargs="*", default=None, help="Exchange codes to fetch")
    fetch_parser.add_argument("--timeout-s", type=float, default=None, help="HTTP timeout in seconds")
    fetch_parser.add_argument("--sleep-s", type=float, default=None, help="Sleep between EODHD requests")
    fetch_parser.add_argument("--lock-file", help="Non-blocking lock file path")
    fetch_parser.add_argument("--no-json-output", action="store_true", help="Suppress stdout summary JSON")
    delisted = fetch_parser.add_mutually_exclusive_group()
    delisted.add_argument("--include-delisted", action="store_true", default=None, help="Fetch delisted tickers too")
    delisted.add_argument("--no-include-delisted", action="store_false", dest="include_delisted")

    metadata_parser = subparsers.add_parser("metadata-filter", help="Filter all_isins by metadata predicates")
    metadata_parser.add_argument("--all-isins-csv", default="lake/reference/all_isins/all_isins.csv")
    metadata_parser.add_argument("--output-root", default="lake/selections/metadata_filter")
    metadata_parser.add_argument("--where", nargs="+", default=[], help="Conjunctive predicates like exchange=US")
    metadata_parser.add_argument("--selection-name", default=None)
    metadata_parser.add_argument("--no-json-output", action="store_true")

    univariate_stats_parser = subparsers.add_parser(
        "univariate-statistics", help="Compute per-ISIN univariate daily-return statistics"
    )
    univariate_stats_parser.add_argument("--prices-csv", required=True)
    univariate_stats_parser.add_argument("--output-csv", default="lake/statistics/univariate_statistics.csv")
    univariate_stats_parser.add_argument("--manifest-json", default="lake/statistics/univariate_statistics.json")
    univariate_stats_parser.add_argument("--price-column", default="adjusted_close")
    univariate_stats_parser.add_argument("--no-json-output", action="store_true")

    univariate_filter_parser = subparsers.add_parser(
        "univariate-filter", help="Filter ISINs by univariate statistics predicates"
    )
    univariate_filter_parser.add_argument("--statistics-csv", default="lake/statistics/univariate_statistics.csv")
    univariate_filter_parser.add_argument("--output-root", default="lake/selections/univariate_filter")
    univariate_filter_parser.add_argument("--where", nargs="+", default=[], help="Conjunctive predicates like sharpe>0")
    univariate_filter_parser.add_argument("--selection-name", default=None)
    univariate_filter_parser.add_argument("--no-json-output", action="store_true")

    bivariate_parser = subparsers.add_parser(
        "bivariate-statistics", help="Compute bivariate statistics for a selected ISIN list"
    )
    bivariate_parser.add_argument("--selection-csv", required=True)
    bivariate_parser.add_argument("--prices-csv", required=True)
    bivariate_parser.add_argument("--output-csv", default="lake/statistics/bivariate_statistics.csv")
    bivariate_parser.add_argument("--manifest-json", default="lake/statistics/bivariate_statistics.json")
    bivariate_parser.add_argument("--price-column", default="adjusted_close")
    bivariate_parser.add_argument("--min-overlap", type=int, default=2)
    bivariate_parser.add_argument("--no-json-output", action="store_true")


def run_fetch_all_isins(*, args: argparse.Namespace, logger: logging.Logger) -> int:
    """Run the canonical EODHD all-ISIN fetch module."""

    cli_args = _fetch_cli_args(args)
    logger.info("Running fetch_all_isins module")
    return fetch_eodhd_isins.main(cli_args)


def run_metadata_filter_command(*, args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run metadata_filter from CLI arguments."""

    result = run_metadata_filter(
        all_isins_csv=Path(args.all_isins_csv),
        output_root=Path(args.output_root),
        predicates=args.where,
        selection_name=args.selection_name,
    )
    logger.info("metadata_filter complete selection_id=%s rows=%s", result.selection_id, result.rows)
    _print_json(
        args,
        {
            "selection_id": result.selection_id,
            "selection_hash": result.selection_hash,
            "rows": result.rows,
            "isins_csv": str(result.isins_path),
            "manifest_json": str(result.manifest_path),
        },
    )


def run_univariate_statistics_command(*, args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run univariate_statistics from CLI arguments."""

    result = run_univariate_statistics(
        prices_csv=Path(args.prices_csv),
        output_csv=Path(args.output_csv),
        manifest_json=Path(args.manifest_json),
        price_column=args.price_column,
    )
    logger.info("univariate_statistics complete rows=%s", result.rows)
    _print_json(args, {"dataset": result.dataset, "rows": result.rows, "output_csv": str(result.output_csv)})


def run_univariate_filter_command(*, args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run univariate_filter from CLI arguments."""

    result = run_univariate_filter(
        statistics_csv=Path(args.statistics_csv),
        output_root=Path(args.output_root),
        predicates=args.where,
        selection_name=args.selection_name,
    )
    logger.info("univariate_filter complete selection_id=%s rows=%s", result.selection_id, result.rows)
    _print_json(
        args,
        {
            "selection_id": result.selection_id,
            "selection_hash": result.selection_hash,
            "rows": result.rows,
            "isins_csv": str(result.isins_path),
            "manifest_json": str(result.manifest_path),
        },
    )


def run_bivariate_statistics_command(*, args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run bivariate_statistics from CLI arguments."""

    result = run_bivariate_statistics(
        selection_csv=Path(args.selection_csv),
        prices_csv=Path(args.prices_csv),
        output_csv=Path(args.output_csv),
        manifest_json=Path(args.manifest_json),
        price_column=args.price_column,
        min_overlap=args.min_overlap,
    )
    logger.info("bivariate_statistics complete rows=%s", result.rows)
    _print_json(args, {"dataset": result.dataset, "rows": result.rows, "output_csv": str(result.output_csv)})


def _fetch_cli_args(args: argparse.Namespace) -> list[str]:
    cli_args = ["--config", args.config]
    for option_name, value in (
        ("--output-csv", args.output_csv),
        ("--manifest-json", args.manifest_json),
        ("--timeout-s", args.timeout_s),
        ("--sleep-s", args.sleep_s),
        ("--lock-file", args.lock_file),
    ):
        if value is not None:
            cli_args.extend([option_name, str(value)])
    if args.exchange_codes is not None:
        cli_args.append("--exchange-codes")
        cli_args.extend(str(value) for value in args.exchange_codes)
    if args.include_delisted is True:
        cli_args.append("--include-delisted")
    elif args.include_delisted is False:
        cli_args.append("--no-include-delisted")
    if args.no_json_output:
        cli_args.append("--no-json-output")
    return cli_args


def _print_json(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    if not bool(getattr(args, "no_json_output", False)):
        print(json.dumps(payload, indent=2, sort_keys=True))
