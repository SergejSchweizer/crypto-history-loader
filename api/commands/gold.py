"""Gold build command for silver-to-gold symbol datasets."""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast

from application.dataset_contracts import supported_gold_dataset_ids
from application.services.gold_service import (
    GOLD_RETENTION_KEEP_VERSIONS,
    build_gold_for_symbol,
    build_gold_timeframe_fanout_for_symbol,
    discover_gold_symbols,
    discover_gold_symbols_for_dataset,
    normalize_symbol,
    validate_gold_retention_keep_versions,
)

_SEMVER_RE = re.compile(r"^v\d+\.\d+\.\d+$")
_HISTORY_FULL_BASE_DATASET_ID = "gold.history.full.m1"
_HISTORY_FULL_DERIVED_DATASET_IDS = {
    "gold.history.full.m5",
    "gold.history.full.m30",
    "gold.history.full.h1",
    "gold.history.extended.m5",
    "gold.history.extended.m30",
    "gold.history.extended.h1",
    "gold.live.extended.m5",
    "gold.live.extended.m30",
    "gold.live.extended.h1",
    "gold.live.full.m5",
    "gold.live.full.m30",
    "gold.live.full.h1",
}
_HISTORY_FULL_DERIVED_BASE_DATASET_IDS = {
    "gold.history.full.m5": "gold.history.full.m1",
    "gold.history.full.m30": "gold.history.full.m1",
    "gold.history.full.h1": "gold.history.full.m1",
    "gold.history.extended.m5": "gold.history.extended.m1",
    "gold.history.extended.m30": "gold.history.extended.m1",
    "gold.history.extended.h1": "gold.history.extended.m1",
    "gold.live.extended.m5": "gold.live.extended.m1",
    "gold.live.extended.m30": "gold.live.extended.m1",
    "gold.live.extended.h1": "gold.live.extended.m1",
    "gold.live.full.m5": "gold.live.full.m1",
    "gold.live.full.m30": "gold.live.full.m1",
    "gold.live.full.h1": "gold.live.full.m1",
}


def add_gold_build_parser(subparsers: Any) -> None:
    """Register ``gold-build`` parser."""

    parser = subparsers.add_parser("gold-build", help="Build gold per-symbol parquet datasets from silver data")
    parser.add_argument("--silver-root", default="lake/silver", help="Silver lake root")
    parser.add_argument("--gold-root", default="lake/gold", help="Gold lake root")
    parser.add_argument(
        "--l2-root",
        default="remote_l2_m1_features",
        help="Root path for upstream L2 minute features (used by hybrid L2 gold dataset)",
    )
    parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    parser.add_argument("--symbols", nargs="+", help="Optional symbol list; auto-discovered when omitted")
    parser.add_argument(
        "--dataset-id",
        choices=list(supported_gold_dataset_ids()),
        help="Gold dataset identifier (when omitted, build all supported datasets)",
    )
    parser.add_argument("--dataset-version", default="v1.0.0", help="Semantic dataset version")
    parser.add_argument(
        "--auto-version", action="store_true", help="Auto-increment semantic version from prior manifests"
    )
    parser.add_argument(
        "--version-base", default="v1.0.0", help="Base version used when auto-version has no prior manifest"
    )
    parser.add_argument("--manifest", action="store_true", help="Deprecated: gold manifests are always generated")
    parser.add_argument("--plot", action="store_true", help="Deprecated: gold plots are always generated")
    parser.add_argument(
        "--l2-validation-mode",
        choices=["strict", "lenient"],
        default="strict",
        help="L2 quality handling for hybrid datasets: strict fails build, lenient drops invalid joined rows",
    )
    parser.add_argument(
        "--retention-keep-versions",
        type=int,
        default=GOLD_RETENTION_KEEP_VERSIONS,
        help="Fixed Gold retention window; only the value 3 is accepted",
    )
    parser.add_argument("--maxprocesses", type=int, default=4, help="Maximum parallel gold build workers")
    parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")


def _resolve_gold_symbols(
    *,
    symbols: list[str] | None,
    silver_root: str,
    exchange: str,
    dataset_id: str | None = None,
) -> list[str]:
    """Return normalized symbol schedule for gold build."""

    if symbols:
        return sorted({normalize_symbol(symbol) for symbol in symbols})
    if dataset_id is not None:
        return discover_gold_symbols_for_dataset(silver_root=silver_root, exchange=exchange, dataset_id=dataset_id)
    return discover_gold_symbols(silver_root=silver_root, exchange=exchange)


def _resolve_dataset_ids(dataset_id: str | None) -> list[str]:
    """Return dataset-id schedule for gold build."""

    return [dataset_id] if dataset_id else list(supported_gold_dataset_ids())


def _order_dataset_ids_with_dependencies(
    dataset_ids: list[str],
    dependencies: dict[str, str],
) -> list[str]:
    """Order Gold datasets so every derived dataset follows its source."""

    ordered: list[str] = []
    pending = list(dict.fromkeys(dataset_ids))
    while pending:
        progressed = False
        for selected_dataset_id in pending.copy():
            source_dataset_id = dependencies.get(selected_dataset_id)
            if source_dataset_id is not None and source_dataset_id not in ordered:
                continue
            ordered.append(selected_dataset_id)
            pending.remove(selected_dataset_id)
            progressed = True
        if not progressed:
            raise ValueError(f"Cyclic or unresolved Gold dataset dependency: {pending}")
    return ordered


def _validate_version_args(*, auto_version: bool, dataset_version: str, version_base: str) -> None:
    """Validate version arguments against semantic version policy."""

    if not auto_version and not _SEMVER_RE.fullmatch(dataset_version):
        raise ValueError(f"Invalid --dataset-version '{dataset_version}'. Expected semantic version like v1.0.0")
    if auto_version and not _SEMVER_RE.fullmatch(version_base):
        raise ValueError(f"Invalid --version-base '{version_base}'. Expected semantic version like v1.0.0")


def run_gold_build(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run gold build for configured symbols."""

    silver_root = cast(str, args.silver_root)
    gold_root = cast(str, args.gold_root)
    l2_root = cast(str, args.l2_root)
    exchange = cast(str, args.exchange)
    dataset_id = cast(str | None, args.dataset_id)
    dataset_version = cast(str, args.dataset_version)
    auto_version = bool(getattr(args, "auto_version", False))
    version_base = cast(str, getattr(args, "version_base", "v1.0.0"))
    symbols = cast(list[str] | None, args.symbols)
    l2_validation_mode = cast(str, getattr(args, "l2_validation_mode", "strict"))
    keep_last_versions = validate_gold_retention_keep_versions(
        int(getattr(args, "retention_keep_versions", GOLD_RETENTION_KEEP_VERSIONS))
    )
    maxprocesses = int(getattr(args, "maxprocesses", 4))
    if maxprocesses < 1:
        raise ValueError(f"Invalid --maxprocesses '{maxprocesses}'. Value must be an integer >= 1")
    reports: list[dict[str, object]] = []

    dataset_ids = _resolve_dataset_ids(dataset_id)
    schedule: dict[str, list[str]] = {}
    for selected_dataset_id in dataset_ids:
        schedule[selected_dataset_id] = _resolve_gold_symbols(
            symbols=symbols,
            silver_root=silver_root,
            exchange=exchange,
            dataset_id=selected_dataset_id,
        )
    derived_dataset_ids = [
        selected_dataset_id
        for selected_dataset_id in dataset_ids
        if selected_dataset_id in _HISTORY_FULL_DERIVED_DATASET_IDS
    ]
    base_dataset_ids = {
        _HISTORY_FULL_DERIVED_BASE_DATASET_IDS[selected_dataset_id] for selected_dataset_id in derived_dataset_ids
    }
    for base_dataset_id in base_dataset_ids:
        if base_dataset_id not in schedule:
            schedule[base_dataset_id] = _resolve_gold_symbols(
                symbols=symbols,
                silver_root=silver_root,
                exchange=exchange,
                dataset_id=base_dataset_id,
            )
    effective_dataset_ids = list(dataset_ids)
    for base_dataset_id in sorted(base_dataset_ids):
        if base_dataset_id not in effective_dataset_ids:
            effective_dataset_ids.append(base_dataset_id)
    effective_dataset_ids = _order_dataset_ids_with_dependencies(
        effective_dataset_ids,
        _HISTORY_FULL_DERIVED_BASE_DATASET_IDS,
    )
    logger.info("Gold build schedule dataset_symbols=%s", schedule)
    _validate_version_args(auto_version=auto_version, dataset_version=dataset_version, version_base=version_base)

    def _run_one(selected_dataset_id: str, symbol: str) -> dict[str, object] | None:
        try:
            report = build_gold_for_symbol(
                silver_root=silver_root,
                gold_root=gold_root,
                l2_root=l2_root,
                exchange=exchange,
                symbol=symbol,
                dataset_id=selected_dataset_id,
                dataset_version=dataset_version,
                auto_version=auto_version,
                version_base=version_base,
                manifest=True,
                plot=True,
                l2_validation_mode=l2_validation_mode,
                keep_last_versions=keep_last_versions,
            )
        except ValueError as exc:
            logger.warning(
                "Gold dataset skipped symbol=%s dataset_id=%s reason=%s",
                symbol,
                selected_dataset_id,
                exc,
            )
            return None
        logger.info(
            "Gold dataset written symbol=%s dataset_id=%s rows_out=%s path=%s",
            symbol,
            selected_dataset_id,
            report.rows_out,
            report.parquet_path,
        )
        return report.to_dict()

    def _make_job(selected_dataset_id: str, symbol: str) -> Callable[[], dict[str, object] | None]:
        def _job() -> dict[str, object] | None:
            return _run_one(selected_dataset_id, symbol)

        return _job

    def _run_fanout(dataset_ids_for_source: list[str], symbol: str) -> list[dict[str, object]]:
        try:
            fanout_reports = build_gold_timeframe_fanout_for_symbol(
                gold_root=gold_root,
                exchange=exchange,
                symbol=symbol,
                dataset_ids=dataset_ids_for_source,
                dataset_version=dataset_version,
                auto_version=auto_version,
                version_base=version_base,
                keep_last_versions=keep_last_versions,
            )
        except ValueError as exc:
            logger.warning(
                "Gold timeframe fan-out skipped symbol=%s dataset_ids=%s reason=%s",
                symbol,
                dataset_ids_for_source,
                exc,
            )
            return []
        for report in fanout_reports:
            logger.info(
                "Gold dataset written symbol=%s dataset_id=%s rows_out=%s path=%s",
                symbol,
                report.dataset_id,
                report.rows_out,
                report.parquet_path,
            )
        return [report.to_dict() for report in fanout_reports]

    def _make_fanout_job(dataset_ids_for_source: list[str], symbol: str) -> Callable[[], list[dict[str, object]]]:
        def _job() -> list[dict[str, object]]:
            return _run_fanout(dataset_ids_for_source, symbol)

        return _job

    total_jobs = sum(len(schedule[selected_dataset_id]) for selected_dataset_id in effective_dataset_ids)
    logger.info("Gold build parallelization maxprocesses=%s jobs=%s", maxprocesses, total_jobs)
    processed_derived_dataset_ids: set[str] = set()
    for selected_dataset_id in effective_dataset_ids:
        if selected_dataset_id in processed_derived_dataset_ids:
            continue
        if selected_dataset_id in _HISTORY_FULL_DERIVED_DATASET_IDS:
            source_dataset_id = _HISTORY_FULL_DERIVED_BASE_DATASET_IDS[selected_dataset_id]
            sibling_dataset_ids = sorted(
                dataset
                for dataset in effective_dataset_ids
                if _HISTORY_FULL_DERIVED_BASE_DATASET_IDS.get(dataset) == source_dataset_id
            )
            sibling_symbols = sorted({symbol for dataset in sibling_dataset_ids for symbol in schedule[dataset]})
            processed_derived_dataset_ids.update(sibling_dataset_ids)
            fanout_jobs = [_make_fanout_job(sibling_dataset_ids, symbol) for symbol in sibling_symbols]
            with ThreadPoolExecutor(max_workers=maxprocesses) as executor:
                fanout_futures = [executor.submit(job) for job in fanout_jobs]
                for fanout_future in fanout_futures:
                    reports.extend(fanout_future.result())
            continue
        # Dataset dependencies must complete before their derived children start.
        # Symbols remain parallel within one dataset because they are independent.
        jobs = [_make_job(selected_dataset_id, symbol) for symbol in schedule[selected_dataset_id]]
        with ThreadPoolExecutor(max_workers=maxprocesses) as executor:
            futures = [executor.submit(job) for job in jobs]
            for build_future in futures:
                payload = build_future.result()
                if payload is not None:
                    reports.append(payload)
    if not bool(args.no_json_output):
        print(json.dumps({"reports": reports}, indent=2))
    logger.info("Command complete: gold-build reports=%s", len(reports))
