"""Gold transformation service for per-symbol model-ready datasets."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from application.dataset_contracts import (
    FULL_MARKET_GOLD_REQUIREMENTS,
    GOLD_DATASET_CONTRACTS,
    gold_dataset_contract,
)
from application.services import (
    feature_metadata_service,
    feature_plot_service,
    gold_audit,
    gold_frames,
    gold_versioning,
)

_feature_hash = feature_metadata_service.feature_hash
_feature_metadata = feature_metadata_service.feature_metadata
_write_feature_distribution_plot = feature_plot_service.write_feature_distribution_plot

_FULL_MARKET_REQUIREMENTS: list[tuple[str, str]] = [
    requirement.as_tuple() for requirement in FULL_MARKET_GOLD_REQUIREMENTS
]
GOLD_DATASET_SPECS: dict[str, dict[str, object]] = {
    dataset_id: contract.legacy_spec() for dataset_id, contract in GOLD_DATASET_CONTRACTS.items()
}
SUPPORTED_GOLD_DATASET_IDS = set(GOLD_DATASET_SPECS.keys())
_parse_semver = gold_versioning.parse_semver
_format_semver = gold_versioning.format_semver
_bump_semver = gold_versioning.bump_semver
_latest_manifest_for_dataset = gold_versioning.latest_manifest_for_dataset
_extract_feature_set_version = gold_versioning.extract_feature_set_version
_prune_gold_versions = gold_versioning.prune_gold_versions
_prune_gold_artifacts = gold_versioning.prune_gold_artifacts
_contract_bump_level = gold_versioning.contract_bump_level


def _require_polars() -> Any:
    return gold_frames.require_polars()


@dataclass(frozen=True)
class GoldBuildReport:
    """Aggregated gold build report for one symbol."""

    exchange: str
    symbol: str
    rows_out: int
    columns: list[str]
    min_timestamp: str | None
    max_timestamp: str | None
    parquet_path: str
    manifest_path: str | None
    plot_path: str | None
    hash_string: str
    dataset_id: str
    dataset_version: str
    feature_set_hash: str
    source_data_hash: str
    git_commit_hash: str
    version_bump_level: str
    version_bump_reason: str
    previous_version: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "rows_out": self.rows_out,
            "columns": self.columns,
            "min_timestamp": self.min_timestamp,
            "max_timestamp": self.max_timestamp,
            "parquet_path": self.parquet_path,
            "manifest_path": self.manifest_path,
            "plot_path": self.plot_path,
            "hash_string": self.hash_string,
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "feature_set_hash": self.feature_set_hash,
            "source_data_hash": self.source_data_hash,
            "git_commit_hash": self.git_commit_hash,
            "version_bump_level": self.version_bump_level,
            "version_bump_reason": self.version_bump_reason,
            "previous_version": self.previous_version,
        }


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today_utc_date() -> str:
    """Return current UTC calendar date as YYYY-MM-DD."""

    return datetime.now(UTC).strftime("%Y-%m-%d")


def _git_commit_hash() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        return out or "nogit"
    except Exception:
        return "nogit"


def normalize_symbol(value: str) -> str:
    """Normalize to canonical base asset symbol used across the repo (e.g. BTC, ETH, SOL)."""

    return gold_frames.normalize_symbol(value)


def discover_gold_symbols(silver_root: str, exchange: str) -> list[str]:
    """Discover symbols that have at least one required silver dataset."""

    required = _FULL_MARKET_REQUIREMENTS
    by_dataset: list[set[str]] = []
    for dataset_type, timeframe in required:
        by_dataset.append(
            _discover_symbols_for_dataset(
                silver_root=silver_root,
                exchange=exchange,
                dataset_type=dataset_type,
                timeframe=timeframe,
            )
        )
    if not by_dataset:
        return []
    return sorted({normalize_symbol(item) for item in set.intersection(*by_dataset)})


def discover_gold_symbols_for_dataset(silver_root: str, exchange: str, dataset_id: str) -> list[str]:
    """Discover symbols for one specific gold dataset requirement set."""

    required = _dataset_requirements(dataset_id)
    by_dataset: list[set[str]] = []
    for dataset_type, timeframe in required:
        by_dataset.append(
            _discover_symbols_for_dataset(
                silver_root=silver_root,
                exchange=exchange,
                dataset_type=dataset_type,
                timeframe=timeframe,
            )
        )
    if not by_dataset:
        return []
    return sorted({normalize_symbol(item) for item in set.intersection(*by_dataset)})


def _discover_symbols_for_dataset(
    *,
    silver_root: str,
    exchange: str,
    dataset_type: str,
    timeframe: str,
) -> set[str]:
    """Discover normalized symbols available for one silver dataset/timeframe."""

    return gold_frames.discover_symbols_for_dataset(
        silver_root=silver_root,
        exchange=exchange,
        dataset_type=dataset_type,
        timeframe=timeframe,
    )


def _dataset_requirements(dataset_id: str) -> list[tuple[str, str]]:
    return [requirement.as_tuple() for requirement in gold_dataset_contract(dataset_id).requirements]


def _dataset_includes_l2(dataset_id: str) -> bool:
    return gold_dataset_contract(dataset_id).include_l2


def _read_latest_l2_gold_frame(*, l2_root: str, exchange: str, symbol: str) -> tuple[Any, Path]:
    return gold_frames.read_latest_l2_gold_frame(l2_root=l2_root, exchange=exchange, symbol=symbol)


def _prepare_l2(pl: Any, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_l2(pl, frame, symbol)


def _l2_invalid_mask_expr(pl: Any, columns: set[str]) -> Any:
    return gold_frames.l2_invalid_mask_expr(pl, columns)


def _validate_or_filter_l2_quality(pl: Any, frame: Any, mode: str) -> tuple[Any, dict[str, int]]:
    return gold_frames.validate_or_filter_l2_quality(pl, frame, mode)


def _read_dataset_frame(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    dataset_type: str,
    timeframe: str,
) -> Any:
    return gold_frames.read_dataset_frame(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        dataset_type=dataset_type,
        timeframe=timeframe,
    )


def _prepare_spot_ohlcv_or_perp(pl: Any, frame: Any, prefix: str, symbol: str) -> Any:
    return gold_frames.prepare_spot_ohlcv_or_perp(pl, frame, prefix, symbol)


def _prepare_oi(pl: Any, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_oi(pl, frame, symbol)


def _prepare_funding(pl: Any, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_funding(pl, frame, symbol)


def _prepare_trades(pl: Any, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_trades(pl, frame, symbol)


def _prepare_option_trades(pl: Any, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_option_trades(pl, frame, symbol)


def _prepare_volatility_index_data(pl: Any, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_volatility_index_data(pl, frame, symbol)


def _prepare_dataset_frame(pl: Any, dataset_type: str, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_dataset_frame(pl, dataset_type, frame, symbol)


def _build_minute_grid(pl: Any, prepared: list[Any], exchange: str, symbol: str) -> Any:
    return gold_frames.build_minute_grid(pl, prepared, exchange, symbol)


def _json_payload_hash(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]


def _feature_source_dataset(column_name: str) -> str:
    return feature_metadata_service.feature_source_dataset(column_name)


def _time_span_coverage(frame: Any) -> tuple[datetime | None, datetime | None, int | None, int | None, float | None]:
    pl = _require_polars()
    return gold_audit.time_span_coverage(pl, frame)


def _source_dataset_summary(
    pl: Any, raw_by_dataset: dict[str, Any], l2_source_path: Path | None
) -> dict[str, dict[str, object]]:
    return gold_audit.source_dataset_summary(pl, raw_by_dataset, l2_source_path)


def _missing_value_audit(pl: Any, frame: Any) -> tuple[dict[str, int], int]:
    return gold_audit.missing_value_audit(pl, frame)


def build_gold_for_symbol(
    *,
    silver_root: str,
    gold_root: str,
    l2_root: str | None = None,
    exchange: str,
    symbol: str,
    dataset_id: str = "gold.market.full.m1",
    dataset_version: str = "v1.0.0",
    auto_version: bool = False,
    version_base: str = "v1.0.0",
    manifest: bool = False,
    plot: bool = False,
    l2_validation_mode: str = "strict",
    keep_last_versions: int = 3,
) -> GoldBuildReport:
    """Build one gold parquet dataset + manifest for a symbol.

    When ``l2_root`` is omitted, L2 lookup falls back to ``gold_root`` for backward compatibility.
    """

    pl = _require_polars()
    symbol = normalize_symbol(symbol)
    required = _dataset_requirements(dataset_id)
    raw_by_dataset: dict[str, Any] = {}
    for dataset_type, timeframe in required:
        raw_by_dataset[dataset_type] = _read_dataset_frame(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            dataset_type=dataset_type,
            timeframe=timeframe,
        )

    prepared: list[Any] = []
    l2_source_path: Path | None = None
    if _dataset_includes_l2(dataset_id):
        effective_l2_root = l2_root or gold_root
        l2_raw, l2_source_path = _read_latest_l2_gold_frame(
            l2_root=effective_l2_root,
            exchange=exchange,
            symbol=symbol,
        )
        raw_by_dataset["gold_l2_m1"] = l2_raw
    for dataset_type, raw_frame in raw_by_dataset.items():
        prepared.append(_prepare_dataset_frame(pl, dataset_type, raw_frame, symbol))
    if not prepared:
        raise ValueError(f"No prepared datasets for symbol={symbol} dataset_id={dataset_id}")
    key_cols = ["timestamp_m1", "exchange", "symbol"]
    merged = _build_minute_grid(pl, prepared, exchange, symbol)
    for frame in prepared:
        merged = merged.join(frame, on=key_cols, how="left", coalesce=True)
    merged = merged.sort("timestamp_m1")
    l2_validation_audit = {"l2_invalid_rows_found": 0, "l2_invalid_rows_dropped": 0}
    if _dataset_includes_l2(dataset_id):
        merged, l2_validation_audit = _validate_or_filter_l2_quality(pl, merged, l2_validation_mode)
    if merged.height == 0:
        raise ValueError(f"Gold build produced zero rows for symbol={symbol} dataset_id={dataset_id}")

    cols = merged.columns
    min_ts, max_ts, expected_minutes, missing_minutes, observed_coverage_ratio = _time_span_coverage(merged)
    source_silver_datasets = _source_dataset_summary(pl, raw_by_dataset, l2_source_path)
    source_data_hash = _json_payload_hash({"source_silver_datasets": source_silver_datasets})
    contract_signature: dict[str, object] = {
        "columns": cols,
        "join_policy": "minute_grid_left_join_coalesce",
        "source_dataset_keys": sorted(source_silver_datasets.keys()),
    }
    missing_by_column, missing_total = _missing_value_audit(pl, merged)
    feature_set_hash = _json_payload_hash(
        {
            "dataset_id": dataset_id,
            "contract_signature": contract_signature,
        }
    )
    git_hash = _git_commit_hash()
    git_short = git_hash[:8] if git_hash != "nogit" else "nogit"
    root = Path(gold_root)
    root.mkdir(parents=True, exist_ok=True)
    resolved_version = dataset_version
    previous_version: str | None = None
    version_bump_level = "manual"
    version_bump_reason = "manual_version"
    if auto_version:
        _parse_semver(version_base)
        previous_manifest = _latest_manifest_for_dataset(root, exchange, symbol, dataset_id)
        if previous_manifest is None:
            resolved_version = version_base
            version_bump_level = "initial"
            version_bump_reason = "no_previous_manifest"
        else:
            previous_version_value = previous_manifest.get("dataset_version")
            previous_version = str(previous_version_value) if isinstance(previous_version_value, str) else version_base
            _parse_semver(previous_version)
            bump_level, bump_reason = _contract_bump_level(
                previous_manifest,
                contract_signature,
                previous_source_data_hash=str(previous_manifest.get("source_data_hash", "")),
                current_source_data_hash=source_data_hash,
            )
            resolved_version = _bump_semver(previous_version, bump_level)
            version_bump_level = bump_level
            version_bump_reason = bump_reason
    else:
        _parse_semver(dataset_version)

    build_id = f"{feature_set_hash}_{source_data_hash}_{git_short}"

    manifest_payload = {
        "dataset": "gold_symbol_dataset",
        "dataset_id": dataset_id,
        "dataset_version": resolved_version,
        "feature_set_hash": feature_set_hash,
        "source_data_hash": source_data_hash,
        "git_commit_hash": git_hash,
        "build_id": build_id,
        "contract_signature": contract_signature,
        "version_bump_level": version_bump_level,
        "version_bump_reason": version_bump_reason,
        "previous_version": previous_version,
        "exchange": exchange,
        "symbol": symbol,
        "build_date_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "column_hash": _feature_hash(cols),
        "rows_out": merged.height,
        "columns": cols,
        "min_timestamp": _iso_utc(min_ts if isinstance(min_ts, datetime) else None),
        "max_timestamp": _iso_utc(max_ts if isinstance(max_ts, datetime) else None),
        "expected_minutes_in_span": expected_minutes,
        "missing_minutes_in_span": missing_minutes,
        "observed_row_coverage_ratio": observed_coverage_ratio,
        "l2_validation_mode": l2_validation_mode if _dataset_includes_l2(dataset_id) else None,
        "l2_invalid_rows_found": l2_validation_audit["l2_invalid_rows_found"]
        if _dataset_includes_l2(dataset_id)
        else None,
        "l2_invalid_rows_dropped": l2_validation_audit["l2_invalid_rows_dropped"]
        if _dataset_includes_l2(dataset_id)
        else None,
        "missing_value_count_total": missing_total,
        "missing_value_count_by_column": missing_by_column,
        "source_silver_datasets": source_silver_datasets,
        "feature_metadata": _feature_metadata(pl, merged, exchange),
    }
    hash_string = f"{feature_set_hash}_{source_data_hash}"
    feature_set_version = resolved_version
    symbol_file = symbol.replace("-", "_")
    stem = f"{symbol_file}_GOLD_{hash_string}"
    artifact_dir = (
        root
        / f"dataset_id={dataset_id}"
        / "dataset_type=gold_symbol_dataset"
        / f"feature_set_version={feature_set_version}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = artifact_dir / f"{stem}.parquet"
    merged.write_parquet(parquet_path)
    # Gold policy: always emit plot + manifest for every dataset artifact.
    _ = manifest
    _ = plot
    plot_path = artifact_dir / f"{stem}.png"
    written_plot = _write_feature_distribution_plot(merged, plot_path, normalize_y=False)
    if written_plot is None:
        raise ValueError(
            "Gold build requires plot generation for every dataset, but plot generation failed "
            "(missing matplotlib dependency or no plottable numeric columns)."
        )
    manifest_payload["plot_generated"] = True
    manifest_path = artifact_dir / f"{stem}.json"
    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    written_manifest: str | None = str(manifest_path.resolve())
    _prune_gold_versions(
        gold_root=root,
        dataset_id=dataset_id,
        exchange=exchange,
        symbol=symbol,
        keep_last_versions=keep_last_versions,
    )
    _prune_gold_artifacts(
        gold_root=root,
        dataset_id=dataset_id,
        exchange=exchange,
        symbol=symbol,
        keep_last_versions=keep_last_versions,
    )

    return GoldBuildReport(
        exchange=exchange,
        symbol=symbol,
        rows_out=merged.height,
        columns=cols,
        min_timestamp=str(manifest_payload["min_timestamp"]) if manifest_payload["min_timestamp"] is not None else None,
        max_timestamp=str(manifest_payload["max_timestamp"]) if manifest_payload["max_timestamp"] is not None else None,
        parquet_path=str(parquet_path.resolve()),
        manifest_path=written_manifest,
        plot_path=written_plot,
        hash_string=hash_string,
        dataset_id=dataset_id,
        dataset_version=resolved_version,
        feature_set_hash=feature_set_hash,
        source_data_hash=source_data_hash,
        git_commit_hash=git_hash,
        version_bump_level=version_bump_level,
        version_bump_reason=version_bump_reason,
        previous_version=previous_version,
    )
