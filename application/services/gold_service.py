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
    SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS,
    gold_dataset_contract,
    supported_gold_dataset_ids,
)
from application.services import (
    feature_metadata_service,
    feature_plot_service,
    gold_audit,
    gold_frames,
    gold_publication,
    gold_versioning,
)
from application.services.gold_incremental_planner import plan_gold_m1_incremental_months
from application.services.gold_input_fingerprint import gold_input_artifact_fingerprints, gold_input_fingerprint

_feature_hash = feature_metadata_service.feature_hash
_feature_metadata = feature_metadata_service.feature_metadata
_write_feature_distribution_plot = feature_plot_service.write_feature_distribution_plot

_FULL_MARKET_REQUIREMENTS: list[tuple[str, str]] = [
    requirement.as_tuple() for requirement in FULL_MARKET_GOLD_REQUIREMENTS
]
GOLD_DATASET_SPECS: dict[str, dict[str, object]] = {
    dataset_id: contract.legacy_spec() for dataset_id, contract in GOLD_DATASET_CONTRACTS.items()
}
SUPPORTED_GOLD_DATASET_IDS = set(supported_gold_dataset_ids())
GOLD_RETENTION_KEEP_VERSIONS = 3
_LIVE_FULL_DATASET_PREFIX = "gold.live.full."
HISTORY_FULL_HISTORY_SOURCE_COLUMNS = (
    "timestamp_m1",
    "exchange",
    "symbol",
    "spot_ohlcv_open_price",
    "spot_ohlcv_high_price",
    "spot_ohlcv_low_price",
    "spot_ohlcv_close_price",
    "spot_ohlcv_volume",
    "spot_ohlcv_quote_volume",
    "spot_ohlcv_trade_count",
    "perp_open_price",
    "perp_high_price",
    "perp_low_price",
    "perp_close_price",
    "perp_volume",
    "perp_quote_volume",
    "perp_trade_count",
    "funding_rate_last_known",
    "funding_observed_at",
    "minutes_since_funding",
    "is_funding_observation_minute",
    "funding_data_available",
    "open_interest_open_interest",
    "open_interest_is_observed",
    "open_interest_is_ffill",
    "minutes_since_open_interest_observation",
    "open_interest_observation_lag_sec",
    "open_interest_source_timestamp",
    "perps_trades_open_price",
    "perps_trades_high_price",
    "perps_trades_low_price",
    "perps_trades_close_price",
    "perps_trades_volume",
    "perps_trades_quote_volume",
    "perps_trades_trade_count",
    "perps_trades_buy_volume",
    "perps_trades_sell_volume",
    "perps_trades_buy_trade_count",
    "perps_trades_sell_trade_count",
    "perps_trades_buy_volume_share",
    "options_trades_open_price",
    "options_trades_high_price",
    "options_trades_low_price",
    "options_trades_close_price",
    "options_trades_volume",
    "options_trades_quote_volume",
    "options_trades_trade_count",
    "options_trades_buy_volume",
    "options_trades_sell_volume",
    "options_trades_buy_trade_count",
    "options_trades_sell_trade_count",
    "options_trades_buy_volume_share",
)
EXTENDED_HISTORY_FULL_HISTORY_SOURCE_COLUMNS = (
    *HISTORY_FULL_HISTORY_SOURCE_COLUMNS,
    *SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS[3:],
)
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
_HISTORY_FULL_DERIVED_INTERVALS = {
    "gold.history.full.m5": "5m",
    "gold.history.full.m30": "30m",
    "gold.history.full.h1": "1h",
    "gold.history.extended.m5": "5m",
    "gold.history.extended.m30": "30m",
    "gold.history.extended.h1": "1h",
    "gold.live.extended.m5": "5m",
    "gold.live.extended.m30": "30m",
    "gold.live.extended.h1": "1h",
    "gold.live.full.m5": "5m",
    "gold.live.full.m30": "30m",
    "gold.live.full.h1": "1h",
}
_HISTORY_FULL_DERIVED_SOURCE_DATASET_IDS = {
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
_LIVE_FULL_NON_GRID_DATASETS = {
    "recent_trade_snapshot_1m_observed",
    "instrument_metadata_snapshot_daily_observed",
    "futures_instrument_metadata_snapshot_daily_observed",
}
_parse_semver = gold_versioning.parse_semver
_format_semver = gold_versioning.format_semver
_bump_semver = gold_versioning.bump_semver
_latest_manifest_for_dataset = gold_versioning.latest_manifest_for_dataset
_extract_feature_set_version = gold_versioning.extract_feature_set_version
_prune_gold_versions = gold_versioning.prune_gold_versions
_prune_gold_artifacts = gold_versioning.prune_gold_artifacts
_contract_bump_level = gold_versioning.contract_bump_level


def validate_gold_retention_keep_versions(keep_last_versions: int) -> int:
    """Return the fixed Gold retention window or fail on unsupported values."""

    if keep_last_versions != GOLD_RETENTION_KEEP_VERSIONS:
        raise ValueError(
            "--retention-keep-versions is fixed at "
            f"{GOLD_RETENTION_KEEP_VERSIONS} versions; received {keep_last_versions}"
        )
    return GOLD_RETENTION_KEEP_VERSIONS


def _retention_keep_versions_for_dataset(dataset_id: str, keep_last_versions: int) -> int:
    """Return the artifact retention count appropriate for a Gold dataset lineage."""

    if dataset_id.startswith(_LIVE_FULL_DATASET_PREFIX):
        return 1
    return keep_last_versions


def _select_history_full_canonical_columns(merged: Any) -> Any:
    """Keep only canonical history-full columns owned by this repository."""

    return merged.select(list(HISTORY_FULL_HISTORY_SOURCE_COLUMNS))


def _select_extended_history_full_columns(merged: Any) -> Any:
    """Keep canonical history-full columns plus extended history-derived features."""

    return merged.select(list(EXTENDED_HISTORY_FULL_HISTORY_SOURCE_COLUMNS))


def _history_full_derived_interval(dataset_id: str) -> str | None:
    """Return the bucket interval for a derived history-full Gold dataset."""

    return _HISTORY_FULL_DERIVED_INTERVALS.get(dataset_id)


def _history_full_source_dataset_id(dataset_id: str) -> str | None:
    """Return the canonical minute dataset used to derive a history-full variant."""

    return _HISTORY_FULL_DERIVED_SOURCE_DATASET_IDS.get(dataset_id)


def _read_latest_gold_dataset_artifact(
    *,
    gold_root: str,
    dataset_id: str,
    exchange: str,
    symbol: str,
) -> tuple[Any, Path, dict[str, object]]:
    """Load the newest Gold parquet and manifest for one dataset lineage."""

    pl = _require_polars()
    root = Path(gold_root)
    dataset_root = root / f"dataset_id={dataset_id}" / "dataset_type=gold_symbol_dataset"
    candidate_paths = sorted(
        dataset_root.glob(f"feature_set_version=*/exchange={exchange}/symbol={symbol}/*.parquet"),
        key=lambda path: (path.stat().st_mtime, str(path)),
    )
    if not candidate_paths:
        raise ValueError(f"Missing gold dataset for symbol={symbol}: {dataset_id}")
    parquet_path = candidate_paths[-1]
    manifest_path = parquet_path.with_suffix(".json")
    if not manifest_path.exists():
        raise ValueError(f"Missing gold manifest for symbol={symbol}: {dataset_id}")
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest_payload.get("dataset_id") != dataset_id:
        raise ValueError(f"Gold artifact lineage mismatch for symbol={symbol}: {dataset_id}")
    return pl.read_parquet(str(parquet_path)), parquet_path, manifest_payload


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


@dataclass(frozen=True)
class GoldTimeframeFanout:
    """Validated M1 source and deterministic derived frames for one Gold symbol."""

    source_dataset_id: str
    source_frame: Any
    source_parquet_path: Path
    source_manifest: dict[str, object]
    frames_by_dataset_id: dict[str, Any]


def prepare_gold_timeframe_fanout(
    *,
    gold_root: str,
    exchange: str,
    symbol: str,
    dataset_ids: list[str],
) -> GoldTimeframeFanout:
    """Read one M1 Gold source and derive all requested sibling timeframes once.

    Every requested dataset must share the same declared M1 source.  Keeping this
    preparation separate from publication lets the caller validate the complete
    sibling set before it exposes any child artifact.

    Args:
        gold_root: Gold lake root containing the published M1 source artifact.
        exchange: Exchange partition identifier.
        symbol: Canonical or exchange-specific symbol to normalize.
        dataset_ids: Derived timeframe dataset IDs to prepare.

    Returns:
        Shared source lineage and one derived frame per requested dataset ID.

    Raises:
        ValueError: If no dataset IDs are supplied or their source lineage differs.
    """

    if not dataset_ids:
        raise ValueError("Gold timeframe fan-out requires at least one derived dataset ID")
    source_ids: set[str] = set()
    intervals: dict[str, str] = {}
    for dataset_id in sorted(set(dataset_ids)):
        source_dataset_id = _history_full_source_dataset_id(dataset_id)
        interval = _history_full_derived_interval(dataset_id)
        if source_dataset_id is None or interval is None:
            raise ValueError(f"Unsupported derived history_full dataset_id: {dataset_id}")
        source_ids.add(source_dataset_id)
        intervals[dataset_id] = interval
    if len(source_ids) != 1:
        raise ValueError("Gold timeframe fan-out datasets must share one M1 source")

    normalized_symbol = normalize_symbol(symbol)
    source_dataset_id = next(iter(source_ids))
    source_frame, source_parquet_path, source_manifest = _read_latest_gold_dataset_artifact(
        gold_root=gold_root,
        dataset_id=source_dataset_id,
        exchange=exchange,
        symbol=normalized_symbol,
    )
    pl = _require_polars()
    frames_by_dataset_id = {
        dataset_id: gold_frames.resample_history_full_frame(pl, source_frame, intervals[dataset_id])
        for dataset_id in sorted(intervals)
    }
    if any(frame.height == 0 for frame in frames_by_dataset_id.values()):
        raise ValueError(f"Gold timeframe fan-out produced zero rows for symbol={normalized_symbol}")
    return GoldTimeframeFanout(
        source_dataset_id=source_dataset_id,
        source_frame=source_frame,
        source_parquet_path=source_parquet_path,
        source_manifest=source_manifest,
        frames_by_dataset_id=frames_by_dataset_id,
    )


def _existing_gold_report_if_unchanged(
    *,
    parquet_path: Path,
    manifest_path: Path,
    plot_path: Path,
    input_fingerprint: str,
    feature_set_hash: str,
    source_data_hash: str,
) -> GoldBuildReport | None:
    """Return the published report when its validated input identity is unchanged.

    The manifest is checked together with the parquet path because an interrupted old
    build may leave an unreferenced parquet behind.  Such a file is never a cache hit.
    """

    if not parquet_path.is_file() or not manifest_path.is_file():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if (
        payload.get("input_fingerprint") != input_fingerprint
        or payload.get("feature_set_hash") != feature_set_hash
        or payload.get("source_data_hash") != source_data_hash
    ):
        return None
    try:
        _require_polars().read_parquet(str(parquet_path), n_rows=1)
    except Exception:
        # A matching manifest alone is insufficient: corrupted parquet must be rebuilt.
        return None
    columns = payload.get("columns")
    rows_out = payload.get("rows_out")
    if not isinstance(columns, list) or not isinstance(rows_out, int):
        return None
    return GoldBuildReport(
        exchange=str(payload.get("exchange", "")),
        symbol=str(payload.get("symbol", "")),
        rows_out=rows_out,
        columns=[str(column) for column in columns],
        min_timestamp=str(payload["min_timestamp"]) if payload.get("min_timestamp") is not None else None,
        max_timestamp=str(payload["max_timestamp"]) if payload.get("max_timestamp") is not None else None,
        parquet_path=str(parquet_path.resolve()),
        manifest_path=str(manifest_path.resolve()),
        plot_path=str(plot_path.resolve()) if plot_path.is_file() else None,
        hash_string=f"{feature_set_hash}_{source_data_hash}",
        dataset_id=str(payload.get("dataset_id", "")),
        dataset_version=str(payload.get("dataset_version", "")),
        feature_set_hash=feature_set_hash,
        source_data_hash=source_data_hash,
        git_commit_hash=str(payload.get("git_commit_hash", "")),
        version_bump_level="none",
        version_bump_reason="unchanged_input",
        previous_version=str(payload["previous_version"]) if payload.get("previous_version") is not None else None,
    )


def _unchanged_gold_report_from_manifest(
    *,
    gold_root: Path,
    exchange: str,
    symbol: str,
    dataset_id: str,
    input_fingerprint: str,
    manifest_payload: dict[str, object] | None,
    expected_dataset_version: str | None,
) -> GoldBuildReport | None:
    """Resolve a valid unchanged Gold artifact before expensive frame preparation."""

    if manifest_payload is None or manifest_payload.get("input_fingerprint") != input_fingerprint:
        return None
    if expected_dataset_version is not None and manifest_payload.get("dataset_version") != expected_dataset_version:
        return None
    feature_set_hash = manifest_payload.get("feature_set_hash")
    source_data_hash = manifest_payload.get("source_data_hash")
    dataset_version = manifest_payload.get("dataset_version")
    if (
        not isinstance(feature_set_hash, str)
        or not feature_set_hash
        or not isinstance(source_data_hash, str)
        or not source_data_hash
        or not isinstance(dataset_version, str)
        or not dataset_version
    ):
        return None
    symbol_file = symbol.replace("-", "_")
    artifact_dir = (
        gold_root
        / f"dataset_id={dataset_id}"
        / "dataset_type=gold_symbol_dataset"
        / f"feature_set_version={dataset_version}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
    )
    stem = f"{symbol_file}_GOLD_{feature_set_hash}_{source_data_hash}"
    return _existing_gold_report_if_unchanged(
        parquet_path=artifact_dir / f"{stem}.parquet",
        manifest_path=artifact_dir / f"{stem}.json",
        plot_path=artifact_dir / f"{stem}.png",
        input_fingerprint=input_fingerprint,
        feature_set_hash=feature_set_hash,
        source_data_hash=source_data_hash,
    )


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

    base_dataset_id = _history_full_source_dataset_id(dataset_id)
    if base_dataset_id is not None:
        return discover_gold_symbols_for_dataset(silver_root=silver_root, exchange=exchange, dataset_id=base_dataset_id)
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


def _dataset_optional_requirements(dataset_id: str) -> list[tuple[str, str]]:
    return [requirement.as_tuple() for requirement in gold_dataset_contract(dataset_id).optional_requirements]


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


def _prepare_open_interest(pl: Any, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_open_interest(pl, frame, symbol)


def _prepare_funding(pl: Any, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_funding(pl, frame, symbol)


def _prepare_trades(pl: Any, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_trades(pl, frame, symbol)


def _prepare_options_trades(pl: Any, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_options_trades(pl, frame, symbol)


def _prepare_volatility_index_data(pl: Any, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_volatility_index_data(pl, frame, symbol)


def _prepare_dataset_frame(pl: Any, dataset_type: str, frame: Any, symbol: str) -> Any:
    return gold_frames.prepare_dataset_frame(pl, dataset_type, frame, symbol)


def _optional_feature_schema(pl: Any, dataset_type: str) -> list[tuple[str, Any]]:
    return gold_frames.optional_feature_schema(pl, dataset_type)


def _strategy_feature_lookbacks(dataset_id: str) -> dict[str, str]:
    if dataset_id == "gold.market.regime_features.m1":
        return gold_frames.strategy_feature_lookbacks()
    return {}


def _feature_lookback_minutes(dataset_id: str) -> int:
    """Return the longest trailing Gold feature dependency for incremental M1 planning."""

    lookbacks = _strategy_feature_lookbacks(dataset_id).values()
    minutes = [int(value.removesuffix("m")) for value in lookbacks if value.endswith("m")]
    return max(minutes, default=0)


def _prediction_target_definitions(dataset_id: str) -> dict[str, object]:
    if dataset_id == "gold.market.prediction_targets.m1":
        return gold_frames.prediction_target_definitions()
    return {}


def _origin_repository(dataset_id: str) -> str:
    if dataset_id.startswith("gold.live."):
        return "crypto-live-loader"
    return "crypto-history-loader"


def _add_strategy_feature_families(pl: Any, frame: Any, dataset_id: str) -> Any:
    if dataset_id == "gold.market.regime_features.m1":
        return gold_frames.add_strategy_feature_families(pl, frame)
    return frame


def _add_prediction_targets(pl: Any, frame: Any, dataset_id: str) -> Any:
    if dataset_id == "gold.market.prediction_targets.m1":
        return gold_frames.add_prediction_target_columns(pl, frame)
    return frame


def _add_live_extended_feature_families(pl: Any, frame: Any, dataset_id: str) -> Any:
    if dataset_id == "gold.live.extended.m1":
        return gold_frames.add_live_extended_feature_families(pl, frame)
    return frame


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


def _optional_source_availability(
    pl: Any,
    optional_requirements: list[tuple[str, str]],
    raw_by_dataset: dict[str, Any],
    prepared_by_dataset: dict[str, Any],
    required_grid: Any,
) -> dict[str, dict[str, object]]:
    return gold_audit.optional_source_availability(
        pl,
        optional_requirements,
        raw_by_dataset,
        prepared_by_dataset,
        required_grid,
    )


def _missing_value_audit(pl: Any, frame: Any) -> tuple[dict[str, int], int]:
    return gold_audit.missing_value_audit(pl, frame)


def _build_history_full_derived_for_symbol(
    *,
    gold_root: str,
    exchange: str,
    symbol: str,
    dataset_id: str,
    dataset_version: str,
    auto_version: bool,
    version_base: str,
    keep_last_versions: int,
    plot: bool = False,
    prepared_fanout: GoldTimeframeFanout | None = None,
    publication_requests: list[gold_publication.GoldArtifactPublishRequest] | None = None,
) -> GoldBuildReport:
    """Build a coarser history-full Gold dataset from the canonical minute artifact."""

    interval = _history_full_derived_interval(dataset_id)
    if interval is None:
        raise ValueError(f"Unsupported derived history_full dataset_id: {dataset_id}")
    source_dataset_id = _history_full_source_dataset_id(dataset_id)
    if source_dataset_id is None:
        raise ValueError(f"Unsupported derived history_full dataset_id: {dataset_id}")
    symbol = normalize_symbol(symbol)
    if prepared_fanout is None:
        source_frame, source_parquet_path, source_manifest = _read_latest_gold_dataset_artifact(
            gold_root=gold_root,
            dataset_id=source_dataset_id,
            exchange=exchange,
            symbol=symbol,
        )
        pl = _require_polars()
        merged = gold_frames.resample_history_full_frame(pl, source_frame, interval)
    else:
        if prepared_fanout.source_dataset_id != source_dataset_id:
            raise ValueError("Gold timeframe fan-out source lineage does not match derived dataset")
        source_frame = prepared_fanout.source_frame
        source_parquet_path = prepared_fanout.source_parquet_path
        source_manifest = prepared_fanout.source_manifest
        try:
            merged = prepared_fanout.frames_by_dataset_id[dataset_id]
        except KeyError as exc:
            raise ValueError(f"Gold timeframe fan-out did not prepare dataset_id={dataset_id}") from exc
        pl = _require_polars()
    if merged.height == 0:
        raise ValueError(f"Gold build produced zero rows for symbol={symbol} dataset_id={dataset_id}")

    cols = merged.columns
    min_ts, max_ts, _, _, _ = _time_span_coverage(merged)
    source_summary = {
        source_dataset_id: {
            "columns": source_frame.columns,
            "rows": source_frame.height,
            "source_symbols": [symbol],
            "source_artifact": source_parquet_path.name,
            "source_dataset_version": source_manifest.get("dataset_version"),
        }
    }
    source_data_hash = _json_payload_hash(
        {
            "source_dataset_id": source_dataset_id,
            "source_dataset_version": source_manifest.get("dataset_version"),
            "source_feature_set_hash": source_manifest.get("feature_set_hash"),
            "source_source_data_hash": source_manifest.get("source_data_hash"),
            "source_rows": source_manifest.get("rows_out"),
            "source_columns": source_manifest.get("columns"),
        }
    )
    contract_signature: dict[str, object] = {
        "columns": cols,
        "join_policy": f"history_full_resample_{interval}",
        "source_dataset_keys": [source_dataset_id],
        "resample_interval": interval,
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
        "origin_repository": _origin_repository(dataset_id),
        "exchange": exchange,
        "symbol": symbol,
        "build_date_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "column_hash": _feature_hash(cols),
        "rows_out": merged.height,
        "columns": cols,
        "min_timestamp": _iso_utc(min_ts if isinstance(min_ts, datetime) else None),
        "max_timestamp": _iso_utc(max_ts if isinstance(max_ts, datetime) else None),
        "expected_minutes_in_span": None,
        "missing_minutes_in_span": None,
        "observed_row_coverage_ratio": None,
        "l2_validation_mode": None,
        "l2_invalid_rows_found": None,
        "l2_invalid_rows_dropped": None,
        "missing_value_count_total": missing_total,
        "missing_value_count_by_column": missing_by_column,
        "source_silver_datasets": source_summary,
        "required_source_datasets": [source_dataset_id],
        "optional_source_datasets": [],
        "optional_source_availability": {},
        "strategy_feature_lookbacks": {},
        "prediction_target_definitions": {},
        "feature_metadata": _feature_metadata(pl, merged, exchange),
        "resample_interval": interval,
        "source_dataset_id": source_dataset_id,
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
    parquet_path = artifact_dir / f"{stem}.parquet"
    plot_path = artifact_dir / f"{stem}.png"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    written_plot = _write_feature_distribution_plot(merged, plot_path, normalize_y=False) if plot else None
    manifest_payload["plot_generated"] = written_plot is not None
    manifest_path = artifact_dir / f"{stem}.json"
    publish_request = gold_publication.GoldArtifactPublishRequest(
        frame=merged,
        parquet_path=parquet_path,
        manifest_path=manifest_path,
        manifest_payload=manifest_payload,
    )
    if publication_requests is None:
        gold_publication.publish_gold_artifact_atomically(
            frame=merged,
            parquet_path=parquet_path,
            manifest_path=manifest_path,
            manifest_payload=manifest_payload,
        )
    else:
        publication_requests.append(publish_request)
    written_manifest: str | None = str(manifest_path.resolve())
    if publication_requests is None:
        retention_keep_versions = _retention_keep_versions_for_dataset(dataset_id, keep_last_versions)
        _prune_gold_versions(
            gold_root=root,
            dataset_id=dataset_id,
            exchange=exchange,
            symbol=symbol,
            keep_last_versions=retention_keep_versions,
        )
        _prune_gold_artifacts(
            gold_root=root,
            dataset_id=dataset_id,
            exchange=exchange,
            symbol=symbol,
            keep_last_versions=retention_keep_versions,
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


def build_gold_timeframe_fanout_for_symbol(
    *,
    gold_root: str,
    exchange: str,
    symbol: str,
    dataset_ids: list[str],
    dataset_version: str = "v1.0.0",
    auto_version: bool = False,
    version_base: str = "v1.0.0",
    keep_last_versions: int = GOLD_RETENTION_KEEP_VERSIONS,
    plot: bool = False,
) -> list[GoldBuildReport]:
    """Build and atomically publish sibling Gold timeframes from one validated M1 source.

    Args:
        gold_root: Gold lake root containing the source and derived artifacts.
        exchange: Exchange partition identifier.
        symbol: Asset symbol whose siblings are built.
        dataset_ids: Derived sibling dataset IDs sharing one M1 source.
        dataset_version: Explicit semantic version when automatic versioning is disabled.
        auto_version: Whether to derive each sibling version from its latest manifest.
        version_base: Semantic version for each sibling's initial automatic publication.
        keep_last_versions: Fixed Gold retention count.
        plot: Whether to generate optional distribution plots after frame preparation.

    Returns:
        Reports for all published sibling artifacts in deterministic dataset-ID order.

    Raises:
        ValueError: If the sibling set is invalid or any prepared frame is empty.
    """

    keep_last_versions = validate_gold_retention_keep_versions(keep_last_versions)
    normalized_symbol = normalize_symbol(symbol)
    ordered_dataset_ids = sorted(set(dataset_ids))
    fanout = prepare_gold_timeframe_fanout(
        gold_root=gold_root,
        exchange=exchange,
        symbol=normalized_symbol,
        dataset_ids=ordered_dataset_ids,
    )
    publication_requests: list[gold_publication.GoldArtifactPublishRequest] = []
    reports = [
        _build_history_full_derived_for_symbol(
            gold_root=gold_root,
            exchange=exchange,
            symbol=normalized_symbol,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            auto_version=auto_version,
            version_base=version_base,
            keep_last_versions=keep_last_versions,
            plot=plot,
            prepared_fanout=fanout,
            publication_requests=publication_requests,
        )
        for dataset_id in ordered_dataset_ids
    ]
    gold_publication.publish_gold_artifacts_atomically(requests=publication_requests)
    root = Path(gold_root)
    for dataset_id in ordered_dataset_ids:
        retention_keep_versions = _retention_keep_versions_for_dataset(dataset_id, keep_last_versions)
        _prune_gold_versions(
            gold_root=root,
            dataset_id=dataset_id,
            exchange=exchange,
            symbol=normalized_symbol,
            keep_last_versions=retention_keep_versions,
        )
        _prune_gold_artifacts(
            gold_root=root,
            dataset_id=dataset_id,
            exchange=exchange,
            symbol=normalized_symbol,
            keep_last_versions=retention_keep_versions,
        )
    return reports


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

    keep_last_versions = validate_gold_retention_keep_versions(keep_last_versions)
    derived_interval = _history_full_derived_interval(dataset_id)
    if derived_interval is not None:
        return _build_history_full_derived_for_symbol(
            gold_root=gold_root,
            exchange=exchange,
            symbol=symbol,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            auto_version=auto_version,
            version_base=version_base,
            keep_last_versions=keep_last_versions,
            plot=plot,
        )
    pl = _require_polars()
    symbol = normalize_symbol(symbol)
    required = _dataset_requirements(dataset_id)
    optional = _dataset_optional_requirements(dataset_id)
    required_artifacts = {
        dataset_type: gold_frames.dataset_artifact_paths(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            dataset_type=dataset_type,
            timeframe=timeframe,
        )
        for dataset_type, timeframe in required
    }
    optional_artifacts = {
        dataset_type: gold_frames.dataset_artifact_paths(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            dataset_type=dataset_type,
            timeframe=timeframe,
        )
        for dataset_type, timeframe in optional
    }
    for dataset_type, _timeframe in required:
        if not required_artifacts[dataset_type]:
            # Preserve the established Gold error contract while failing before any
            # fingerprint or partial artifact can make a missing source look valid.
            raise ValueError(f"Missing silver dataset for symbol={symbol}: {dataset_type}")
    feature_configuration: dict[str, object] = {
        "strategy_feature_lookbacks": _strategy_feature_lookbacks(dataset_id),
        "prediction_target_definitions": _prediction_target_definitions(dataset_id),
        "live_extended_feature_families": dataset_id == "gold.live.extended.m1",
    }
    input_fingerprint = gold_input_fingerprint(
        root=Path(silver_root),
        required_files=required_artifacts,
        optional_files=optional_artifacts,
        dataset_id=dataset_id,
        contract_version="gold-input/v1",
        feature_configuration=feature_configuration,
    )
    input_artifact_fingerprints = gold_input_artifact_fingerprints(
        root=Path(silver_root),
        required_files=required_artifacts,
        optional_files=optional_artifacts,
    )
    prior_manifest_for_plan = _latest_manifest_for_dataset(Path(gold_root), exchange, symbol, dataset_id)
    unchanged_report = _unchanged_gold_report_from_manifest(
        gold_root=Path(gold_root),
        exchange=exchange,
        symbol=symbol,
        dataset_id=dataset_id,
        input_fingerprint=input_fingerprint,
        manifest_payload=prior_manifest_for_plan,
        expected_dataset_version=None if auto_version else dataset_version,
    )
    if unchanged_report is not None:
        return unchanged_report
    prior_artifact_fingerprints = (
        prior_manifest_for_plan.get("input_artifact_fingerprints")
        if isinstance(prior_manifest_for_plan, dict)
        else None
    )
    previous_artifact_fingerprints: dict[str, str] | None = None
    if isinstance(prior_artifact_fingerprints, dict) and all(
        isinstance(key, str) and isinstance(value, str) for key, value in prior_artifact_fingerprints.items()
    ):
        previous_artifact_fingerprints = {str(key): str(value) for key, value in prior_artifact_fingerprints.items()}
    incremental_m1_plan = plan_gold_m1_incremental_months(
        current_artifacts=input_artifact_fingerprints,
        previous_artifacts=previous_artifact_fingerprints,
        feature_lookback_minutes=_feature_lookback_minutes(dataset_id),
    )
    raw_by_dataset: dict[str, Any] = {}
    required_prepared_by_dataset: list[tuple[str, Any]] = []
    for dataset_type, timeframe in required:
        raw_by_dataset[dataset_type] = _read_dataset_frame(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            dataset_type=dataset_type,
            timeframe=timeframe,
        )

    l2_source_path: Path | None = None
    if _dataset_includes_l2(dataset_id):
        effective_l2_root = l2_root or gold_root
        l2_raw, l2_source_path = _read_latest_l2_gold_frame(
            l2_root=effective_l2_root,
            exchange=exchange,
            symbol=symbol,
        )
        raw_by_dataset["gold_l2_m1"] = l2_raw
    for dataset_type, _timeframe in required:
        required_prepared_by_dataset.append(
            (dataset_type, _prepare_dataset_frame(pl, dataset_type, raw_by_dataset[dataset_type], symbol))
        )
    if _dataset_includes_l2(dataset_id):
        required_prepared_by_dataset.append(
            ("gold_l2_m1", _prepare_dataset_frame(pl, "gold_l2_m1", raw_by_dataset["gold_l2_m1"], symbol))
        )
    if not required_prepared_by_dataset:
        raise ValueError(f"No prepared datasets for symbol={symbol} dataset_id={dataset_id}")
    key_cols = ["timestamp_m1", "exchange", "symbol"]
    grid_prepared = [
        frame
        for dataset_type, frame in required_prepared_by_dataset
        if dataset_id not in {"gold.live.full.m1", "gold.live.extended.m1"}
        or dataset_type not in _LIVE_FULL_NON_GRID_DATASETS
    ]
    if not grid_prepared:
        grid_prepared = [frame for _dataset_type, frame in required_prepared_by_dataset]
    merged = _build_minute_grid(pl, grid_prepared, exchange, symbol)
    for _dataset_type, frame in required_prepared_by_dataset:
        merged = merged.join(frame, on=key_cols, how="left", coalesce=True)
    required_grid = merged.select(key_cols)

    optional_prepared_by_dataset: dict[str, Any] = {}
    for dataset_type, timeframe in optional:
        available_symbols = _discover_symbols_for_dataset(
            silver_root=silver_root,
            exchange=exchange,
            dataset_type=dataset_type,
            timeframe=timeframe,
        )
        if symbol in available_symbols:
            optional_raw = _read_dataset_frame(
                silver_root=silver_root,
                exchange=exchange,
                symbol=symbol,
                dataset_type=dataset_type,
                timeframe=timeframe,
            )
            raw_by_dataset[dataset_type] = optional_raw
            optional_prepared = _prepare_dataset_frame(pl, dataset_type, optional_raw, symbol)
            optional_prepared_by_dataset[dataset_type] = optional_prepared
            merged = merged.join(optional_prepared, on=key_cols, how="left", coalesce=True)
        else:
            merged = merged.with_columns(
                [
                    pl.lit(None, dtype=dtype).alias(column)
                    for column, dtype in _optional_feature_schema(pl, dataset_type)
                ]
            )
    merged = _add_strategy_feature_families(pl, merged.sort("timestamp_m1"), dataset_id)
    merged = _add_prediction_targets(pl, merged, dataset_id)
    merged = _add_live_extended_feature_families(pl, merged, dataset_id)
    if dataset_id == "gold.history.full.m1":
        merged = _select_history_full_canonical_columns(merged)
    elif dataset_id in {"gold.history.extended.m1", "gold.history.extended_full.m1"}:
        merged = _select_extended_history_full_columns(merged)
    l2_validation_audit = {"l2_invalid_rows_found": 0, "l2_invalid_rows_dropped": 0}
    if _dataset_includes_l2(dataset_id):
        merged, l2_validation_audit = _validate_or_filter_l2_quality(pl, merged, l2_validation_mode)
    if merged.height == 0:
        raise ValueError(f"Gold build produced zero rows for symbol={symbol} dataset_id={dataset_id}")

    cols = merged.columns
    min_ts, max_ts, expected_minutes, missing_minutes, observed_coverage_ratio = _time_span_coverage(merged)
    source_silver_datasets = _source_dataset_summary(pl, raw_by_dataset, l2_source_path)
    optional_source_availability = _optional_source_availability(
        pl,
        optional,
        raw_by_dataset,
        optional_prepared_by_dataset,
        required_grid,
    )
    for dataset_type, _timeframe in optional:
        source_summary = source_silver_datasets.setdefault(
            dataset_type,
            {"columns": [], "rows": 0, "source_symbols": []},
        )
        source_summary["available"] = bool(optional_source_availability[dataset_type]["available"])
    source_data_hash = _json_payload_hash(
        {
            "input_fingerprint": input_fingerprint,
            "source_silver_datasets": source_silver_datasets,
            "optional_source_availability": optional_source_availability,
        }
    )
    configured_source_keys = [
        f"{dataset_type}_1m" if dataset_type in {"spot_ohlcv", "perps_ohlcv"} else dataset_type
        for dataset_type, _timeframe in [*required, *optional]
    ]
    if _dataset_includes_l2(dataset_id):
        configured_source_keys.append("gold_l2_m1")
    contract_signature: dict[str, object] = {
        "columns": cols,
        "join_policy": (
            "required_minute_grid_left_join_optional_nullable" if optional else "minute_grid_left_join_coalesce"
        ),
        "source_dataset_keys": sorted(configured_source_keys),
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
        "input_fingerprint": input_fingerprint,
        "input_artifact_fingerprints": input_artifact_fingerprints,
        "incremental_m1_plan": {
            "changed_months": list(incremental_m1_plan.changed_months),
            "rebuild_months": list(incremental_m1_plan.rebuild_months),
            "feature_lookback_minutes": incremental_m1_plan.feature_lookback_minutes,
        },
        "git_commit_hash": git_hash,
        "build_id": build_id,
        "contract_signature": contract_signature,
        "version_bump_level": version_bump_level,
        "version_bump_reason": version_bump_reason,
        "previous_version": previous_version,
        "origin_repository": _origin_repository(dataset_id),
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
        "required_source_datasets": [dataset_type for dataset_type, _timeframe in required],
        "optional_source_datasets": [dataset_type for dataset_type, _timeframe in optional],
        "optional_source_availability": optional_source_availability,
        "strategy_feature_lookbacks": _strategy_feature_lookbacks(dataset_id),
        "prediction_target_definitions": _prediction_target_definitions(dataset_id),
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
    parquet_path = artifact_dir / f"{stem}.parquet"
    # Gold policy: always emit plot + manifest for every dataset artifact.
    _ = manifest
    _ = plot
    plot_path = artifact_dir / f"{stem}.png"
    manifest_path = artifact_dir / f"{stem}.json"
    unchanged_report = _existing_gold_report_if_unchanged(
        parquet_path=parquet_path,
        manifest_path=manifest_path,
        plot_path=plot_path,
        input_fingerprint=input_fingerprint,
        feature_set_hash=feature_set_hash,
        source_data_hash=source_data_hash,
    )
    if unchanged_report is not None:
        return unchanged_report
    artifact_dir.mkdir(parents=True, exist_ok=True)
    written_plot = _write_feature_distribution_plot(merged, plot_path, normalize_y=False) if plot else None
    manifest_payload["plot_generated"] = written_plot is not None
    gold_publication.publish_gold_artifact_atomically(
        frame=merged,
        parquet_path=parquet_path,
        manifest_path=manifest_path,
        manifest_payload=manifest_payload,
    )
    written_manifest: str | None = str(manifest_path.resolve())
    retention_keep_versions = _retention_keep_versions_for_dataset(dataset_id, keep_last_versions)
    _prune_gold_versions(
        gold_root=root,
        dataset_id=dataset_id,
        exchange=exchange,
        symbol=symbol,
        keep_last_versions=retention_keep_versions,
    )
    _prune_gold_artifacts(
        gold_root=root,
        dataset_id=dataset_id,
        exchange=exchange,
        symbol=symbol,
        keep_last_versions=retention_keep_versions,
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
