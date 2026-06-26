"""Gold transformation service for per-symbol model-ready datasets."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ingestion import feature_profile

_feature_hash = feature_profile.feature_hash
_feature_metadata = feature_profile.feature_metadata
_write_feature_distribution_plot = feature_profile.write_feature_distribution_plot

_FULL_MARKET_REQUIREMENTS: list[tuple[str, str]] = [
    ("spot", "1m"),
    ("perp", "1m"),
    ("oi_1m_feature", "1m"),
    ("funding_1m_feature", "1m"),
    ("perp_trades_1m_feature", "1m"),
    ("option_trades_1m_feature", "1m"),
    ("volatility_index_data_observed", "1m"),
]
GOLD_DATASET_SPECS: dict[str, dict[str, object]] = {
    "gold.market.perp_trades.m1": {
        "requirements": [("perp_trades_1m_feature", "1m")],
        "include_l2": False,
    },
    "gold.market.option_trades.m1": {
        "requirements": [("option_trades_1m_feature", "1m")],
        "include_l2": False,
    },
    "gold.market.core.m1": {
        "requirements": [("spot", "1m"), ("perp", "1m")],
        "include_l2": False,
    },
    "gold.market.core_funding.m1": {
        "requirements": [("spot", "1m"), ("perp", "1m"), ("funding_1m_feature", "1m")],
        "include_l2": False,
    },
    "gold.market.full.m1": {
        "requirements": _FULL_MARKET_REQUIREMENTS,
        "include_l2": False,
    },
    "gold.hybrid.full_l2.m1": {
        "requirements": _FULL_MARKET_REQUIREMENTS,
        "include_l2": True,
    },
}
SUPPORTED_GOLD_DATASET_IDS = set(GOLD_DATASET_SPECS.keys())


def _require_polars() -> Any:
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for gold-build. Install project dependencies.") from exc
    return pl


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


_SEMVER_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


def _parse_semver(version: str) -> tuple[int, int, int]:
    match = _SEMVER_RE.fullmatch(version)
    if match is None:
        raise ValueError(f"Invalid semantic version '{version}'. Expected format like v1.0.0")
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def _format_semver(major: int, minor: int, patch: int) -> str:
    return f"v{major}.{minor}.{patch}"


def _bump_semver(version: str, level: str) -> str:
    major, minor, patch = _parse_semver(version)
    if level == "major":
        return _format_semver(major + 1, 0, 0)
    if level == "minor":
        return _format_semver(major, minor + 1, 0)
    if level == "patch":
        return _format_semver(major, minor, patch + 1)
    if level == "none":
        return version
    raise ValueError(f"Unsupported semver bump level: {level}")


def _latest_manifest_for_dataset(
    gold_root: Path, exchange: str, symbol: str, dataset_id: str
) -> dict[str, object] | None:
    dataset_root = gold_root / f"dataset_id={dataset_id}" / f"exchange={exchange}" / f"symbol={symbol}"
    if not dataset_root.exists():
        return None
    latest_payload: dict[str, object] | None = None
    latest_mtime = -1.0
    candidate_paths = list(dataset_root.glob("version=*/build_id=*/manifest.json"))
    candidate_paths.extend(
        dataset_root.glob("dataset_type=gold_symbol_dataset/feature_set_version=*/exchange=*/symbol=*/*.json")
    )
    for path in candidate_paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if payload.get("dataset_id") != dataset_id:
            continue
        mtime = path.stat().st_mtime
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_payload = payload
    return latest_payload


def _extract_feature_set_version(version_dir: Path) -> str | None:
    segment = version_dir.name
    if not segment.startswith("feature_set_version="):
        return None
    value = segment.split("=", 1)[1].strip()
    return value or None


def _prune_gold_versions(
    *,
    gold_root: Path,
    dataset_id: str,
    exchange: str,
    symbol: str,
    keep_last_versions: int,
) -> None:
    if keep_last_versions < 1:
        raise ValueError(f"keep_last_versions must be >= 1; received {keep_last_versions}")
    dataset_base = gold_root / f"dataset_id={dataset_id}" / "dataset_type=gold_symbol_dataset"
    symbol_dirs = list(dataset_base.glob(f"feature_set_version=*/exchange={exchange}/symbol={symbol}"))
    if len(symbol_dirs) <= keep_last_versions:
        return

    version_dirs: list[Path] = []
    for symbol_dir in symbol_dirs:
        version_dir = symbol_dir.parent.parent
        if version_dir.exists():
            version_dirs.append(version_dir)
    version_dirs = sorted(set(version_dirs))
    if len(version_dirs) <= keep_last_versions:
        return

    def _sort_key(path: Path) -> tuple[int, tuple[int, int, int], float, str]:
        parsed_version = _extract_feature_set_version(path)
        if parsed_version is not None:
            try:
                semver = _parse_semver(parsed_version)
                return (1, semver, path.stat().st_mtime, path.name)
            except ValueError:
                pass
        return (0, (0, 0, 0), path.stat().st_mtime, path.name)

    version_dirs_sorted = sorted(version_dirs, key=_sort_key, reverse=True)
    for old_dir in version_dirs_sorted[keep_last_versions:]:
        shutil.rmtree(old_dir, ignore_errors=False)


def _prune_gold_artifacts(
    *,
    gold_root: Path,
    dataset_id: str,
    exchange: str,
    symbol: str,
    keep_last_versions: int,
) -> None:
    """Keep only latest N gold artifact stems per dataset/exchange/symbol lineage."""

    if keep_last_versions < 1:
        raise ValueError(f"keep_last_versions must be >= 1; received {keep_last_versions}")

    dataset_base = gold_root / f"dataset_id={dataset_id}" / "dataset_type=gold_symbol_dataset"
    symbol_dirs = list(dataset_base.glob(f"feature_set_version=*/exchange={exchange}/symbol={symbol}"))
    if not symbol_dirs:
        return

    grouped: dict[Path, list[Path]] = {}
    for symbol_dir in symbol_dirs:
        for artifact in symbol_dir.glob("*"):
            if not artifact.is_file():
                continue
            grouped.setdefault(artifact.with_suffix(""), []).append(artifact)
    if len(grouped) <= keep_last_versions:
        return

    def _group_sort_key(stem_path: Path, files: list[Path]) -> tuple[int, tuple[int, int, int], float, str]:
        version_dir = stem_path.parent.parent.parent
        parsed_version = _extract_feature_set_version(version_dir)
        mtime = max(path.stat().st_mtime for path in files)
        if parsed_version is not None:
            try:
                semver = _parse_semver(parsed_version)
                return (1, semver, mtime, str(stem_path))
            except ValueError:
                pass
        return (0, (0, 0, 0), mtime, str(stem_path))

    sorted_groups = sorted(grouped.items(), key=lambda item: _group_sort_key(item[0], item[1]), reverse=True)
    for _, files in sorted_groups[keep_last_versions:]:
        for path in files:
            path.unlink(missing_ok=True)


def _contract_bump_level(
    previous: Mapping[str, object],
    current_contract: Mapping[str, object],
    *,
    previous_source_data_hash: str,
    current_source_data_hash: str,
) -> tuple[str, str]:
    prev_contract = previous.get("contract_signature")
    if not isinstance(prev_contract, dict):
        # Backward-compatible fallback for older manifests.
        source_silver_datasets = previous.get("source_silver_datasets")
        source_dataset_keys = sorted(source_silver_datasets.keys()) if isinstance(source_silver_datasets, dict) else []
        prev_contract = {
            "columns": previous.get("columns"),
            "join_policy": "full_outer_coalesce",
            "source_dataset_keys": source_dataset_keys,
        }
    prev_contract_map = dict(prev_contract)

    prev_columns = prev_contract_map.get("columns")
    curr_columns = current_contract.get("columns")
    if not isinstance(prev_columns, list) or not isinstance(curr_columns, list):
        return "major", "invalid_contract_signature"

    prev_join = prev_contract_map.get("join_policy")
    curr_join = current_contract.get("join_policy")
    if prev_join != curr_join:
        return "major", "join_policy_changed"

    prev_keys = prev_contract_map.get("source_dataset_keys")
    curr_keys = current_contract.get("source_dataset_keys")
    if not isinstance(prev_keys, list) or not isinstance(curr_keys, list):
        return "major", "invalid_source_dataset_keys"
    prev_set = set(str(item) for item in prev_keys)
    curr_set = set(str(item) for item in curr_keys)
    if not prev_set.issubset(curr_set):
        return "major", "source_dataset_removed"
    if curr_set != prev_set:
        return "minor", "source_dataset_added"

    prev_set_cols = set(str(item) for item in prev_columns)
    curr_set_cols = set(str(item) for item in curr_columns)
    if not prev_set_cols.issubset(curr_set_cols):
        return "major", "column_removed_or_renamed"
    if curr_set_cols != prev_set_cols:
        return "minor", "column_added"
    if [str(item) for item in prev_columns] != [str(item) for item in curr_columns]:
        return "major", "column_order_changed"

    if previous_source_data_hash != current_source_data_hash:
        return "patch", "source_data_changed"
    return "none", "no_change"


def normalize_symbol(value: str) -> str:
    """Normalize to canonical base asset symbol used across the repo (e.g. BTC, ETH, SOL)."""

    raw = value.strip().upper()
    normalized = raw.replace("_", "-").replace("/", "-")
    parts = [part for part in normalized.split("-") if part]
    if parts:
        return parts[0]
    for candidate in ("BTC", "ETH", "SOL"):
        if raw.startswith(candidate):
            return candidate
    return raw


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

    root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
    symbols: set[str] = set()
    if not root.exists():
        return symbols
    for path in root.glob("symbol=*/timeframe=*"):
        if path.name != f"timeframe={timeframe}":
            continue
        parent = path.parent.name
        if not parent.startswith("symbol="):
            continue
        symbols.add(normalize_symbol(parent.split("=", 1)[1]))
    return symbols


def _dataset_requirements(dataset_id: str) -> list[tuple[str, str]]:
    spec = GOLD_DATASET_SPECS.get(dataset_id)
    if spec is None:
        raise ValueError(f"Unsupported dataset_id: {dataset_id}")
    requirements = spec.get("requirements")
    if not isinstance(requirements, list):
        raise ValueError(f"Invalid dataset requirements for dataset_id: {dataset_id}")
    parsed: list[tuple[str, str]] = []
    for item in requirements:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError(f"Invalid dataset requirements for dataset_id: {dataset_id}")
        parsed.append((str(item[0]), str(item[1])))
    return parsed


def _dataset_includes_l2(dataset_id: str) -> bool:
    spec = GOLD_DATASET_SPECS.get(dataset_id)
    if spec is None:
        raise ValueError(f"Unsupported dataset_id: {dataset_id}")
    return bool(spec.get("include_l2", False))


def _read_latest_l2_gold_frame(*, l2_root: str, exchange: str, symbol: str) -> tuple[Any, Path]:
    pl = _require_polars()
    root = Path(l2_root)
    # Support either a direct L2 artifact root or a root containing the previous dataset folder.
    nested = root / "dataset_id=gold.l2.micro.m1"
    if nested.exists():
        root = nested
    candidates: list[Path] = []
    # Preferred nested layout.
    for path in root.glob("exchange=*/symbol=*/version=*/build_id=*/data.parquet"):
        exchange_segment = next((part for part in path.parts if part.startswith("exchange=")), None)
        if exchange_segment is None:
            continue
        raw_exchange = exchange_segment.split("=", 1)[1]
        if raw_exchange != exchange:
            continue
        symbol_segment = next((part for part in path.parts if part.startswith("symbol=")), None)
        if symbol_segment is None:
            continue
        raw_symbol = symbol_segment.split("=", 1)[1]
        if normalize_symbol(raw_symbol) != normalize_symbol(symbol):
            continue
        candidates.append(path)
    # Backward-compatible flat layout: <SYMBOL>_L2_<hash>_<hash>.parquet
    if not candidates:
        for path in root.glob("**/*_L2_*.parquet"):
            base = path.name.split("_L2_", 1)[0]
            if normalize_symbol(base) != normalize_symbol(symbol):
                continue
            candidates.append(path)
    candidates = sorted(candidates, key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise ValueError(f"Missing L2 parquet for symbol={symbol} under l2_root={l2_root}")
    chosen = candidates[-1]
    return pl.read_parquet(str(chosen)), chosen


def _prepare_l2(pl: Any, frame: Any, symbol: str) -> Any:
    key_cols = {"ts_minute", "exchange", "symbol"}
    if "ts_minute" not in frame.columns:
        raise ValueError("L2 parquet missing required column 'ts_minute'")
    if "exchange" not in frame.columns:
        frame = frame.with_columns(pl.lit("deribit").alias("exchange"))
    if "symbol" not in frame.columns:
        frame = frame.with_columns(pl.lit(symbol).alias("symbol"))
    renamed = []
    for col in frame.columns:
        if col in key_cols:
            continue
        renamed.append(pl.col(col).alias(f"l2_{col}"))
    return (
        frame.with_columns(
            [
                pl.col("ts_minute")
                .cast(pl.Datetime(time_unit="us", time_zone="UTC"))
                .dt.truncate("1m")
                .alias("timestamp_m1"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(["timestamp_m1", "exchange", "symbol", *renamed])
        .sort("timestamp_m1")
    )


def _l2_invalid_mask_expr(pl: Any, columns: set[str]) -> Any:
    cond = pl.lit(False)
    if "l2_coverage_ratio" in columns:
        cond = cond | (pl.col("l2_coverage_ratio") < 0.0) | (pl.col("l2_coverage_ratio") > 1.0)
    if "l2_snapshot_count" in columns:
        cond = cond | (pl.col("l2_snapshot_count") < 0)
    if "l2_first_snapshot_ts" in columns and "l2_last_snapshot_ts" in columns:
        cond = cond | (pl.col("l2_first_snapshot_ts") > pl.col("l2_last_snapshot_ts"))
    return cond


def _validate_or_filter_l2_quality(pl: Any, frame: Any, mode: str) -> tuple[Any, dict[str, int]]:
    if mode not in {"strict", "lenient"}:
        raise ValueError(f"Unsupported l2_validation_mode: {mode}")
    l2_columns = set(frame.columns)
    if "l2_coverage_ratio" not in l2_columns and "l2_snapshot_count" not in l2_columns:
        raise ValueError("L2 validation failed: no supported L2 quality columns present")
    invalid_mask = _l2_invalid_mask_expr(pl, l2_columns)
    invalid_rows = frame.filter(invalid_mask).height
    if invalid_rows == 0:
        return frame, {"l2_invalid_rows_found": 0, "l2_invalid_rows_dropped": 0}
    if mode == "strict":
        raise ValueError(f"L2 validation failed: {invalid_rows} invalid rows detected")
    filtered = frame.filter(~invalid_mask)
    dropped = frame.height - filtered.height
    return filtered, {"l2_invalid_rows_found": invalid_rows, "l2_invalid_rows_dropped": dropped}


def _read_dataset_frame(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    dataset_type: str,
    timeframe: str,
) -> Any:
    pl = _require_polars()
    dataset_root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
    candidate_files: list[Path] = []
    symbol_dirs = sorted(dataset_root.glob(f"symbol=*/timeframe={timeframe}"))
    for sym_dir in symbol_dirs:
        sym_segment = sym_dir.parent.name
        if not sym_segment.startswith("symbol="):
            continue
        raw_symbol = sym_segment.split("=", 1)[1]
        if normalize_symbol(raw_symbol) != symbol:
            continue
        candidate_files.extend(path for path in sorted(sym_dir.glob("**/*.parquet")) if path.is_file())
    if not candidate_files:
        raise ValueError(f"Missing silver dataset for symbol={symbol}: {dataset_type}")
    selected_file = max(candidate_files, key=lambda path: (path.stat().st_mtime, str(path)))
    frame = pl.read_parquet(str(selected_file))
    return frame


def _prepare_spot_or_perp(pl: Any, frame: Any, prefix: str, symbol: str) -> Any:
    return (
        frame.with_columns(
            [
                pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp_m1"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("open_price").cast(pl.Float64).alias(f"{prefix}_open_price"),
                pl.col("high_price").cast(pl.Float64).alias(f"{prefix}_high_price"),
                pl.col("low_price").cast(pl.Float64).alias(f"{prefix}_low_price"),
                pl.col("close_price").cast(pl.Float64).alias(f"{prefix}_close_price"),
                pl.col("volume").cast(pl.Float64).alias(f"{prefix}_volume"),
            ]
        )
        .sort("timestamp_m1")
    )


def _prepare_oi(pl: Any, frame: Any, symbol: str) -> Any:
    return (
        frame.with_columns(
            [
                pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("open_interest").cast(pl.Float64).alias("oi_open_interest"),
                pl.col("oi_is_observed").cast(pl.Boolean),
                pl.col("oi_is_ffill").cast(pl.Boolean),
                pl.col("minutes_since_oi_observation").cast(pl.Int64),
                pl.col("oi_observation_lag_sec").cast(pl.Int64),
            ]
        )
        .sort("timestamp_m1")
    )


def _prepare_funding(pl: Any, frame: Any, symbol: str) -> Any:
    return (
        frame.with_columns(
            [
                pl.col("timestamp").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp_m1"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("funding_rate_last_known").cast(pl.Float64),
                pl.col("minutes_since_funding").cast(pl.Int64),
                pl.col("is_funding_observation_minute").cast(pl.Boolean),
                pl.col("funding_data_available").cast(pl.Boolean),
            ]
        )
        .sort("timestamp_m1")
    )


def _prepare_trades(pl: Any, frame: Any, symbol: str) -> Any:
    return (
        frame.with_columns(
            [
                pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("open_price").cast(pl.Float64).alias("trades_open_price"),
                pl.col("high_price").cast(pl.Float64).alias("trades_high_price"),
                pl.col("low_price").cast(pl.Float64).alias("trades_low_price"),
                pl.col("close_price").cast(pl.Float64).alias("trades_close_price"),
                pl.col("volume").cast(pl.Float64).alias("trades_volume"),
                pl.col("quote_volume").cast(pl.Float64).alias("trades_quote_volume"),
                pl.col("trade_count").cast(pl.Int64).alias("trades_trade_count"),
                pl.col("buy_volume").cast(pl.Float64).alias("trades_buy_volume"),
                pl.col("sell_volume").cast(pl.Float64).alias("trades_sell_volume"),
                pl.col("buy_trade_count").cast(pl.Int64).alias("trades_buy_trade_count"),
                pl.col("sell_trade_count").cast(pl.Int64).alias("trades_sell_trade_count"),
                pl.col("buy_volume_share").cast(pl.Float64).alias("trades_buy_volume_share"),
            ]
        )
        .sort("timestamp_m1")
    )


def _prepare_option_trades(pl: Any, frame: Any, symbol: str) -> Any:
    return (
        frame.with_columns(
            [
                pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("open_price").cast(pl.Float64).alias("option_trades_open_price"),
                pl.col("high_price").cast(pl.Float64).alias("option_trades_high_price"),
                pl.col("low_price").cast(pl.Float64).alias("option_trades_low_price"),
                pl.col("close_price").cast(pl.Float64).alias("option_trades_close_price"),
                pl.col("volume").cast(pl.Float64).alias("option_trades_volume"),
                pl.col("quote_volume").cast(pl.Float64).alias("option_trades_quote_volume"),
                pl.col("trade_count").cast(pl.Int64).alias("option_trades_trade_count"),
                pl.col("buy_volume").cast(pl.Float64).alias("option_trades_buy_volume"),
                pl.col("sell_volume").cast(pl.Float64).alias("option_trades_sell_volume"),
                pl.col("buy_trade_count").cast(pl.Int64).alias("option_trades_buy_trade_count"),
                pl.col("sell_trade_count").cast(pl.Int64).alias("option_trades_sell_trade_count"),
                pl.col("buy_volume_share").cast(pl.Float64).alias("option_trades_buy_volume_share"),
            ]
        )
        .sort("timestamp_m1")
    )


def _prepare_volatility_index_data(pl: Any, frame: Any, symbol: str) -> Any:
    return (
        frame.with_columns(
            [
                pl.col("timestamp").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp_m1"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("volatility_value").cast(pl.Float64).alias("volatility_index_value"),
            ]
        )
        .sort("timestamp_m1")
    )


def _prepare_dataset_frame(pl: Any, dataset_type: str, frame: Any, symbol: str) -> Any:
    dataset_preparers: dict[str, Any] = {
        "spot": lambda: _prepare_spot_or_perp(pl, frame, "spot", symbol),
        "perp": lambda: _prepare_spot_or_perp(pl, frame, "perp", symbol),
        "oi_1m_feature": lambda: _prepare_oi(pl, frame, symbol),
        "funding_1m_feature": lambda: _prepare_funding(pl, frame, symbol),
        "perp_trades_1m_feature": lambda: _prepare_trades(pl, frame, symbol),
        "option_trades_1m_feature": lambda: _prepare_option_trades(pl, frame, symbol),
        "volatility_index_data_observed": lambda: _prepare_volatility_index_data(pl, frame, symbol),
        "gold_l2_m1": lambda: _prepare_l2(pl, frame, symbol),
    }
    preparer = dataset_preparers.get(dataset_type)
    if preparer is None:
        raise ValueError(f"Unsupported dataset_type for preparation: {dataset_type}")
    return preparer()


def _build_minute_grid(pl: Any, prepared: list[Any], exchange: str, symbol: str) -> Any:
    mins: list[datetime] = []
    maxs: list[datetime] = []
    for frame in prepared:
        if frame.height == 0:
            continue
        min_ts = frame.select(pl.col("timestamp_m1").min()).item()
        max_ts = frame.select(pl.col("timestamp_m1").max()).item()
        if isinstance(min_ts, datetime) and isinstance(max_ts, datetime):
            mins.append(min_ts)
            maxs.append(max_ts)
    if not mins or not maxs:
        raise ValueError("No timestamp coverage available across prepared datasets")
    start = min(mins)
    end = max(maxs)
    timestamp_grid = pl.datetime_range(start, end, interval="1m", eager=True).alias("timestamp_m1")
    return pl.DataFrame({"timestamp_m1": timestamp_grid}).with_columns(
        [pl.lit(exchange).alias("exchange"), pl.lit(symbol).alias("symbol")]
    )


def _json_payload_hash(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]


def _feature_source_dataset(column_name: str) -> str:
    return feature_profile.feature_source_dataset(column_name)


def _time_span_coverage(frame: Any) -> tuple[datetime | None, datetime | None, int | None, int | None, float | None]:
    pl = _require_polars()
    min_ts = frame.select(pl.col("timestamp_m1").min()).item()
    max_ts = frame.select(pl.col("timestamp_m1").max()).item()
    expected_minutes: int | None = None
    missing_minutes: int | None = None
    observed_coverage_ratio: float | None = None
    if isinstance(min_ts, datetime) and isinstance(max_ts, datetime):
        expected_minutes = int(((max_ts - min_ts).total_seconds() // 60) + 1)
        if expected_minutes > 0:
            observed_coverage_ratio = frame.height / float(expected_minutes)
            missing_minutes = max(expected_minutes - frame.height, 0)
    return min_ts, max_ts, expected_minutes, missing_minutes, observed_coverage_ratio


def _source_dataset_summary(
    pl: Any, raw_by_dataset: dict[str, Any], l2_source_path: Path | None
) -> dict[str, dict[str, object]]:
    summary: dict[str, dict[str, object]] = {}
    for dataset_type, raw in raw_by_dataset.items():
        source_key = f"{dataset_type}_1m" if dataset_type in {"spot", "perp"} else dataset_type
        source_symbols = (
            sorted(set(raw.get_column("symbol").cast(pl.Utf8).to_list())) if "symbol" in raw.columns else []
        )
        summary[source_key] = {
            "columns": raw.columns,
            "rows": raw.height,
            "source_symbols": source_symbols,
        }
        if dataset_type == "gold_l2_m1" and l2_source_path is not None:
            summary[source_key]["source_artifact"] = l2_source_path.name
    return summary


def _missing_value_audit(pl: Any, frame: Any) -> tuple[dict[str, int], int]:
    missing_by_column = {col: int(frame.select(pl.col(col).is_null().sum()).item()) for col in frame.columns}
    missing_total = int(sum(missing_by_column.values()))
    return missing_by_column, missing_total


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
