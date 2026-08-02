"""Gold dataset versioning and artifact-retention helpers."""

from __future__ import annotations

import json
import re
import shutil
from collections.abc import Mapping
from pathlib import Path

_SEMVER_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


def parse_semver(version: str) -> tuple[int, int, int]:
    """Parse a Gold dataset semantic version in `vMAJOR.MINOR.PATCH` format."""

    match = _SEMVER_RE.fullmatch(version)
    if match is None:
        raise ValueError(f"Invalid semantic version '{version}'. Expected format like v1.0.0")
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def format_semver(major: int, minor: int, patch: int) -> str:
    """Format numeric semantic-version parts as a Gold dataset version string."""

    return f"v{major}.{minor}.{patch}"


def bump_semver(version: str, level: str) -> str:
    """Return the next semantic version for a Gold contract bump level."""

    major, minor, patch = parse_semver(version)
    if level == "major":
        return format_semver(major + 1, 0, 0)
    if level == "minor":
        return format_semver(major, minor + 1, 0)
    if level == "patch":
        return format_semver(major, minor, patch + 1)
    if level == "none":
        return version
    raise ValueError(f"Unsupported semver bump level: {level}")


def latest_manifest_for_dataset(
    gold_root: Path, exchange: str, symbol: str, dataset_id: str
) -> dict[str, object] | None:
    """Return the newest manifest payload for one Gold dataset lineage."""

    dataset_base = gold_root / f"dataset_id={dataset_id}"
    if not dataset_base.exists():
        return None
    legacy_symbol_root = dataset_base / f"exchange={exchange}" / f"symbol={symbol}"
    latest_payload: dict[str, object] | None = None
    latest_mtime = -1.0
    candidate_paths = list(dataset_base.glob(f"exchange={exchange}/symbol={symbol}/version=*/build_id=*/manifest.json"))
    candidate_paths.extend(
        dataset_base.glob(
            f"dataset_type=gold_symbol_dataset/feature_set_version=*/exchange={exchange}/symbol={symbol}/*.json"
        )
    )
    candidate_paths.extend(
        legacy_symbol_root.glob("dataset_type=gold_symbol_dataset/feature_set_version=*/exchange=*/symbol=*/*.json")
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


def extract_feature_set_version(version_dir: Path) -> str | None:
    """Extract a semantic version from a `feature_set_version=...` artifact directory."""

    segment = version_dir.name
    if not segment.startswith("feature_set_version="):
        return None
    value = segment.split("=", 1)[1].strip()
    return value or None


def prune_gold_versions(
    *,
    gold_root: Path,
    dataset_id: str,
    exchange: str,
    symbol: str,
    keep_last_versions: int,
) -> None:
    """Remove old Gold feature-set version directories for one dataset."""

    if keep_last_versions < 1:
        raise ValueError(f"keep_last_versions must be >= 1; received {keep_last_versions}")
    dataset_base = gold_root / f"dataset_id={dataset_id}" / "dataset_type=gold_symbol_dataset"
    _ = exchange
    _ = symbol
    version_dirs = [path for path in dataset_base.glob("feature_set_version=*") if path.is_dir()]
    if len(version_dirs) <= keep_last_versions:
        return

    def _sort_key(path: Path) -> tuple[int, tuple[int, int, int], float, str]:
        parsed_version = extract_feature_set_version(path)
        if parsed_version is not None:
            try:
                semver = parse_semver(parsed_version)
                return (1, semver, path.stat().st_mtime, path.name)
            except ValueError:
                pass
        return (0, (0, 0, 0), path.stat().st_mtime, path.name)

    version_dirs_sorted = sorted(version_dirs, key=_sort_key, reverse=True)
    for old_dir in version_dirs_sorted[keep_last_versions:]:
        shutil.rmtree(old_dir, ignore_errors=False)


def prune_gold_artifacts(
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
        parsed_version = extract_feature_set_version(version_dir)
        mtime = max(path.stat().st_mtime for path in files)
        if parsed_version is not None:
            try:
                semver = parse_semver(parsed_version)
                return (1, semver, mtime, str(stem_path))
            except ValueError:
                pass
        return (0, (0, 0, 0), mtime, str(stem_path))

    sorted_groups = sorted(grouped.items(), key=lambda item: _group_sort_key(item[0], item[1]), reverse=True)
    for _, files in sorted_groups[keep_last_versions:]:
        for path in files:
            path.unlink(missing_ok=True)


def contract_bump_level(
    previous: Mapping[str, object],
    current_contract: Mapping[str, object],
    *,
    previous_source_data_hash: str,
    current_source_data_hash: str,
) -> tuple[str, str]:
    """Classify the required Gold dataset version bump from contract and source changes."""

    prev_contract = previous.get("contract_signature")
    if not isinstance(prev_contract, dict):
        # Backward-compatible fallback for older manifests without an explicit contract signature.
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
