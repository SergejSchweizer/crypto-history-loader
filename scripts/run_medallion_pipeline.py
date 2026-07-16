"""Run Bronze, Silver, and Gold builders sequentially for cron automation."""

from __future__ import annotations

import argparse
import fcntl
import os
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from application.services.runtime_service import LOG_ARCHIVE_RETENTION_DAYS, enforce_log_retention


@dataclass(frozen=True)
class PipelineStep:
    """One CLI command step in the medallion pipeline."""

    name: str
    args: list[str]


def _utc_ts() -> str:
    """Return current UTC timestamp in ISO-like format."""

    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log_line(message: str) -> None:
    """Write one timestamped log line."""

    print(f"[{_utc_ts()}] {message}", flush=True)


def _default_repo_root() -> Path:
    """Return repository root inferred from this script location."""

    return Path(__file__).resolve().parents[1]


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML mapping from config path."""

    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required to load pipeline config.") from exc

    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError("config file must contain a top-level mapping")
    return loaded


def _build_steps(*, main_path: Path, config_path: Path, config_data: dict[str, Any]) -> list[PipelineStep]:
    """Build pipeline command sequence from config."""

    pipeline_cfg = config_data.get("medallion-pipeline")
    if not isinstance(pipeline_cfg, dict):
        raise ValueError("config missing required section: medallion-pipeline")

    order_raw = pipeline_cfg.get("execution_order")
    if not isinstance(order_raw, list) or not order_raw:
        raise ValueError("medallion-pipeline.execution_order must be a non-empty list")
    execution_order = [str(name).strip() for name in order_raw if str(name).strip()]
    valid_layers = {"bronze", "silver", "gold"}
    invalid = [name for name in execution_order if name not in valid_layers]
    if invalid:
        raise ValueError(
            f"Unsupported pipeline layer(s) in execution_order: {invalid}. Allowed: {sorted(valid_layers)}"
        )

    steps: list[PipelineStep] = []
    for layer_name in execution_order:
        layer_cfg = pipeline_cfg.get(layer_name)
        if not isinstance(layer_cfg, dict):
            raise ValueError(f"medallion-pipeline.{layer_name} must be a mapping")
        enabled = bool(layer_cfg.get("enabled", True))
        if not enabled:
            continue

        command = str(layer_cfg.get("command", "")).strip()
        if not command:
            raise ValueError(f"medallion-pipeline.{layer_name}.command is required")

        cli_args_raw = layer_cfg.get("cli_args", [])
        if not isinstance(cli_args_raw, list):
            raise ValueError(f"medallion-pipeline.{layer_name}.cli_args must be a list")
        cli_args = [str(token) for token in cli_args_raw]
        if layer_name == "bronze" and command == "bronze-build":
            cli_args = _apply_bronze_start_defaults(cli_args=cli_args, config_data=config_data)
            cli_args = _ensure_volatility_dataset_arg(cli_args)
        if layer_name == "silver" and command == "silver-build":
            cli_args = _ensure_volatility_dataset_arg(cli_args)

        cmd = [str(main_path), "--config", str(config_path), command, *cli_args]
        steps.append(PipelineStep(name=layer_name, args=cmd))
    return steps


def _has_option(cli_args: list[str], option_name: str) -> bool:
    """Return whether a CLI option is already present."""

    return option_name in cli_args


def _ensure_volatility_dataset_arg(cli_args: list[str]) -> list[str]:
    """Include volatility index data in Bronze medallion dataset schedules."""

    if "--dataset" not in cli_args or "volatility_index_data" in cli_args:
        return cli_args
    rewritten = list(cli_args)
    dataset_idx = rewritten.index("--dataset")
    insert_idx = dataset_idx + 1
    while insert_idx < len(rewritten) and not rewritten[insert_idx].startswith("--"):
        insert_idx += 1
    rewritten.insert(insert_idx, "volatility_index_data")
    return rewritten


def _string_list(value: object) -> list[str]:
    """Return a clean string list from config values."""

    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _apply_bronze_start_defaults(*, cli_args: list[str], config_data: dict[str, Any]) -> list[str]:
    """Append Bronze symbol start-bound defaults from ``bronze-build``.

    Medallion runs should use the same symbol-specific historical boundaries
    as direct ``bronze-build`` runs unless the step explicitly overrides them.
    """

    bronze_cfg = config_data.get("bronze-build")
    if not isinstance(bronze_cfg, dict):
        return cli_args

    rewritten = list(cli_args)
    symbol_start_dates = _string_list(bronze_cfg.get("symbol_start_dates"))
    if symbol_start_dates and not _has_option(rewritten, "--symbol-start-dates"):
        rewritten.append("--symbol-start-dates")
        rewritten.extend(symbol_start_dates)

    return rewritten


def _log_path_from_config(*, config_data: dict[str, Any], repo_root: Path) -> Path:
    """Resolve shared log file path from config env mapping."""

    env_cfg = config_data.get("env")
    if isinstance(env_cfg, dict):
        configured_file = env_cfg.get("DEPTH_SYNC_LOG_FILE")
        if isinstance(configured_file, str) and configured_file.strip():
            return (Path(configured_file.strip()).parent / "run-medallion-pipeline.log").resolve()
        configured_dir = env_cfg.get("DEPTH_SYNC_LOG_DIR")
        if isinstance(configured_dir, str) and configured_dir.strip():
            return (Path(configured_dir.strip()) / "crypto-history-loader.log").resolve()
    return (repo_root / ".run" / "logs" / "crypto-history-loader.log").resolve()


def _run_pipeline(
    *,
    python_bin: str,
    steps: list[PipelineStep],
    repo_root: Path,
    env: dict[str, str],
) -> None:
    """Run all steps sequentially, stopping on first failure."""

    for step in steps:
        _log_line(f"ACTIVE_STEP={step.name}")
        _log_line(f"START {step.name}")
        subprocess.run(
            [python_bin, *step.args],
            cwd=str(repo_root),
            env=env,
            check=True,
        )
        _log_line(f"DONE {step.name}")


@contextmanager
def _redirect_output_to(log_path: Path):
    """Redirect stdout/stderr to log file for the pipeline scope."""

    with log_path.open("a", encoding="utf-8") as log_handle:
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        try:
            sys.stdout = log_handle
            sys.stderr = log_handle
            yield
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr


def _acquire_nonblocking_lock(lock_file: Path) -> int | None:
    """Acquire non-blocking lock. Returns fd on success, ``None`` when locked."""

    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_file, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        return None


def _release_lock(fd: int) -> None:
    """Release file lock and close descriptor."""

    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


def _rotate_pipeline_log(log_path: Path, *, retention_days: int = LOG_ARCHIVE_RETENTION_DAYS) -> None:
    """Rotate shared pipeline log by day and apply repository log retention."""

    if not log_path.exists():
        return
    today = datetime.now(UTC).date()
    modified_date = datetime.fromtimestamp(log_path.stat().st_mtime, tz=UTC).date()
    if modified_date < today:
        rotated_path = log_path.with_name(f"{log_path.name}.{modified_date.isoformat()}")
        if rotated_path.exists():
            suffix = datetime.now(UTC).strftime("%H%M%S")
            rotated_path = log_path.with_name(f"{log_path.name}.{modified_date.isoformat()}.{suffix}")
        log_path.rename(rotated_path)

    enforce_log_retention(log_path, archive_retention_days=retention_days)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""

    repo_root = _default_repo_root()
    default_lock_file = repo_root / ".run" / "full-pipeline.lock"

    parser = argparse.ArgumentParser(description="Run bronze, silver, and gold builders as one pipeline.")
    parser.add_argument("--repo-root", default=str(repo_root), help="Repository root path")
    parser.add_argument("--config", default=str(repo_root / "config.yaml"), help="Path to config.yaml")
    parser.add_argument("--python-bin", default=sys.executable, help="Python executable used for builder commands")
    parser.add_argument("--main-path", default=str(repo_root / "main.py"), help="Path to main.py entrypoint")
    parser.add_argument("--lock-file", default=str(default_lock_file), help="Non-blocking lock file path")
    parser.add_argument("--log-file", help="Single append-only pipeline log file (overrides config)")
    return parser.parse_args()


def main() -> int:
    """Entrypoint for cron-friendly medallion pipeline execution."""

    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    config_path = Path(args.config).resolve()
    main_path = Path(args.main_path).resolve()
    lock_file = Path(args.lock_file).resolve()

    if not config_path.exists():
        print(f"missing required config file: {config_path}", file=sys.stderr)
        return 2
    if not main_path.exists():
        print(f"missing main entrypoint: {main_path}", file=sys.stderr)
        return 2

    config_data = _load_yaml(config_path)
    config_log_path = _log_path_from_config(config_data=config_data, repo_root=repo_root)
    log_path = Path(args.log_file).resolve() if args.log_file else config_log_path

    log_path.parent.mkdir(parents=True, exist_ok=True)
    _rotate_pipeline_log(log_path)

    lock_fd = _acquire_nonblocking_lock(lock_file)
    if lock_fd is None:
        print("pipeline already running", file=sys.stderr)
        return 1
    try:
        with _redirect_output_to(log_path):
            _log_line("PIPELINE START script=run_medallion_pipeline.py")
            env = dict(os.environ)
            steps = _build_steps(main_path=main_path, config_path=config_path, config_data=config_data)
            _log_line(f"SCHEDULED_STEPS={','.join(step.name for step in steps)}")
            _run_pipeline(
                python_bin=str(args.python_bin),
                steps=steps,
                repo_root=repo_root,
                env=env,
            )
            _log_line("PIPELINE DONE")
    finally:
        _release_lock(lock_fd)

    print(f"pipeline completed: log={log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
