"""Run Bronze, Silver, and Gold builders sequentially for cron automation."""

from __future__ import annotations

import argparse
import calendar
import fcntl
import os
import re
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


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
            cli_args = _enforce_six_month_download_window(cli_args)

        cmd = [str(main_path), "--config", str(config_path), command, *cli_args]
        steps.append(PipelineStep(name=layer_name, args=cmd))
    return steps


def _six_months_ago_utc_date(today_utc: datetime) -> str:
    """Return YYYY-MM-DD for six calendar months before ``today_utc``."""

    year = today_utc.year
    month = today_utc.month
    day = today_utc.day
    target_month_index = month - 6
    prev_year = year
    while target_month_index <= 0:
        target_month_index += 12
        prev_year -= 1
    prev_month = target_month_index
    prev_month_last_day = calendar.monthrange(prev_year, prev_month)[1]
    prev_day = min(day, prev_month_last_day)
    return datetime(prev_year, prev_month, prev_day, tzinfo=UTC).strftime("%Y-%m-%d")


def _clamp_date_lower_bound(value: str, lower_bound: str) -> str:
    """Clamp YYYY-MM-DD value to be no earlier than ``lower_bound``."""

    return lower_bound if value < lower_bound else value


def _clamp_symbol_date_entries(entries: list[str], *, lower_bound: str) -> list[str]:
    """Clamp ``SYMBOL=YYYY-MM-DD`` or ``EXCHANGE:SYMBOL=YYYY-MM-DD`` tokens."""

    clamped: list[str] = []
    for item in entries:
        if "=" not in item:
            clamped.append(item)
            continue
        key, date_part = item.split("=", 1)
        clamped.append(f"{key}={_clamp_date_lower_bound(date_part, lower_bound)}")
    return clamped


def _enforce_six_month_download_window(cli_args: list[str]) -> list[str]:
    """Ensure Bronze CLI date boundaries do not request more than six months of history."""

    lower_bound = _six_months_ago_utc_date(datetime.now(UTC))
    rewritten: list[str] = []
    i = 0
    has_start_date = False

    while i < len(cli_args):
        token = cli_args[i]
        if token == "--start-date":
            has_start_date = True
            rewritten.append(token)
            if i + 1 < len(cli_args) and not cli_args[i + 1].startswith("--"):
                rewritten.append(_clamp_date_lower_bound(cli_args[i + 1], lower_bound))
                i += 2
            else:
                rewritten.append(lower_bound)
                i += 1
            continue
        if token in {"--symbol-start-dates", "--exchange-symbol-start-dates"}:
            rewritten.append(token)
            i += 1
            values: list[str] = []
            while i < len(cli_args) and not cli_args[i].startswith("--"):
                values.append(cli_args[i])
                i += 1
            rewritten.extend(_clamp_symbol_date_entries(values, lower_bound=lower_bound))
            continue
        rewritten.append(token)
        i += 1

    if not has_start_date:
        rewritten.extend(["--start-date", lower_bound])
    return rewritten


def _log_path_from_config(*, config_data: dict[str, Any], repo_root: Path) -> Path:
    """Resolve shared log file path from config env mapping."""

    env_cfg = config_data.get("env")
    if isinstance(env_cfg, dict):
        configured_file = env_cfg.get("DEPTH_SYNC_LOG_FILE")
        if isinstance(configured_file, str) and configured_file.strip():
            return Path(configured_file.strip()).resolve()
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


def _rotate_pipeline_log(log_path: Path, *, retention_days: int = 30) -> None:
    """Rotate shared pipeline log by day and delete rotations older than retention."""

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

    cutoff = datetime.now(UTC).date().toordinal() - retention_days
    pattern = re.compile(rf"^{re.escape(log_path.name)}\.(\d{{4}}-\d{{2}}-\d{{2}})(?:\.\d{{6}})?$")
    for candidate in log_path.parent.iterdir():
        match = pattern.match(candidate.name)
        if not match:
            continue
        try:
            candidate_date = datetime.fromisoformat(match.group(1)).date()
        except ValueError:
            continue
        if candidate_date.toordinal() <= cutoff:
            candidate.unlink(missing_ok=True)


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
    _rotate_pipeline_log(log_path, retention_days=30)

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
