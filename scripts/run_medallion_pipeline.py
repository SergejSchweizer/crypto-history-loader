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
from typing import cast


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


def _load_yaml(path: Path) -> dict[str, object]:
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
    return cast(dict[str, object], loaded)


def _build_steps(*, main_path: Path, config_path: Path, config_data: dict[str, object]) -> list[PipelineStep]:
    """Build pipeline command sequence from config."""

    pipeline_cfg = config_data.get("medallion-pipeline")
    if not isinstance(pipeline_cfg, dict):
        raise ValueError("config missing required section: medallion-pipeline")
    pipeline_map = cast(dict[str, object], pipeline_cfg)

    order_raw = pipeline_map.get("execution_order")
    if not isinstance(order_raw, list) or not order_raw:
        raise ValueError("medallion-pipeline.execution_order must be a non-empty list")
    execution_order = [str(name).strip() for name in cast(list[object], order_raw) if str(name).strip()]
    valid_layers = {"bronze", "silver", "gold"}
    invalid = [name for name in execution_order if name not in valid_layers]
    if invalid:
        raise ValueError(
            f"Unsupported pipeline layer(s) in execution_order: {invalid}. Allowed: {sorted(valid_layers)}"
        )

    steps: list[PipelineStep] = []
    for layer_name in execution_order:
        layer_cfg = pipeline_map.get(layer_name)
        if not isinstance(layer_cfg, dict):
            raise ValueError(f"medallion-pipeline.{layer_name} must be a mapping")
        layer_map = cast(dict[str, object], layer_cfg)
        enabled = bool(layer_map.get("enabled", True))
        if not enabled:
            continue

        command = str(layer_map.get("command", "")).strip()
        if not command:
            raise ValueError(f"medallion-pipeline.{layer_name}.command is required")

        cli_args_raw = layer_map.get("cli_args", [])
        if not isinstance(cli_args_raw, list):
            raise ValueError(f"medallion-pipeline.{layer_name}.cli_args must be a list")
        cli_args = [str(token) for token in cast(list[object], cli_args_raw)]
        if layer_name == "bronze" and command == "bronze-build":
            cli_args = _ensure_bronze_market_datasets(cli_args)
            cli_args = _enforce_oldest_missing_start_window(cli_args)
            cli_args = _ensure_full_gap_fill(cli_args)

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


_DATE_TOKEN_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_date_token(value: str) -> bool:
    """Return whether token is an ISO UTC date (YYYY-MM-DD)."""

    return bool(_DATE_TOKEN_RE.fullmatch(value.strip()))


def _extract_oldest_start_date(cli_args: list[str]) -> str | None:
    """Return oldest date found in --start-date and symbol date override tokens."""

    dates: list[str] = []
    i = 0
    while i < len(cli_args):
        token = cli_args[i]
        if token == "--start-date":
            if i + 1 < len(cli_args) and not cli_args[i + 1].startswith("--") and _is_date_token(cli_args[i + 1]):
                dates.append(cli_args[i + 1])
                i += 2
                continue
        if token in {"--symbol-start-dates", "--exchange-symbol-start-dates"}:
            i += 1
            while i < len(cli_args) and not cli_args[i].startswith("--"):
                item = cli_args[i]
                if "=" in item:
                    maybe_date = item.split("=", 1)[1].strip()
                    if _is_date_token(maybe_date):
                        dates.append(maybe_date)
                i += 1
            continue
        i += 1
    if not dates:
        return None
    return min(dates)


def _enforce_oldest_missing_start_window(cli_args: list[str]) -> list[str]:
    """Set Bronze start-date to oldest configured missing date across symbols/datasets."""

    oldest = _extract_oldest_start_date(cli_args)
    if oldest is None:
        oldest = _six_months_ago_utc_date(datetime.now(UTC))

    rewritten: list[str] = []
    i = 0
    start_date_written = False

    while i < len(cli_args):
        token = cli_args[i]
        if token == "--start-date":
            if not start_date_written:
                rewritten.extend(["--start-date", oldest])
                start_date_written = True
            i += 1
            if i < len(cli_args) and not cli_args[i].startswith("--"):
                i += 1
            continue
        rewritten.append(token)
        i += 1

    if not start_date_written:
        rewritten.extend(["--start-date", oldest])
    return rewritten


def _ensure_full_gap_fill(cli_args: list[str]) -> list[str]:
    """Ensure Bronze runs in full-gap-fill mode for missing-history backfill."""

    if "--full-gap-fill" in cli_args:
        return cli_args
    rewritten = [token for token in cli_args if token != "--tail-delta-only"]
    rewritten.append("--full-gap-fill")
    return rewritten


def _ensure_bronze_market_datasets(
    cli_args: list[str],
    required_datasets: tuple[str, ...] = ("volatility_index_data",),
) -> list[str]:
    """Ensure Bronze CLI args include required ``--market`` dataset tokens."""

    rewritten: list[str] = []
    i = 0
    market_handled = False

    while i < len(cli_args):
        token = cli_args[i]
        if token != "--market":
            rewritten.append(token)
            i += 1
            continue

        market_handled = True
        rewritten.append(token)
        i += 1
        seen: set[str] = set()
        market_values: list[str] = []
        while i < len(cli_args) and not cli_args[i].startswith("--"):
            value = cli_args[i].strip()
            if value and value not in seen:
                seen.add(value)
                market_values.append(value)
            i += 1

        for dataset in required_datasets:
            if dataset not in seen:
                market_values.append(dataset)
        rewritten.extend(market_values)

    if not market_handled:
        rewritten.extend(["--market", *required_datasets])
    return rewritten


def _log_path_from_config(*, config_data: dict[str, object], repo_root: Path) -> Path:
    """Resolve module-specific log file path from config env mapping."""

    env_cfg = config_data.get("env")
    if isinstance(env_cfg, dict):
        env_map = cast(dict[str, object], env_cfg)
        configured_file = env_map.get("DEPTH_SYNC_LOG_FILE")
        if isinstance(configured_file, str) and configured_file.strip():
            return (Path(configured_file.strip()).resolve().parent / "run-medallion-pipeline.log").resolve()
        configured_dir = env_map.get("DEPTH_SYNC_LOG_DIR")
        if isinstance(configured_dir, str) and configured_dir.strip():
            return (Path(configured_dir.strip()) / "run-medallion-pipeline.log").resolve()
    return (repo_root / ".run" / "logs" / "run-medallion-pipeline.log").resolve()


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
