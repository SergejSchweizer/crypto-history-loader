"""Repository quality-gate alignment tests."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_GATE_COMMANDS = (
    "ruff check .",
    "ruff format --check .",
    "mypy .",
    "pyright --level error",
    "ty check",
    "lint-imports --config .importlinter",
    "python scripts/validate_config_with_pydantic.py --config config.yaml",
    "python scripts/validate_readme_inventory.py",
    "python scripts/validate_conventional_commit.py",
    "pytest",
)


def _load_yaml(path: Path) -> dict[str, Any]:
    yaml = pytest.importorskip("yaml")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _normalized_commands(values: list[str]) -> list[str]:
    return [re.sub(r"\s+", " ", value).strip() for value in values]


def _job_run_commands(ci: dict[str, Any], *, prefix: str) -> list[str]:
    commands: list[str] = []
    for job_name, job in ci["jobs"].items():
        if not str(job_name).startswith(prefix):
            continue
        commands.extend(str(step["run"]) for step in job.get("steps", []) if "run" in step)
    return _normalized_commands(
        [command.removeprefix("uv run --extra dev ").removeprefix("uv run ") for command in commands]
    )


def test_pre_commit_and_ci_include_same_required_quality_gates() -> None:
    """Keep local pre-commit and CI quality gates aligned."""

    pre_commit = _load_yaml(REPO_ROOT / ".pre-commit-config.yaml")
    ci = _load_yaml(REPO_ROOT / ".github" / "workflows" / "ci.yml")

    hook_entries = _normalized_commands(
        [
            str(hook["entry"]).removeprefix("uv run --extra dev ").removeprefix("uv run ")
            for repo in pre_commit["repos"]
            for hook in repo["hooks"]
        ]
    )
    pr_ci_steps = _job_run_commands(ci, prefix="pr-")
    main_ci_steps = _job_run_commands(ci, prefix="main-")

    for command in REQUIRED_GATE_COMMANDS:
        assert any(command in entry for entry in hook_entries), f"pre-commit missing gate: {command}"
        assert any(command in step for step in pr_ci_steps), f"pr-quality missing gate: {command}"
        assert any(command in step for step in main_ci_steps), f"main-quality missing gate: {command}"

    assert ci["jobs"]["pr-unit-tests"]["strategy"]["matrix"]["shard"] == [1, 2, 3, 4]
    assert ci["jobs"]["pr-integration-tests"]["strategy"]["matrix"]["shard"] == [1, 2, 3, 4]
    assert ci["jobs"]["main-unit-tests"]["strategy"]["matrix"]["shard"] == [1, 2, 3, 4]
    assert ci["jobs"]["main-integration-tests"]["strategy"]["matrix"]["shard"] == [1, 2, 3, 4]
    assert ci["jobs"]["pr-quality"]["needs"] == [
        "pr-lint-quality",
        "pr-typing-quality",
        "pr-unit-tests",
        "pr-integration-tests",
        "pr-coverage-95",
    ]
    assert ci["jobs"]["main-quality"]["needs"] == [
        "main-lint-quality",
        "main-typing-quality",
        "main-unit-tests",
        "main-integration-tests",
    ]
    assert any("--suite unit" in step for step in pr_ci_steps)
    assert any("--suite integration" in step for step in pr_ci_steps)
    assert any("--cov=application --cov=ingestion --cov=api" in step for step in main_ci_steps)
    assert any("coverage combine coverage-shards" in step for step in main_ci_steps)
    assert any("coverage report" in step for step in main_ci_steps)


def test_make_check_runs_required_quality_gates() -> None:
    """Keep the Makefile check target aligned with documented validation gates."""

    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    check_match = re.search(r"^check:(?P<deps>.+)$", makefile, flags=re.MULTILINE)
    assert check_match is not None
    check_deps = set(check_match.group("deps").split())

    assert {
        "lint",
        "format",
        "typecheck",
        "pyright",
        "ty",
        "imports",
        "config-check",
        "readme-inventory-check",
        "conventional-commit",
        "test",
    } <= check_deps


def test_coverage_threshold_is_explicit_in_pyproject() -> None:
    """Keep coverage enforcement in the canonical Python tool configuration."""

    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    coverage_report = pyproject["tool"]["coverage"]["report"]

    assert coverage_report["fail_under"] >= 95
    assert coverage_report["show_missing"] is True
    assert "--cov-fail-under" not in pyproject["tool"]["pytest"]["ini_options"]["addopts"]
