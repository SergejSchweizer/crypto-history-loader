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
    pr_ci_steps = _normalized_commands(
        [
            str(step["run"]).removeprefix("uv run --extra dev ").removeprefix("uv run ")
            for step in ci["jobs"]["pr-quality"]["steps"]
            if "run" in step
        ]
    )
    main_ci_steps = _normalized_commands(
        [
            str(step["run"]).removeprefix("uv run --extra dev ").removeprefix("uv run ")
            for step in ci["jobs"]["main-quality"]["steps"]
            if "run" in step
        ]
    )

    for command in REQUIRED_GATE_COMMANDS:
        assert any(command in entry for entry in hook_entries), f"pre-commit missing gate: {command}"
        assert any(command in step for step in pr_ci_steps), f"pr-quality missing gate: {command}"
        assert any(command in step for step in main_ci_steps), f"main-quality missing gate: {command}"

    assert any("pytest --cov --cov-report=term-missing" in step for step in main_ci_steps)


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

    assert coverage_report["fail_under"] >= 85
    assert coverage_report["show_missing"] is True
    assert "--cov-fail-under" not in pyproject["tool"]["pytest"]["ini_options"]["addopts"]
