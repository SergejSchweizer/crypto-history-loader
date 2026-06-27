"""Architecture boundary tests using import-linter."""

from __future__ import annotations

import ast
import configparser
import subprocess
import sys
from pathlib import Path


def test_import_linter_contracts_pass() -> None:
    """Validate import-linter contracts for package layering."""

    executable = Path(sys.executable).parent / "lint-imports"
    process = subprocess.run(
        [str(executable), "--config", ".importlinter"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert process.returncode == 0, process.stdout + process.stderr


def test_import_linter_includes_acyclic_root_package_contract() -> None:
    """Root packages should remain guarded against circular sibling dependencies."""

    config = configparser.ConfigParser()
    config.read(".importlinter")

    section = "importlinter:contract:acyclic_root_packages"
    assert section in config
    assert config[section]["type"] == "acyclic_siblings"
    assert set(config[section]["ancestors"].split()) == {"api", "application", "ingestion"}


def test_api_does_not_import_lake_adapter_directly() -> None:
    """API modules should reach lake persistence through application-facing services."""

    violations: list[str] = []
    for path in sorted(Path("api").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "ingestion.lake":
                violations.append(f"{path}:{node.lineno}")
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "ingestion.lake":
                        violations.append(f"{path}:{node.lineno}")

    assert violations == []
