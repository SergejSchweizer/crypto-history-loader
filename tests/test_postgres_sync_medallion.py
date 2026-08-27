"""Regression tests for post-Gold PostgreSQL synchronization ordering."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest


def _load_pipeline_module() -> Any:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_medallion_pipeline.py"
    spec = importlib.util.spec_from_file_location("run_medallion_pipeline_postgres", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _config(*, gold_enabled: bool = True) -> dict[str, object]:
    return {
        "medallion-pipeline": {
            "execution_order": ["bronze", "silver", "gold"],
            "bronze": {"enabled": True, "command": "bronze-build", "cli_args": []},
            "silver": {"enabled": True, "command": "silver-build", "cli_args": []},
            "gold": {"enabled": gold_enabled, "command": "gold-build", "cli_args": []},
        }
    }


def test_postgres_sync_is_exactly_once_directly_after_gold(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    config_path = tmp_path / "config.yaml"
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=_config())

    assert [step.name for step in steps] == ["bronze", "silver", "gold", "postgres-gold-sync"]
    publication_result = tmp_path / ".run" / "gold-publication-result.json"
    gold_step = steps[-2]
    assert gold_step.args[-2:] == ["--publication-result", str(publication_result)]
    sync_step = steps[-1]
    assert sync_step.args == [
        str(main_path),
        "--config",
        str(config_path),
        "gold-sync-postgres",
        "--gold-root",
        "lake/gold",
        "--publication-result",
        str(publication_result),
    ]
    assert sum(step.name == "postgres-gold-sync" for step in steps) == 1


def test_disabled_gold_produces_no_postgres_sync(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    steps = module._build_steps(
        main_path=tmp_path / "main.py",
        config_path=tmp_path / "config.yaml",
        config_data=_config(gold_enabled=False),
    )
    assert [step.name for step in steps] == ["bronze", "silver"]


def test_duplicate_gold_layer_is_rejected_before_execution(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    config = _config()
    pipeline = config["medallion-pipeline"]
    assert isinstance(pipeline, dict)
    pipeline["execution_order"] = ["bronze", "gold", "gold"]
    with pytest.raises(ValueError, match="Gold at most once"):
        module._build_steps(
            main_path=tmp_path / "main.py",
            config_path=tmp_path / "config.yaml",
            config_data=config,
        )


def test_gold_failure_gates_postgres_sync(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_pipeline_module()
    steps = module._build_steps(
        main_path=tmp_path / "main.py",
        config_path=tmp_path / "config.yaml",
        config_data=_config(),
    )
    executed: list[str] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        command_name = next(
            argument
            for argument in command
            if argument in {"bronze-build", "silver-build", "gold-build", "gold-sync-postgres"}
        )
        executed.append(command_name)
        if command_name == "gold-build":
            raise subprocess.CalledProcessError(7, command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    with pytest.raises(subprocess.CalledProcessError):
        module._run_pipeline(
            python_bin="python",
            steps=steps,
            repo_root=tmp_path,
            env={},
        )
    assert executed == ["bronze-build", "silver-build", "gold-build"]
    assert "gold-sync-postgres" not in executed


def test_postgres_failure_propagates_after_gold_without_rebuild(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _load_pipeline_module()
    steps = module._build_steps(
        main_path=tmp_path / "main.py",
        config_path=tmp_path / "config.yaml",
        config_data=_config(),
    )
    executed: list[str] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        command_name = next(
            argument
            for argument in command
            if argument in {"bronze-build", "silver-build", "gold-build", "gold-sync-postgres"}
        )
        executed.append(command_name)
        if command_name == "gold-sync-postgres":
            raise subprocess.CalledProcessError(9, command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    with pytest.raises(subprocess.CalledProcessError):
        module._run_pipeline(
            python_bin="python",
            steps=steps,
            repo_root=tmp_path,
            env={},
        )
    assert executed == ["bronze-build", "silver-build", "gold-build", "gold-sync-postgres"]

    executed.clear()
    retry = [step for step in steps if step.name == "postgres-gold-sync"]
    with pytest.raises(subprocess.CalledProcessError):
        module._run_pipeline(python_bin="python", steps=retry, repo_root=tmp_path, env={})
    assert executed == ["gold-sync-postgres"]


def test_sync_plan_contains_no_credentials(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    steps = module._build_steps(
        main_path=tmp_path / "main.py",
        config_path=tmp_path / "config.yaml",
        config_data=_config(),
    )
    serialized = repr([(step.name, step.args) for step in steps])
    assert "PGPASSWORD" not in serialized
    assert "PGADMINPASSWORD" not in serialized
    assert "postgresql://" not in serialized


def test_pipeline_rejects_implicit_serving_deletion_policy(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    config = _config()
    pipeline = config["medallion-pipeline"]
    assert isinstance(pipeline, dict)
    pipeline["postgres-sync"] = {"serving_deprecation_policy": "delete"}

    with pytest.raises(ValueError, match="serving deletion requires"):
        module._build_steps(
            main_path=tmp_path / "main.py",
            config_path=tmp_path / "config.yaml",
            config_data=config,
        )
