PYTHON ?= .venv/bin/python

.PHONY: setup test lint format typecheck pyright ty imports config-check check

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -U pip
	$(PYTHON) -m pip install -e .

test:
	uv run --extra dev pytest

lint:
	uv run ruff check .

format:
	uv run ruff format --check .

typecheck:
	uv run mypy .

pyright:
	uv run pyright --level error

ty:
	uv run ty check

imports:
	uv run lint-imports --config .importlinter

config-check:
	uv run python scripts/validate_config_with_pydantic.py --config config.yaml

check: lint format typecheck pyright ty imports config-check test
