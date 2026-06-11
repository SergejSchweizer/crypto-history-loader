PYTHON ?= .venv/bin/python

.PHONY: setup test lint typecheck check

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -U pip
	$(PYTHON) -m pip install -e .

test:
	uv run --extra dev pytest

lint:
	$(PYTHON) -m ruff check .

typecheck:
	$(PYTHON) -m mypy .

check: lint typecheck test
