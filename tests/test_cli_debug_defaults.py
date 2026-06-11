"""Tests for CLI debug logging defaults."""

from __future__ import annotations

import argparse

from api.cli import _is_debug_logging_enabled


def test_build_commands_enable_debug_logging_by_default() -> None:
    """Bronze, Silver, and Gold build commands should default to debug logging."""

    for command in ("bronze-build", "silver-build", "gold-build"):
        assert _is_debug_logging_enabled(argparse.Namespace(command=command, debug=False)) is True


def test_non_build_commands_keep_debug_logging_opt_in() -> None:
    """Non-build commands should require the explicit debug switch."""

    assert _is_debug_logging_enabled(argparse.Namespace(command="list-spot-timeframes", debug=False)) is False
    assert _is_debug_logging_enabled(argparse.Namespace(command="list-spot-timeframes", debug=True)) is True
