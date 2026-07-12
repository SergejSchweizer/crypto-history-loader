"""Tests for Conventional Commit validation."""

from __future__ import annotations

from scripts.validate_conventional_commit import main, validate_subject


def test_validate_subject_accepts_conventional_commit_forms() -> None:
    """Allow common Conventional Commit subject variants."""

    assert validate_subject("feat: add full gold dataset") is None
    assert validate_subject("fix(loader): close trade gap planner bug") is None
    assert validate_subject("feat!: change exported dataset contract") is None
    assert validate_subject("refactor(gold)!: split live joins") is None


def test_validate_subject_rejects_non_conventional_subjects() -> None:
    """Reject imperative-only or malformed commit subjects."""

    assert validate_subject("Update README missing day snapshot") is not None
    assert validate_subject("feat add missing colon") is not None
    assert validate_subject("unknown: add dataset") is not None
    assert validate_subject("") is not None


def test_main_validates_message_file(tmp_path) -> None:
    """Validate the commit-msg hook entrypoint."""

    message = tmp_path / "COMMIT_EDITMSG"
    message.write_text("docs: update quality policy\n\nBody text.\n", encoding="utf-8")

    assert main(["--message-file", str(message)]) == 0

    message.write_text("Update quality policy\n", encoding="utf-8")
    assert main(["--message-file", str(message)]) == 1
