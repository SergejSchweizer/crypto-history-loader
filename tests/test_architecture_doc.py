"""Documentation contract tests for the architecture overview."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_architecture_doc_exists_and_is_linked_from_readme() -> None:
    """Keep the durable architecture document discoverable from the README."""

    architecture = REPO_ROOT / "ARCHITECTURE.md"
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert architecture.exists()
    assert "ARCHITECTURE.md" in readme


def test_architecture_doc_mentions_enforced_boundaries_and_contracts() -> None:
    """Keep architecture docs aligned with enforced package and dataset contracts."""

    text = (REPO_ROOT / "ARCHITECTURE.md").read_text(encoding="utf-8")

    for expected in (
        ".importlinter",
        "application/datasets.py",
        "application/dataset_contracts.py",
        "config.yaml",
        "lake/bronze -> lake/silver -> lake/gold",
    ):
        assert expected in text


def test_backlog_has_no_duplicate_postgres_source() -> None:
    """Keep BACKLOG.md as the sole repository planning source."""

    assert (REPO_ROOT / "BACKLOG.md").is_file()
    assert not (REPO_ROOT / "BACKLOG_POSTGRES.md").exists()
