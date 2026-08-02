"""Tests for deterministic Silver dependency graph validation."""

from __future__ import annotations

import pytest

from application.services.silver_dependency_graph import SilverWorkNode, bounded_work_batches, topological_work_order


def test_graph_orders_sources_before_derived_and_sidecars() -> None:
    """Produce a stable order while retaining independent input declaration order."""

    nodes = (
        SilverWorkNode("funding_feature", "derived", ("funding_observed",)),
        SilverWorkNode("spot_observed", "source"),
        SilverWorkNode("funding_observed", "source"),
        SilverWorkNode("funding_sidecar", "sidecar", ("funding_feature",)),
    )

    assert [node.name for node in topological_work_order(nodes)] == [
        "spot_observed",
        "funding_observed",
        "funding_feature",
        "funding_sidecar",
    ]


@pytest.mark.parametrize(
    ("nodes", "message"),
    [
        (
            (SilverWorkNode("feature", "derived", ("missing",)),),
            "missing nodes",
        ),
        (
            (
                SilverWorkNode("a", "derived", ("b",)),
                SilverWorkNode("b", "derived", ("a",)),
            ),
            "cycle",
        ),
        (
            (SilverWorkNode("a", "source"), SilverWorkNode("a", "sidecar")),
            "duplicate",
        ),
    ],
)
def test_graph_rejects_invalid_contracts(nodes: tuple[SilverWorkNode, ...], message: str) -> None:
    """Fail before work starts when published-input ownership is ambiguous."""

    with pytest.raises(ValueError, match=message):
        topological_work_order(nodes)


def test_bounded_batches_preserve_dependencies_and_four_worker_limit() -> None:
    """Release source work before scheduling dependent work beyond a batch boundary."""

    nodes = (
        SilverWorkNode("source_a", "source"),
        SilverWorkNode("source_b", "source"),
        SilverWorkNode("source_c", "source"),
        SilverWorkNode("source_d", "source"),
        SilverWorkNode("source_e", "source"),
        SilverWorkNode("derived", "derived", ("source_a", "source_e")),
    )

    assert [[node.name for node in batch] for batch in bounded_work_batches(nodes, max_workers=4)] == [
        ["source_a", "source_b", "source_c", "source_d"],
        ["source_e"],
        ["derived"],
    ]


@pytest.mark.parametrize("max_workers", [0, 5])
def test_bounded_batches_rejects_invalid_worker_limits(max_workers: int) -> None:
    """Keep application concurrency aligned with the repository-wide Polars bound."""

    with pytest.raises(ValueError, match="1 through 4"):
        bounded_work_batches((SilverWorkNode("source", "source"),), max_workers=max_workers)
