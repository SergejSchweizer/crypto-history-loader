"""Tests for deterministic Silver dependency graph validation."""

from __future__ import annotations

import pytest

from application.services.silver_dependency_graph import SilverWorkNode, topological_work_order


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
