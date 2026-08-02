"""Typed, deterministic dependency planning for bounded Silver execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SilverWorkKind = Literal["source", "derived", "sidecar"]


@dataclass(frozen=True)
class SilverWorkNode:
    """One explicit Silver work unit and its published-output dependencies."""

    name: str
    kind: SilverWorkKind
    depends_on: tuple[str, ...] = ()


def topological_work_order(nodes: tuple[SilverWorkNode, ...]) -> tuple[SilverWorkNode, ...]:
    """Return a stable dependency-safe Silver work order.

    Args:
        nodes: Unique named source, derived, or sidecar work nodes.

    Returns:
        Nodes ordered so every declared dependency appears first; ties retain input
        declaration order so retry plans remain reproducible.

    Raises:
        ValueError: If node names repeat, an input is absent, or dependencies cycle.
    """

    by_name = {node.name: node for node in nodes}
    if len(by_name) != len(nodes):
        raise ValueError("Silver dependency graph contains duplicate node names")
    for node in nodes:
        missing = sorted(set(node.depends_on).difference(by_name))
        if missing:
            raise ValueError(f"Silver work node '{node.name}' depends on missing nodes: {', '.join(missing)}")

    resolved: list[SilverWorkNode] = []
    pending = list(nodes)
    resolved_names: set[str] = set()
    while pending:
        ready = [node for node in pending if set(node.depends_on).issubset(resolved_names)]
        if not ready:
            cycle = ", ".join(node.name for node in pending)
            raise ValueError(f"Silver dependency graph contains a cycle: {cycle}")
        resolved.extend(ready)
        resolved_names.update(node.name for node in ready)
        pending = [node for node in pending if node not in ready]
    return tuple(resolved)
