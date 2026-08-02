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


def bounded_work_batches(
    nodes: tuple[SilverWorkNode, ...],
    *,
    max_workers: int,
) -> tuple[tuple[SilverWorkNode, ...], ...]:
    """Group topologically valid work into deterministic bounded execution batches.

    A node is placed only after every dependency is in an earlier batch. Independent
    nodes retain declaration order and each batch has at most ``max_workers`` items,
    so callers can release all batch-local source frames before the next batch.

    Args:
        nodes: Valid or invalid work graph nodes to schedule.
        max_workers: Maximum concurrent application workers, from one through four.

    Returns:
        Ordered worker batches with dependencies published before their consumers.

    Raises:
        ValueError: If the worker bound is unsupported or the graph is invalid.
    """

    if not 1 <= max_workers <= 4:
        raise ValueError("max_workers must be an integer from 1 through 4")
    ordered = topological_work_order(nodes)
    batches: list[tuple[SilverWorkNode, ...]] = []
    completed: set[str] = set()
    remaining = list(ordered)
    while remaining:
        ready = [node for node in remaining if set(node.depends_on).issubset(completed)]
        batch = tuple(ready[:max_workers])
        if not batch:
            raise ValueError("Silver dependency graph cannot schedule remaining work")
        batches.append(batch)
        completed.update(node.name for node in batch)
        remaining = [node for node in remaining if node not in batch]
    return tuple(batches)
