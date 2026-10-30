"""Pure canonical authority for hypothetical active skill relationship graphs."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .sessions import ContractError
from .skill_models import SkillRevision
from .skill_values import LAYER_RANK, digest, digest_json


@dataclass(frozen=True, slots=True)
class GraphIssue:
    """One deterministic violation in an active relationship graph."""

    code: str
    source_digest: str | None
    target_digest: str | None
    detail: str

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "source_digest": self.source_digest,
            "target_digest": self.target_digest,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class GraphSnapshot:
    """Canonical active graph projection and its validation result."""

    graph_digest: str
    issues: tuple[GraphIssue, ...]
    active_revisions: tuple[SkillRevision, ...]

    @property
    def valid(self) -> bool:
        return not self.issues


def _values(values: Iterable[SkillRevision], label: str) -> tuple[SkillRevision, ...]:
    result = tuple(values)
    if any(type(item) is not SkillRevision for item in result):
        raise ContractError(f"{label} must contain SkillRevision values")
    return result


def hypothetical_active_revisions(
    candidate: SkillRevision,
    active_revisions: Iterable[SkillRevision],
) -> tuple[SkillRevision, ...]:
    """Replace the active pointer for the candidate name, without mutating inputs."""

    if type(candidate) is not SkillRevision:
        raise ContractError("graph candidate must be a SkillRevision")
    active = _values(active_revisions, "active_revisions")
    return tuple(item for item in active if item.skill_name != candidate.skill_name) + (candidate,)


def _declares_conflict(left: SkillRevision, target_digest: str) -> bool:
    return any(
        relation.kind == "conflicts_with" and relation.target == target_digest
        for relation in left.relationships
    )


def _payload(
    active: tuple[SkillRevision, ...], available: dict[str, SkillRevision]
) -> dict[str, object]:
    active_digests = {item.revision_digest for item in active}
    nodes: list[dict[str, object]] = []
    edges: list[dict[str, object]] = []
    for revision in sorted(active, key=lambda item: (item.skill_name, item.revision_digest or "")):
        digest_value = revision.revision_digest or ""
        nodes.append(
            {
                "skill_name": revision.skill_name,
                "revision_digest": digest_value,
                "cognitive_layer": revision.cognitive_layer,
                "relationships": [relation.to_dict() for relation in revision.relationships],
            }
        )
        for relation in revision.relationships:
            target_state = (
                "active"
                if relation.target in active_digests
                else "materialized"
                if relation.target in available
                else "missing"
            )
            edges.append(
                {
                    "source_digest": digest_value,
                    "kind": relation.kind,
                    "target_digest": relation.target,
                    "target_state": target_state,
                }
            )
    edges.sort(key=lambda item: (item["source_digest"], item["kind"], item["target_digest"]))
    return {
        "active": [
            {"skill_name": item.skill_name, "revision_digest": item.revision_digest}
            for item in sorted(active, key=lambda item: item.skill_name)
        ],
        "nodes": nodes,
        "edges": edges,
    }


def validate_active_graph(
    active_revisions: Iterable[SkillRevision],
    materialized_revisions: Iterable[SkillRevision] = (),
) -> GraphSnapshot:
    """Validate exact-digest edges for a hypothetical active graph.

    Dormant materialized revisions are used only to prove exact target existence and target
    activity. They are not included as graph nodes, so unrelated dormant rows cannot change
    the snapshot digest.
    """

    active = _values(active_revisions, "active_revisions")
    materialized = _values(materialized_revisions, "materialized_revisions")
    available: dict[str, SkillRevision] = {}
    for revision in materialized + active:
        if revision.revision_digest is None:
            raise ContractError("graph revisions must have exact digests")
        available.setdefault(revision.revision_digest, revision)
    active_by_digest = {item.revision_digest: item for item in active}
    issues: list[GraphIssue] = []
    active_names: dict[str, list[SkillRevision]] = {}
    dependency_edges: dict[str, set[str]] = {}
    for revision in active:
        active_names.setdefault(revision.skill_name, []).append(revision)
        source_digest = revision.revision_digest or ""
        for relation in revision.relationships:
            target = available.get(relation.target)
            if target is None:
                issues.append(
                    GraphIssue(
                        "missing-target",
                        source_digest,
                        relation.target,
                        "relationship target is not materialized",
                    )
                )
                continue
            if relation.kind != "depends_on":
                continue
            if relation.target not in active_by_digest:
                issues.append(
                    GraphIssue(
                        "inactive-dependency",
                        source_digest,
                        relation.target,
                        "dependency target is materialized but inactive",
                    )
                )
            if LAYER_RANK[target.cognitive_layer] > LAYER_RANK[revision.cognitive_layer]:
                issues.append(
                    GraphIssue(
                        "later-layer-dependency",
                        source_digest,
                        relation.target,
                        "dependency points to a later cognitive layer",
                    )
                )
            if relation.target in active_by_digest:
                dependency_edges.setdefault(source_digest, set()).add(relation.target)
    for name, revisions in sorted(active_names.items()):
        if len(revisions) > 1:
            issues.append(
                GraphIssue(
                    "duplicate-active-name",
                    revisions[0].revision_digest,
                    revisions[1].revision_digest,
                    f"active graph contains multiple revisions for {name}",
                )
            )
    ordered_active = sorted(active, key=lambda item: item.revision_digest or "")
    for index, left in enumerate(ordered_active):
        for right in ordered_active[index:]:
            left_digest = left.revision_digest or ""
            right_digest = right.revision_digest or ""
            if _declares_conflict(left, right_digest) or _declares_conflict(right, left_digest):
                issues.append(
                    GraphIssue(
                        "active-conflict",
                        left_digest,
                        right_digest,
                        "active conflict relationship is not symmetric-safe",
                    )
                )
    colors: dict[str, int] = {}
    stack: list[str] = []
    cycle_keys: set[tuple[str, ...]] = set()

    def visit(source_digest: str) -> None:
        colors[source_digest] = 1
        stack.append(source_digest)
        for target_digest in sorted(dependency_edges.get(source_digest, ())):
            if colors.get(target_digest, 0) == 1:
                start = stack.index(target_digest)
                cycle = tuple(stack[start:] + [target_digest])
                key = tuple(sorted(set(cycle)))
                if key not in cycle_keys:
                    cycle_keys.add(key)
                    issues.append(
                        GraphIssue(
                            "dependency-cycle",
                            source_digest,
                            target_digest,
                            f"dependency cycle: {' -> '.join(cycle)}",
                        )
                    )
            elif colors.get(target_digest, 0) == 0:
                visit(target_digest)
        stack.pop()
        colors[source_digest] = 2

    for source_digest in sorted(dependency_edges):
        if colors.get(source_digest, 0) == 0:
            visit(source_digest)
    unique_issues = tuple(
        dict.fromkeys(
            sorted(
                issues,
                key=lambda item: (
                    item.code,
                    item.source_digest or "",
                    item.target_digest or "",
                    item.detail,
                ),
            )
        )
    )
    graph_digest = digest_json(_payload(active, available))
    return GraphSnapshot(graph_digest, unique_issues, tuple(active))


def graph_snapshot_digest(
    active_revisions: Iterable[SkillRevision],
    materialized_revisions: Iterable[SkillRevision] = (),
) -> str:
    """Return the digest of the canonical active graph projection."""

    result = validate_active_graph(active_revisions, materialized_revisions)
    digest(result.graph_digest, "graph_snapshot_digest")
    return result.graph_digest


__all__ = [
    "GraphIssue",
    "GraphSnapshot",
    "graph_snapshot_digest",
    "hypothetical_active_revisions",
    "validate_active_graph",
]
