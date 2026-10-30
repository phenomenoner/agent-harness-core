"""Pure role-first skill eligibility, trigger matching, and selection."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass

from .sessions import ContractError
from .skill_graph import validate_active_graph
from .skill_models import SelectionRequest, SkillRevision
from .skill_records import ManifestEntry, SkillManifest
from .skill_values import (
    COGNITIVE_LAYERS,
    LAYER_RANK,
    SKILL_MANIFEST_SCHEMA_VERSION,
)

_TOKEN_RE = re.compile(r"[a-z0-9]+|[\u3400-\u9fff]")


@dataclass(frozen=True, slots=True)
class SelectionResult:
    manifest: SkillManifest
    selected: tuple[SkillRevision, ...]
    abstained_roles: tuple[str, ...]


def _normal(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold()


def trigger_evidence(trigger: str, task_text: str) -> tuple[str, ...]:
    needle = _normal(trigger)
    haystack = _normal(task_text)
    if not needle:
        return ()
    needle_tokens = _TOKEN_RE.findall(needle)
    haystack_tokens = _TOKEN_RE.findall(haystack)
    width = len(needle_tokens)
    matched = width > 0 and any(
        haystack_tokens[index : index + width] == needle_tokens
        for index in range(len(haystack_tokens) - width + 1)
    )
    return (trigger,) if matched else ()


def eligible(
    revision: SkillRevision, request: SelectionRequest, role: str
) -> tuple[bool, tuple[str, ...]]:
    if role not in revision.roles:
        return False, ()
    if revision.agent_allowlist and request.agent_id not in revision.agent_allowlist:
        return False, ()
    if not set(revision.required_tools).issubset(request.available_tools):
        return False, ()
    evidence = tuple(
        sorted(
            item
            for trigger in revision.triggers
            for item in trigger_evidence(trigger, request.task_text)
        )
    )
    if not evidence:
        return False, ()
    return True, evidence


def _score(revision: SkillRevision, evidence: tuple[str, ...], belief: object) -> float:
    successes = failures = 0
    if isinstance(belief, Mapping):
        successes = int(belief.get("successes", 0))
        failures = int(belief.get("failures", 0))
    elif isinstance(belief, tuple) and len(belief) == 2:
        successes, failures = int(belief[0]), int(belief[1])
    posterior = (successes + 1.0) / (successes + failures + 2.0)
    cost = len(revision.body.encode("utf-8")) / 98_304.0
    return (len(evidence) / len(revision.triggers)) + posterior - cost


def _target(target: str, by_digest: dict[str, SkillRevision]) -> SkillRevision | None:
    return by_digest.get(target)


def _conflicts(left: SkillRevision, right: SkillRevision) -> bool:
    target_digest = right.revision_digest
    return any(
        relation.kind == "conflicts_with" and relation.target == target_digest
        for relation in left.relationships
    )


def select_revisions(
    request: SelectionRequest,
    active_revisions: tuple[SkillRevision, ...] | list[SkillRevision],
    beliefs: Mapping[tuple[str, str, str], object] | None = None,
    materialized_revisions: tuple[SkillRevision, ...] | list[SkillRevision] | None = None,
) -> SelectionResult:
    values = tuple(active_revisions)
    if any(type(item) is not SkillRevision for item in values):
        raise ContractError("active_revisions must contain SkillRevision values")
    catalog = values if materialized_revisions is None else tuple(materialized_revisions)
    graph = validate_active_graph(values, catalog)
    if not graph.valid:
        manifest = SkillManifest(
            SKILL_MANIFEST_SCHEMA_VERSION,
            request.session,
            request.turn_id,
            request.goal_digest,
            (),
        )
        return SelectionResult(manifest, (), request.roles)
    by_digest = {item.revision_digest: item for item in values if item.revision_digest is not None}
    belief_map = {} if beliefs is None else beliefs
    chosen: list[tuple[SkillRevision, str, tuple[str, ...], float]] = []
    claimed: set[tuple[str, str]] = set()
    selected_digests: set[str] = set()

    def can_add(
        candidate: SkillRevision, role: str, evidence: tuple[str, ...], stack: set[str]
    ) -> bool:
        chosen_length = len(chosen)
        claimed_before = set(claimed)
        selected_before = set(selected_digests)

        def reject() -> bool:
            del chosen[chosen_length:]
            claimed.clear()
            claimed.update(claimed_before)
            selected_digests.clear()
            selected_digests.update(selected_before)
            return False

        key = (candidate.cognitive_layer, role)
        if key in claimed and candidate.revision_digest not in selected_digests:
            return reject()
        if candidate.revision_digest in stack:
            return reject()
        if any(
            _conflicts(candidate, selected) or _conflicts(selected, candidate)
            for selected, _, _, _ in chosen
        ):
            return reject()
        stack = set(stack)
        stack.add(candidate.revision_digest or "")
        for relation in candidate.relationships:
            target = _target(relation.target, by_digest)
            if relation.kind == "depends_on":
                if (
                    target is None
                    or LAYER_RANK[target.cognitive_layer] > LAYER_RANK[candidate.cognitive_layer]
                ):
                    return reject()
                target_role = next(
                    (
                        item
                        for item in target.roles
                        if (target.cognitive_layer, item) not in claimed
                    ),
                    target.roles[0],
                )
                target_ok, target_evidence = eligible(target, request, target_role)
                if not target_ok or not can_add(target, target_role, target_evidence, stack):
                    return reject()
                if target.revision_digest not in selected_digests:
                    chosen.append(
                        (
                            target,
                            target_role,
                            target_evidence,
                            _score(
                                target,
                                target_evidence,
                                belief_map.get(
                                    (target.revision_digest or "", request.agent_id, target_role)
                                ),
                            ),
                        )
                    )
                    claimed.add((target.cognitive_layer, target_role))
                    selected_digests.add(target.revision_digest or "")
            elif (
                relation.kind == "conflicts_with"
                and target is not None
                and target.revision_digest in selected_digests
            ):
                return reject()
        return True

    for layer in COGNITIVE_LAYERS:
        for role in request.roles:
            if (layer, role) in claimed:
                continue
            candidates: list[tuple[float, str, SkillRevision, tuple[str, ...]]] = []
            for candidate in values:
                if candidate.cognitive_layer != layer:
                    continue
                ok, evidence = eligible(candidate, request, role)
                if ok:
                    candidates.append(
                        (
                            _score(
                                candidate,
                                evidence,
                                belief_map.get(
                                    (candidate.revision_digest or "", request.agent_id, role)
                                ),
                            ),
                            candidate.revision_digest or "",
                            candidate,
                            evidence,
                        )
                    )
            candidates.sort(key=lambda item: (-item[0], item[1]))
            for score, _, candidate, evidence in candidates:
                if can_add(candidate, role, evidence, set()):
                    chosen.append((candidate, role, evidence, score))
                    claimed.add((layer, role))
                    selected_digests.add(candidate.revision_digest or "")
                    break

    chosen.sort(
        key=lambda item: (
            LAYER_RANK[item[0].cognitive_layer],
            item[1],
            item[0].revision_digest or "",
        )
    )
    entries = tuple(
        ManifestEntry(
            item[0].revision_digest or "",
            item[0].skill_name,
            item[0].cognitive_layer,
            item[1],
            item[2],
            item[3],
        )
        for item in chosen
    )
    manifest = SkillManifest(
        SKILL_MANIFEST_SCHEMA_VERSION,
        request.session,
        request.turn_id,
        request.goal_digest,
        entries,
    )
    selected_values = tuple(dict.fromkeys(item[0] for item in chosen))
    abstained = tuple(role for role in request.roles if not any(item[1] == role for item in chosen))
    return SelectionResult(manifest, selected_values, abstained)


def disclose_bodies(
    manifest: SkillManifest, revisions: Mapping[str, SkillRevision]
) -> dict[str, str]:
    output: dict[str, str] = {}
    total = 0
    for entry in manifest.entries:
        revision = revisions.get(entry.revision_digest)
        if revision is None or revision.revision_digest != entry.revision_digest:
            raise ContractError("manifest references a missing or drifted revision")
        output[entry.revision_digest] = revision.body
        total += len(revision.body.encode("utf-8"))
    if total > 98_304:
        raise ContractError("manifest disclosure exceeds 98304 UTF-8 bytes")
    return output


__all__ = ["SelectionResult", "disclose_bodies", "eligible", "select_revisions", "trigger_evidence"]
