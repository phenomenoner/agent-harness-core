"""Application reflection, independent evaluation, belief update, and CAS activation."""

from __future__ import annotations

import re
from collections.abc import Mapping

from ahc.domain.judgment_records import OutcomeAttribution
from ahc.domain.judgment_routing import require_learning_layer
from ahc.domain.sessions import ContractError
from ahc.domain.skill_behavior import HeldOutTaskEvidence
from ahc.domain.skill_models import SkillRelationship, SkillRevision
from ahc.domain.skill_records import SkillEpisode
from ahc.domain.skill_values import (
    SKILL_REVISION_SCHEMA_VERSION,
    digest,
    digest_json,
    exact_mapping,
    skill_name,
)
from ahc.infrastructure.skill_evaluator import evaluate_revision
from ahc.infrastructure.skill_repository import SkillRepository

_SAFE_NAME = re.compile(r"[^a-z0-9-]+")
_RELATION_DIGEST = re.compile(r"^[0-9a-f]{64}$")


def _body(name: str) -> str:
    return (
        f"# {name}\n\n"
        "## Purpose\nA bounded reusable procedure.\n\n"
        "## Activation\nUse when the declared trigger matches.\n\n"
        "## Procedure\nFollow the explicit steps inside the accepted goal.\n\n"
        "## Verification\nCheck the observable result independently.\n\n"
        "## Boundaries\nNo authority, identity, or external effect changes.\n"
    )


def _version(value: str) -> tuple[int, int, int]:
    return tuple(int(part) for part in value.split("."))  # type: ignore[return-value]


def _seq(lesson: Mapping[str, object], key: str, default: tuple[object, ...]) -> tuple[object, ...]:
    value = lesson.get(key, default)
    if not isinstance(value, (list, tuple)):
        raise ContractError(f"lesson {key} must be an array")
    return tuple(value)


def _resolve_relationships(
    lesson: Mapping[str, object],
    materialized_revisions: tuple[SkillRevision, ...],
    active_revisions: tuple[SkillRevision, ...],
) -> tuple[SkillRelationship, ...]:
    by_digest: dict[str, SkillRevision] = {}
    for revision in materialized_revisions:
        if type(revision) is not SkillRevision or revision.revision_digest is None:
            raise ContractError("materialized relationship catalog is invalid")
        by_digest[revision.revision_digest] = revision
    by_name: dict[str, SkillRevision] = {}
    for revision in active_revisions:
        if type(revision) is not SkillRevision or revision.revision_digest is None:
            raise ContractError("active relationship catalog is invalid")
        if revision.skill_name in by_name:
            raise ContractError("active relationship name is ambiguous")
        by_name[revision.skill_name] = revision
    resolved: list[SkillRelationship] = []
    for value in _seq(lesson, "relationships", ()):
        raw = exact_mapping(value, frozenset({"kind", "target"}), "lesson relationship")
        kind = raw["kind"]
        target = raw["target"]
        if type(kind) is not str or type(target) is not str:
            raise ContractError("lesson relationship kind and target must be text")
        if _RELATION_DIGEST.fullmatch(target):
            if target not in by_digest:
                raise ContractError("lesson relationship target is not materialized")
            target_digest = target
        else:
            try:
                skill_name(target, "lesson relationship target")
            except ContractError as exc:
                raise ContractError("lesson relationship target must be a name or digest") from exc
            match = by_name.get(target)
            if match is None:
                raise ContractError("lesson relationship name is not active")
            target_digest = match.revision_digest or ""
        digest(target_digest, "resolved relationship target")
        resolved.append(SkillRelationship(kind, target_digest))
    return tuple(resolved)


def reflect_lesson(
    lesson: Mapping[str, object],
    *,
    episode: SkillEpisode,
    active: SkillRevision | None,
    materialized_revisions: tuple[SkillRevision, ...] = (),
    active_revisions: tuple[SkillRevision, ...] = (),
) -> SkillRevision:
    if not isinstance(lesson, Mapping):
        raise ContractError("lesson must be an object")
    name = lesson.get("skill_name", "learned-skill")
    if type(name) is not str:
        raise ContractError("lesson skill_name must be text")
    name = _SAFE_NAME.sub("-", name.casefold()).strip("-") or "learned-skill"
    if active is not None:
        major, minor, patch = _version(active.version)
        revision_version = f"{major}.{minor}.{patch + 1}"
        predecessor = active.revision_digest
    else:
        revision_version, predecessor = "0.1.0", None
    body = lesson.get("body", _body(name))
    relations = _resolve_relationships(lesson, materialized_revisions, active_revisions)
    return SkillRevision(
        SKILL_REVISION_SCHEMA_VERSION,
        name,
        revision_version if active is not None else str(lesson.get("version", revision_version)),
        str(lesson.get("description", "A bounded experimental procedural lesson")),
        str(lesson.get("cognitive_layer", "how-procedure")),
        tuple(_seq(lesson, "roles", ("procedure",))),
        tuple(_seq(lesson, "agent_allowlist", ())),
        tuple(_seq(lesson, "required_tools", ())),
        tuple(_seq(lesson, "triggers", ("procedure",))),
        tuple(_seq(lesson, "expected_effects", ("descriptive procedure only",))),
        tuple(_seq(lesson, "positive_examples", ("procedure task",))),
        tuple(_seq(lesson, "negative_examples", ("unrelated task",))),
        relations,
        predecessor,
        str(body),
    )


def _episode_with_outcome(
    episode: SkillEpisode,
    outcome: str,
    evidence_refs: tuple[str, ...],
    lesson: dict[str, object] | None,
    attribution: OutcomeAttribution | None,
) -> SkillEpisode:
    return SkillEpisode(
        episode.schema_version,
        episode.episode_id,
        episode.session,
        episode.turn_id,
        episode.task_digest,
        episode.manifest_digest,
        episode.surfaced_revisions,
        episode.disclosed_revisions,
        episode.used_revisions,
        outcome,
        evidence_refs,
        lesson,
        None if attribution is None else attribution.digest,
    )


def learn_episode(
    repository: SkillRepository,
    *,
    episode_id: str,
    outcome: str,
    evidence_refs: tuple[str, ...],
    attribution: OutcomeAttribution | None = None,
    lesson: dict[str, object] | None = None,
    reflect: bool = False,
    evaluate: bool = False,
    activate: bool = False,
    held_out_evidence: tuple[HeldOutTaskEvidence, ...] | list[HeldOutTaskEvidence] = (),
    operation_key: str | None = None,
) -> dict[str, object]:
    episode = repository.get_episode(episode_id)
    if episode is None:
        raise ContractError("episode is not recorded")
    if attribution is not None and attribution.episode_id != episode_id:
        raise ContractError("attribution episode identity is stale")
    if outcome == "unknown" and (reflect or evaluate or activate):
        raise ContractError("unknown outcome cannot reflect, evaluate, or activate")
    if (reflect or evaluate or activate) and attribution is None:
        raise ContractError("reflection/evaluation/activation requires attribution")
    if attribution is not None:
        repository.save_judgment_record(attribution)
    updated = _episode_with_outcome(episode, outcome, evidence_refs, lesson, attribution)
    if episode.to_dict() != updated.to_dict():
        updated = repository.update_episode(updated)
    changed = repository.apply_belief_update(updated, outcome=outcome)
    result: dict[str, object] = {"episode": updated, "belief_updates": changed}
    if not (reflect or evaluate or activate):
        return result
    if lesson is None or attribution is None:
        raise ContractError("a reusable lesson and attribution are required")
    active = None
    candidate_name = lesson.get("skill_name")
    active_revisions, materialized_revisions = repository.graph_revisions()
    if isinstance(candidate_name, str):
        active = next(
            (item for item in active_revisions if item.skill_name == candidate_name),
            None,
        )
    candidate = reflect_lesson(
        lesson,
        episode=updated,
        active=active,
        materialized_revisions=materialized_revisions,
        active_revisions=active_revisions,
    )
    require_learning_layer(attribution, candidate.cognitive_layer)
    repository.register_revision(candidate)
    proposal_payload = {
        "episode_id": episode_id,
        "attribution": attribution.to_dict(),
        "candidate": candidate.to_dict(),
    }
    proposal_digest = digest_json(proposal_payload)
    repository.save_proposal(
        proposal_digest,
        episode_id,
        candidate.revision_digest or "0" * 64,
        attribution.digest or "0" * 64,
        proposal_payload,
    )
    result["candidate"] = candidate
    result["proposal_digest"] = proposal_digest
    if not (evaluate or activate):
        return result
    evaluation_active, evaluation_materialized = repository.graph_revisions()
    report = evaluate_revision(
        candidate,
        active_revisions=evaluation_active,
        materialized_revisions=evaluation_materialized,
        behavior_evidence=held_out_evidence,
    )
    repository.save_evaluation(report)
    result["evaluation"] = report
    if not activate:
        return result
    if report.verdict != "accept":
        raise ContractError("independent evaluator blocked activation")
    current_digest = None if active is None else active.revision_digest
    receipt = repository.activate(
        skill_name=candidate.skill_name,
        candidate_digest=candidate.revision_digest or "0" * 64,
        expected_active_digest=current_digest,
        evaluation_digest=report.evaluation_digest or "0" * 64,
        input_digest=report.input_digest,
        operation_key=operation_key or f"activate:{proposal_digest}",
        graph_input_digest=report.graph_input_digest,
    )
    result["activation"] = receipt
    return result


__all__ = ["learn_episode", "reflect_lesson"]
