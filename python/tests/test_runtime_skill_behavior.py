"""R5 behavior evidence and content-guard discriminators."""

from __future__ import annotations

import json

import pytest
from skill_test_support import goal, request, revision

from ahc.application.skill_learning import learn_episode, reflect_lesson
from ahc.application.skill_service import SkillTurnService
from ahc.domain import OutcomeAttribution
from ahc.domain.judgment_records import ATTRIBUTION_SCHEMA
from ahc.domain.sessions import ContractError
from ahc.domain.skill_behavior import HeldOutTaskEvidence
from ahc.domain.skill_guard import GuardViolation, guard_text
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.skill_evaluator import evaluate_revision
from ahc.infrastructure.skill_repository import SkillRepository


def _lesson() -> dict[str, object]:
    item = revision()
    return {
        "skill_name": item.skill_name,
        "description": item.description,
        "cognitive_layer": item.cognitive_layer,
        "roles": list(item.roles),
        "agent_allowlist": list(item.agent_allowlist),
        "required_tools": list(item.required_tools),
        "triggers": list(item.triggers),
        "expected_effects": list(item.expected_effects),
        "positive_examples": list(item.positive_examples),
        "negative_examples": list(item.negative_examples),
        "relationships": [],
        "body": item.body,
    }


def _evidence(
    candidate_digest: str,
    *,
    observed: str = "right",
    uncertainty: tuple[str, ...] = (),
) -> HeldOutTaskEvidence:
    return HeldOutTaskEvidence(
        candidate_digest,
        digest_json({"task": "held-out-task"}),
        digest_json({"answer": "right"}),
        digest_json({"answer": observed}),
        ("evidence-held-out-1",),
        uncertainty,
    )


def test_structural_validity_is_distinct_from_candidate_bound_behavior() -> None:
    candidate = revision()
    wrong = evaluate_revision(
        candidate,
        behavior_evidence=(_evidence(candidate.revision_digest or "", observed="wrong"),),
    )

    assert wrong.structural_verdict == "accept"
    assert wrong.verdict == "block"
    behavior = next(check for check in wrong.checks if check.check_id == "behavioral-improvement")
    detail = json.loads(behavior.detail)
    assert detail["reason"] == "wrong-answer"
    assert detail["evidence"][0]["candidate_digest"] == candidate.revision_digest
    assert detail["evidence"][0]["task_digest"] == digest_json({"task": "held-out-task"})

    uncertain = evaluate_revision(
        candidate,
        behavior_evidence=(
            _evidence(candidate.revision_digest or "", uncertainty=("ambiguous rubric",)),
        ),
    )
    assert uncertain.structural_verdict == "accept"
    assert uncertain.verdict == "block"
    uncertain_behavior = next(
        check for check in uncertain.checks if check.check_id == "behavioral-improvement"
    )
    assert json.loads(uncertain_behavior.detail)["reason"] == "unresolved-uncertainty"

    accepted = evaluate_revision(
        candidate,
        behavior_evidence=(_evidence(candidate.revision_digest or ""),),
    )
    assert accepted.structural_verdict == "accept"
    assert accepted.verdict == "accept"


def test_learned_activation_blocks_wrong_held_out_answer_without_changing_direct_path(
    repository: SkillRepository,
) -> None:
    current_goal = goal()
    service = SkillTurnService(repository)
    service.select_and_pin(
        request(1, current_goal.digest or ""),
        episode_id="episode-r5",
        goal=current_goal,
    )
    episode = repository.get_episode("episode-r5")
    assert episode is not None
    lesson = _lesson()
    candidate = reflect_lesson(lesson, episode=episode, active=None)
    attribution = OutcomeAttribution(
        ATTRIBUTION_SCHEMA,
        "episode-r5",
        "wrong-how",
        ("how-procedure",),
        ("evidence-lesson",),
        "verifier-test",
    )

    with pytest.raises(ContractError, match="evaluator blocked activation"):
        learn_episode(
            repository,
            episode_id="episode-r5",
            outcome="verified-failure",
            evidence_refs=("evidence-lesson",),
            attribution=attribution,
            lesson=lesson,
            reflect=True,
            evaluate=True,
            activate=True,
            held_out_evidence=(_evidence(candidate.revision_digest or "", observed="wrong"),),
        )

    assert repository.get_active(candidate.skill_name) is None


def test_direct_operator_activation_keeps_structural_route(repository: SkillRepository) -> None:
    candidate = revision()
    repository.register_revision(candidate)
    report = evaluate_revision(
        candidate,
        active_revisions=repository.active_revisions(),
        materialized_revisions=repository.list_revisions(),
    )
    assert report.structural_verdict == "accept"
    assert report.verdict == "accept"
    repository.save_evaluation(report)

    receipt = repository.activate(
        skill_name=candidate.skill_name,
        candidate_digest=candidate.revision_digest or "",
        expected_active_digest=None,
        evaluation_digest=report.evaluation_digest or "",
        input_digest=report.input_digest,
        operation_key="operator-direct-r5",
    )
    assert receipt["candidate_digest"] == candidate.revision_digest
    assert repository.get_active(candidate.skill_name) == candidate


def test_guard_allows_benign_security_guidance_and_url_but_blocks_an_effect() -> None:
    guard_text(
        "Security guidance: do not transmit credentials; use https://docs.example.invalid only."
    )
    with pytest.raises(GuardViolation, match="external-effect"):
        guard_text("Send the report to https://example.invalid.")


@pytest.mark.parametrize("variant", ["missing", "wrong-candidate", "not-held-out"])
def test_unqualified_behavior_evidence_is_not_acceptance(variant):
    from dataclasses import replace

    candidate = revision()
    evidence = _evidence(candidate.revision_digest)
    if variant == "missing":
        records = ()
    elif variant == "wrong-candidate":
        records = (replace(evidence, candidate_digest="0" * 64),)
    else:
        records = (replace(evidence, held_out=False),)
    report = evaluate_revision(candidate, behavior_evidence=records)
    assert report.structural_verdict == "accept" and report.verdict == "block"
