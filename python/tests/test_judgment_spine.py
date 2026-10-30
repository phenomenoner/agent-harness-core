"""Discriminating Why-What-How judgment and attribution tests."""

from __future__ import annotations

from dataclasses import replace

import pytest
from skill_test_support import goal, request

from ahc.application.skill_learning import learn_episode
from ahc.application.skill_service import SkillTurnService
from ahc.domain import (
    DirectionDecisionReceipt,
    DirectionSignal,
    IntentFrame,
    JudgmentAssessment,
    OutcomeAttribution,
    Premise,
)
from ahc.domain.judgment_models import INTENT_SCHEMA, SIGNAL_SCHEMA
from ahc.domain.judgment_records import ASSESSMENT_SCHEMA, ATTRIBUTION_SCHEMA, DECISION_SCHEMA
from ahc.domain.judgment_routing import DirectionGateError, gate_how_selection, validate_decision
from ahc.domain.sessions import ContractError
from ahc.infrastructure.skill_doctor import doctor, inventory
from ahc.infrastructure.skill_repository import SkillRepository


def _intent() -> IntentFrame:
    current = request(1, goal().digest or "")
    return IntentFrame(
        INTENT_SCHEMA,
        current.session,
        current.turn_id,
        "Keep improving the seam-heavy Rust implementation",
        "Reach a complete experimental harness",
        ("Preserve evidence",),
        ("No hidden pivot",),
        (
            Premise("The seam is still necessary", "assumption", None),
            Premise("The user asked for a direct application", "user-asserted", "request"),
        ),
        ("A simpler path may exist",),
        ("No authority mutation",),
    )


def _signal() -> DirectionSignal:
    return DirectionSignal(
        SIGNAL_SCHEMA,
        "simpler-path-available",
        1,
        1,
        True,
        ("python-standalone-comparison",),
    )


def _assessment(
    intent: IntentFrame, disposition: str = "CHALLENGE_AND_DISCUSS"
) -> JudgmentAssessment:
    return JudgmentAssessment(
        ASSESSMENT_SCHEMA,
        "assessment-1",
        "deterministic-evaluator",
        intent.digest or "",
        goal().digest or "",
        disposition,
        ("The necessity premise is unsupported",),
        ("A direct standalone path is simpler",),
        ("Discuss the Python standalone alternative",),
        ("python-standalone-comparison",),
        9000,
        disposition in ("CLARIFY", "CHALLENGE_AND_DISCUSS", "PARK"),
    )


def test_intent_preserves_provenance_and_rejects_unknown_fields() -> None:
    intent = _intent()
    assert tuple(item.provenance for item in intent.premises) == (
        "assumption",
        "user-asserted",
    )
    assert IntentFrame.from_dict(intent.to_dict()) == intent
    malformed = intent.to_dict()
    malformed["hidden"] = "pivot"
    with pytest.raises(ContractError, match="unknown keys"):
        IntentFrame.from_dict(malformed)


def test_fired_signal_blocks_how_without_assessment_even_when_intent_omitted(
    repository: SkillRepository,
) -> None:
    with pytest.raises(DirectionGateError, match="requires assessment"):
        SkillTurnService(repository).select_and_pin(
            request(1, goal().digest or ""),
            episode_id="episode-signal",
            goal=goal(),
            signals=(_signal(),),
        )
    assert repository.status()["counts"]["manifests"] == 0


def test_visible_challenge_and_user_accept_current_receipt_authorize_current_goal(
    repository: SkillRepository,
) -> None:
    intent = _intent()
    assessment = _assessment(intent)
    receipt = DirectionDecisionReceipt(
        DECISION_SCHEMA,
        "decision-1",
        assessment.digest or "",
        goal().digest or "",
        "accept-current",
        "user",
        "user-test",
        goal().digest,
        1,
        "Retain the current bounded direction after discussion",
    )
    gate = gate_how_selection(goal(), (_signal(),), assessment, receipt)
    assert gate.allowed is True
    result = SkillTurnService(repository).select_and_pin(
        request(1, goal().digest or ""),
        episode_id="episode-override",
        intent=intent,
        goal=goal(),
        signals=(_signal(),),
        assessment=assessment,
        decision=receipt,
    )
    assert result["manifest"].entries == ()


def test_material_direction_change_requires_exact_successor_goal() -> None:
    intent = _intent()
    assessment = _assessment(intent)
    receipt = DirectionDecisionReceipt(
        DECISION_SCHEMA,
        "decision-material",
        assessment.digest or "",
        goal().digest or "",
        "revise-goal",
        "user",
        "user-test",
        "1" * 64,
        1,
        "Use the direct path",
    )
    with pytest.raises(DirectionGateError, match="successor goal"):
        validate_decision(assessment, goal(), receipt, None)


def test_wrong_why_cannot_launder_into_how_revision(repository: SkillRepository) -> None:
    service = SkillTurnService(repository)
    current_goal = goal()
    service.select_and_pin(
        request(1, current_goal.digest or ""), episode_id="episode-layer", goal=current_goal
    )
    attribution = OutcomeAttribution(
        ATTRIBUTION_SCHEMA,
        "episode-layer",
        "wrong-why",
        ("why-judgment",),
        ("evidence-direction",),
        "verifier-test",
    )
    from test_skill_active_loop import _lesson

    with pytest.raises(DirectionGateError, match="cognitive layer"):
        learn_episode(
            repository,
            episode_id="episode-layer",
            outcome="verified-failure",
            evidence_refs=("evidence-direction",),
            attribution=attribution,
            lesson=_lesson(),
            reflect=True,
        )
    assert repository.status()["counts"]["revisions"] == 0


def test_unacknowledged_goal_and_wrong_policy_owner_remain_blocked() -> None:
    gate = gate_how_selection(goal(acknowledgement="proposed"), (), None, None)
    assert gate.allowed is False
    assessment = _assessment(_intent())
    with pytest.raises(ContractError, match="policy ownership"):
        DirectionDecisionReceipt(
            DECISION_SCHEMA,
            "policy-decision",
            assessment.digest or "",
            goal().digest or "",
            "policy-refused",
            "user",
            "user-test",
            None,
            1,
            "Claimed policy refusal",
        )


def test_doctor_reports_hidden_successor_goal_without_mutation(
    repository: SkillRepository,
) -> None:
    prior = goal()
    successor = replace(
        prior,
        revision=2,
        predecessor_digest=prior.digest,
        what="Silently substitute another target",
        digest=None,
    )
    repository.save_judgment_record(prior)
    repository.save_judgment_record(successor)
    before = inventory(repository.root)
    report = doctor(repository.root)
    assert report["ok"] is False
    assert f"goal-missing-decision:{successor.digest}" in report["issues"]
    assert inventory(repository.root) == before
