"""Regressions for the first formal Judgment blocker batch."""

from __future__ import annotations

from dataclasses import replace

import pytest
from skill_test_support import goal, request
from test_judgment_spine import _assessment, _intent, _signal

from ahc.application.skill_service import SkillTurnService
from ahc.domain import DirectionDecisionReceipt, JudgmentAssessment, OutcomeAttribution
from ahc.domain.judgment_records import ASSESSMENT_SCHEMA, ATTRIBUTION_SCHEMA, DECISION_SCHEMA
from ahc.domain.judgment_routing import (
    DirectionGateError,
    allowed_learning_layer,
    gate_how_selection,
)
from ahc.infrastructure.skill_repository import SkillRepository


def test_policy_refusal_cannot_be_overridden_by_user_accept_current() -> None:
    intent = _intent()
    refused = JudgmentAssessment(
        ASSESSMENT_SCHEMA,
        "assessment-policy",
        "policy-evaluator",
        intent.digest or "",
        goal().digest or "",
        "REFUSE_BY_POLICY",
        ("Existing policy forbids the effect",),
        (),
        (),
        ("policy-evidence",),
        10_000,
        False,
        "policy://runtime/effects",
    )
    override = DirectionDecisionReceipt(
        DECISION_SCHEMA,
        "decision-policy-override",
        refused.digest or "",
        goal().digest or "",
        "accept-current",
        "user",
        "user-test",
        goal().digest,
        1,
        "Attempt to retain the task despite policy",
    )
    gate = gate_how_selection(goal(), (), refused, override)
    assert gate.allowed is False
    assert "policy" in gate.reason


def test_uncertainty_notes_require_assessment_before_how_selection(
    repository: SkillRepository,
) -> None:
    intent = _intent()
    assert intent.uncertainty_notes
    with pytest.raises(DirectionGateError, match="uncertainty"):
        SkillTurnService(repository).select_and_pin(
            request(1, goal().digest or ""),
            episode_id="episode-uncertain",
            intent=intent,
            goal=goal(),
        )
    assert repository.status()["counts"]["manifests"] == 0


def test_environment_drift_can_bind_only_eligible_how_learning() -> None:
    attribution = OutcomeAttribution(
        ATTRIBUTION_SCHEMA,
        "episode-environment",
        "environment-drift",
        ("how-procedure",),
        ("runtime-version-evidence",),
        "independent-verifier",
    )
    assert allowed_learning_layer(attribution, "how-procedure") is True
    assert allowed_learning_layer(attribution, "why-judgment") is False


def test_challenge_accept_current_behavior_is_unchanged() -> None:
    intent = _intent()
    assessment = _assessment(intent)
    receipt = DirectionDecisionReceipt(
        DECISION_SCHEMA,
        "decision-current",
        assessment.digest or "",
        goal().digest or "",
        "accept-current",
        "user",
        "user-test",
        goal().digest,
        1,
        "Retain current direction after a non-policy challenge",
    )
    assert gate_how_selection(goal(), (), assessment, receipt).allowed is True
    assert replace(assessment, digest=None).digest == assessment.digest


def test_skill_turn_rejects_policy_owned_accept_current_receipt(
    repository: SkillRepository,
) -> None:
    intent = _intent()
    assessment = _assessment(intent)
    receipt = DirectionDecisionReceipt(
        DECISION_SCHEMA,
        "decision-policy-owner",
        assessment.digest or "",
        goal().digest or "",
        "accept-current",
        "policy",
        "policy-engine",
        goal().digest,
        1,
        "Policy cannot own a user direction acceptance",
    )
    with pytest.raises(DirectionGateError, match="user decision"):
        SkillTurnService(repository).select_and_pin(
            request(1, goal().digest or ""),
            episode_id="episode-policy-owner",
            intent=intent,
            goal=goal(),
            signals=(_signal(),),
            assessment=assessment,
            decision=receipt,
        )
    assert repository.status()["counts"]["manifests"] == 0
