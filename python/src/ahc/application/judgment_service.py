"""Application commands for explicit Why-What-How direction evidence."""

from __future__ import annotations

from ahc.domain.judgment_models import DirectionSignal, GoalContract, IntentFrame
from ahc.domain.judgment_records import (
    DirectionDecisionReceipt,
    JudgmentAssessment,
    OutcomeAttribution,
)
from ahc.domain.judgment_routing import (
    DirectionGate,
    gate_how_selection,
    validate_assessment,
    validate_decision,
)
from ahc.domain.sessions import ContractError
from ahc.infrastructure.skill_repository import SkillRepository


class JudgmentService:
    """Thin application adapter; semantic proposals remain explicit inputs."""

    def __init__(self, repository: SkillRepository) -> None:
        self.repository = repository

    def assess(
        self,
        intent: IntentFrame,
        goal: GoalContract,
        signals: tuple[DirectionSignal, ...],
        assessment: JudgmentAssessment,
    ) -> JudgmentAssessment:
        validate_assessment(intent, goal, signals, assessment)
        if intent.session.ref.virtual_session_id != intent.session.ref.virtual_session_id:
            raise ContractError("intent session identity mismatch")
        self.repository.save_judgment_record(intent)
        self.repository.save_judgment_record(goal)
        for signal in signals:
            self.repository.save_judgment_record(signal) if hasattr(signal, "to_dict") else None
        self.repository.save_judgment_record(assessment)
        return assessment

    def decide(
        self,
        assessment: JudgmentAssessment,
        prior_goal: GoalContract,
        receipt: DirectionDecisionReceipt,
        successor_goal: GoalContract | None = None,
    ) -> DirectionDecisionReceipt:
        validate_decision(assessment, prior_goal, receipt, successor_goal)
        self.repository.save_judgment_record(assessment)
        self.repository.save_judgment_record(prior_goal)
        if successor_goal is not None:
            self.repository.save_judgment_record(successor_goal)
        self.repository.save_judgment_record(receipt)
        return receipt

    def gate(
        self,
        goal: GoalContract,
        signals: tuple[DirectionSignal, ...],
        assessment: JudgmentAssessment | None,
        receipt: DirectionDecisionReceipt | None,
    ) -> DirectionGate:
        return gate_how_selection(goal, signals, assessment, receipt)

    def record_attribution(self, attribution: OutcomeAttribution) -> str:
        return self.repository.save_judgment_record(attribution)


__all__ = ["JudgmentService"]
