"""Pure gates for direction discussion and layer-correct learning."""

from __future__ import annotations

from dataclasses import dataclass

from .judgment_models import DirectionSignal, GoalContract, IntentFrame
from .judgment_records import DirectionDecisionReceipt, JudgmentAssessment, OutcomeAttribution
from .sessions import ContractError
from .skill_values import COGNITIVE_LAYERS


class DirectionGateError(ContractError):
    """A direction or attribution prerequisite is not satisfied."""


@dataclass(frozen=True, slots=True)
class DirectionGate:
    allowed: bool
    reason: str
    fired_signal_digests: tuple[str, ...]
    current_goal_digest: str


def fired_signals(
    signals: tuple[DirectionSignal, ...] | list[DirectionSignal],
) -> tuple[DirectionSignal, ...]:
    values = tuple(signals)
    if any(type(signal) is not DirectionSignal for signal in values):
        raise DirectionGateError("signals must contain DirectionSignal values")
    if len({signal.digest for signal in values}) != len(values):
        raise DirectionGateError("signals must not repeat a digest")
    return tuple(signal for signal in values if signal.fired)


def validate_assessment(
    intent: IntentFrame,
    goal: GoalContract,
    signals: tuple[DirectionSignal, ...],
    assessment: JudgmentAssessment,
) -> None:
    if assessment.intent_digest != intent.digest:
        raise DirectionGateError("assessment intent digest is stale")
    if assessment.goal_digest != goal.digest:
        raise DirectionGateError("assessment goal digest is stale")
    if not isinstance(signals, tuple):
        signals = tuple(signals)
    fired_signals(signals)
    if assessment.disposition == "REFUSE_BY_POLICY" and not assessment.policy_reference:
        raise DirectionGateError("policy refusal lacks an existing policy reference")


def validate_decision(
    assessment: JudgmentAssessment,
    prior_goal: GoalContract,
    receipt: DirectionDecisionReceipt,
    successor_goal: GoalContract | None = None,
) -> None:
    if receipt.assessment_digest != assessment.digest:
        raise DirectionGateError("decision receipt assessment digest is stale")
    if receipt.prior_goal_digest != prior_goal.digest:
        raise DirectionGateError("decision receipt prior goal digest is stale")
    if receipt.decision == "accept-current" and receipt.owner_kind != "user":
        raise DirectionGateError("accept-current is a user decision")
    material = receipt.decision in ("accept-alternative", "revise-goal", "narrow-scope")
    if material:
        if successor_goal is None:
            raise DirectionGateError("material decision requires a successor goal")
        if receipt.resulting_goal_digest != successor_goal.digest:
            raise DirectionGateError("decision does not bind the supplied successor goal")
        if successor_goal.predecessor_digest != prior_goal.digest:
            raise DirectionGateError("successor goal does not bind the prior goal")
        if successor_goal.revision != prior_goal.revision + 1:
            raise DirectionGateError("successor goal revision is stale")
    elif successor_goal is not None:
        raise DirectionGateError("non-material decision cannot carry a successor goal")


def gate_how_selection(
    goal: GoalContract,
    signals: tuple[DirectionSignal, ...],
    assessment: JudgmentAssessment | None,
    receipt: DirectionDecisionReceipt | None,
) -> DirectionGate:
    fired = fired_signals(signals)
    digests = tuple(signal.digest for signal in fired)
    if goal.acknowledgement != "acknowledged":
        return DirectionGate(False, "goal is not acknowledged", digests, goal.digest)
    if assessment is None:
        if fired:
            return DirectionGate(
                False, "fired direction signal requires assessment", digests, goal.digest
            )
        return DirectionGate(True, "low-risk direction may proceed", digests, goal.digest)
    if assessment.goal_digest != goal.digest:
        return DirectionGate(
            False, "assessment is bound to a superseded goal", digests, goal.digest
        )
    if assessment.disposition == "REFUSE_BY_POLICY":
        return DirectionGate(False, "policy refusal forbids HOW selection", digests, goal.digest)
    if assessment.disposition in ("PROCEED", "PROCEED_WITH_ASSUMPTIONS"):
        return DirectionGate(True, "assessment permits procedure", digests, goal.digest)
    if receipt is None:
        return DirectionGate(
            False, "material direction discussion has no decision receipt", digests, goal.digest
        )
    if receipt.assessment_digest != assessment.digest or receipt.prior_goal_digest != goal.digest:
        return DirectionGate(False, "decision receipt is stale", digests, goal.digest)
    if receipt.decision == "accept-current":
        return DirectionGate(
            True, "user explicitly retained the current direction", digests, goal.digest
        )
    return DirectionGate(False, "decision does not authorize HOW selection", digests, goal.digest)


def needs_direction_assessment(
    intent: IntentFrame, goal: GoalContract, signals: tuple[DirectionSignal, ...]
) -> bool:
    if goal.acknowledgement != "acknowledged":
        return True
    if fired_signals(signals):
        return True
    return bool(intent.uncertainty_notes)


def allowed_learning_layer(attribution: OutcomeAttribution, candidate_layer: str) -> bool:
    if candidate_layer not in COGNITIVE_LAYERS:
        raise DirectionGateError("candidate layer is unsupported")
    if attribution.primary_label in ("unknown", "insufficient-evidence"):
        return False
    if attribution.primary_label == "wrong-why":
        return candidate_layer == "why-judgment"
    if attribution.primary_label == "wrong-what":
        return candidate_layer == "what-strategy"
    if attribution.primary_label in ("wrong-how", "execution-slip"):
        return candidate_layer == "how-procedure"
    if attribution.primary_label == "environment-drift":
        return candidate_layer == "how-procedure" and "how-procedure" in attribution.affected_layers
    if attribution.primary_label == "mixed":
        return candidate_layer in attribution.affected_layers
    return False


def require_learning_layer(attribution: OutcomeAttribution, candidate_layer: str) -> None:
    if not allowed_learning_layer(attribution, candidate_layer):
        raise DirectionGateError("attribution cannot authorize a revision in this cognitive layer")
