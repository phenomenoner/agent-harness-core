"""Compatibility exports for strict judgment records."""

from .judgment_assessment import ASSESSMENT_SCHEMA, DISPOSITIONS, JudgmentAssessment
from .judgment_attribution import ATTRIBUTION_SCHEMA, ATTRIBUTIONS, OutcomeAttribution
from .judgment_decision import DECISION_SCHEMA, DECISIONS, DirectionDecisionReceipt

__all__ = [
    "ASSESSMENT_SCHEMA",
    "ATTRIBUTIONS",
    "ATTRIBUTION_SCHEMA",
    "DECISIONS",
    "DECISION_SCHEMA",
    "DISPOSITIONS",
    "DirectionDecisionReceipt",
    "JudgmentAssessment",
    "OutcomeAttribution",
]
