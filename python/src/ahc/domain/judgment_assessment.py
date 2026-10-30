"""Strict independent judgment-assessment records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from .sessions import ContractError
from .skill_values import (
    digest,
    digest_json,
    exact_mapping,
    identifier,
    ordered_text_items,
    schema,
    text,
)


def _finish(payload: dict[str, object], supplied: object, label: str) -> str:
    actual = digest_json(payload)
    if supplied is not None and digest(supplied, label) != actual:
        raise ContractError(f"{label} does not match canonical content")
    return actual


ASSESSMENT_SCHEMA = "ahc.judgment-assessment.v1"
DISPOSITIONS = (
    "PROCEED",
    "PROCEED_WITH_ASSUMPTIONS",
    "CLARIFY",
    "CHALLENGE_AND_DISCUSS",
    "REFUSE_BY_POLICY",
    "PARK",
)


def _refs(value: object, label: str) -> tuple[str, ...]:
    return ordered_text_items(value, label, 0, 32)


@dataclass(frozen=True, slots=True)
class JudgmentAssessment:
    schema_version: str
    assessment_id: str
    evaluator_id: str
    intent_digest: str
    goal_digest: str
    disposition: str
    why_findings: tuple[str, ...]
    what_findings: tuple[str, ...]
    alternatives: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    confidence_bps: int
    requires_user_decision: bool
    policy_reference: str | None = None
    digest: str | None = None

    def __post_init__(self) -> None:
        schema(self.schema_version, ASSESSMENT_SCHEMA, "judgment assessment")
        identifier(self.assessment_id, "assessment_id")
        identifier(self.evaluator_id, "evaluator_id")
        digest(self.intent_digest, "intent_digest")
        digest(self.goal_digest, "goal_digest")
        if self.disposition not in DISPOSITIONS:
            raise ContractError("assessment disposition is unsupported")
        why, what, alternatives = (
            ordered_text_items(self.why_findings, "why_findings", 0, 32),
            ordered_text_items(self.what_findings, "what_findings", 0, 32),
            ordered_text_items(self.alternatives, "alternatives", 0, 32),
        )
        refs = _refs(self.evidence_refs, "assessment evidence_refs")
        if type(self.confidence_bps) is not int or not 0 <= self.confidence_bps <= 10_000:
            raise ContractError("confidence_bps must be an integer in 0..10000")
        if type(self.requires_user_decision) is not bool:
            raise ContractError("requires_user_decision must be boolean")
        if self.disposition == "REFUSE_BY_POLICY" and self.policy_reference is None:
            raise ContractError("policy refusal requires an existing policy reference")
        if self.disposition != "REFUSE_BY_POLICY" and self.policy_reference is not None:
            raise ContractError("only policy refusal may carry a policy reference")
        needs = self.disposition in ("CLARIFY", "CHALLENGE_AND_DISCUSS", "PARK")
        if self.requires_user_decision != needs:
            raise ContractError("requires_user_decision does not match disposition")
        policy = (
            None
            if self.policy_reference is None
            else text(self.policy_reference, "policy_reference", 512)
        )
        payload = {
            "schema_version": self.schema_version,
            "assessment_id": self.assessment_id,
            "evaluator_id": self.evaluator_id,
            "intent_digest": self.intent_digest,
            "goal_digest": self.goal_digest,
            "disposition": self.disposition,
            "why_findings": list(why),
            "what_findings": list(what),
            "alternatives": list(alternatives),
            "evidence_refs": list(refs),
            "confidence_bps": self.confidence_bps,
            "requires_user_decision": self.requires_user_decision,
            "policy_reference": policy,
        }
        actual = _finish(payload, self.digest, "assessment digest")
        for name, value in (
            ("why_findings", why),
            ("what_findings", what),
            ("alternatives", alternatives),
            ("evidence_refs", refs),
            ("policy_reference", policy),
            ("digest", actual),
        ):
            object.__setattr__(self, name, value)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "assessment_id": self.assessment_id,
            "evaluator_id": self.evaluator_id,
            "intent_digest": self.intent_digest,
            "goal_digest": self.goal_digest,
            "disposition": self.disposition,
            "why_findings": list(self.why_findings),
            "what_findings": list(self.what_findings),
            "alternatives": list(self.alternatives),
            "evidence_refs": list(self.evidence_refs),
            "confidence_bps": self.confidence_bps,
            "requires_user_decision": self.requires_user_decision,
            "policy_reference": self.policy_reference,
            "digest": self.digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> JudgmentAssessment:
        keys = frozenset(
            {
                "schema_version",
                "assessment_id",
                "evaluator_id",
                "intent_digest",
                "goal_digest",
                "disposition",
                "why_findings",
                "what_findings",
                "alternatives",
                "evidence_refs",
                "confidence_bps",
                "requires_user_decision",
                "policy_reference",
                "digest",
            }
        )
        raw = exact_mapping(value, keys, "judgment assessment")
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["assessment_id"]),
            cast(str, raw["evaluator_id"]),
            cast(str, raw["intent_digest"]),
            cast(str, raw["goal_digest"]),
            cast(str, raw["disposition"]),
            cast(tuple[str, ...], raw["why_findings"]),
            cast(tuple[str, ...], raw["what_findings"]),
            cast(tuple[str, ...], raw["alternatives"]),
            cast(tuple[str, ...], raw["evidence_refs"]),
            cast(int, raw["confidence_bps"]),
            cast(bool, raw["requires_user_decision"]),
            cast(str | None, raw["policy_reference"]),
            cast(str, raw["digest"]),
        )
