"""Bounded held-out behavior evidence for learned skill revisions."""

from __future__ import annotations

from dataclasses import dataclass

from .sessions import ContractError
from .skill_values import canonical_json, digest


@dataclass(frozen=True, slots=True)
class HeldOutTaskEvidence:
    """One candidate-bound task result, retained without task or answer contents."""

    candidate_digest: str
    task_digest: str
    expected_answer_digest: str
    observed_answer_digest: str
    evidence_refs: tuple[str, ...]
    uncertainty: tuple[str, ...] = ()
    held_out: bool = True

    def __post_init__(self) -> None:
        for label, value in (
            ("candidate_digest", self.candidate_digest),
            ("task_digest", self.task_digest),
            ("expected_answer_digest", self.expected_answer_digest),
            ("observed_answer_digest", self.observed_answer_digest),
        ):
            digest(value, label)
        if not isinstance(self.evidence_refs, tuple) or not self.evidence_refs:
            raise ContractError("held-out evidence requires evidence references")
        if any(type(item) is not str or not item or len(item) > 256 for item in self.evidence_refs):
            raise ContractError("held-out evidence references are invalid")
        if not isinstance(self.uncertainty, tuple) or any(
            type(item) is not str or not item or len(item) > 256 for item in self.uncertainty
        ):
            raise ContractError("held-out evidence uncertainty is invalid")
        if type(self.held_out) is not bool:
            raise ContractError("held_out must be boolean")

    @classmethod
    def from_dict(cls, value):
        fields = {
            "candidate_digest",
            "task_digest",
            "expected_answer_digest",
            "observed_answer_digest",
            "evidence_refs",
            "uncertainty",
            "held_out",
        }
        if type(value) is not dict or set(value) != fields:
            raise ContractError("invalid held-out evidence fields")
        if any(type(value[name]) is not list for name in ("evidence_refs", "uncertainty")):
            raise ContractError("invalid held-out evidence lists")
        return cls(
            value["candidate_digest"],
            value["task_digest"],
            value["expected_answer_digest"],
            value["observed_answer_digest"],
            tuple(value["evidence_refs"]),
            tuple(value["uncertainty"]),
            value["held_out"],
        )

    @property
    def correct(self) -> bool:
        return self.observed_answer_digest == self.expected_answer_digest

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_digest": self.candidate_digest,
            "task_digest": self.task_digest,
            "expected_answer_digest": self.expected_answer_digest,
            "observed_answer_digest": self.observed_answer_digest,
            "evidence_refs": list(self.evidence_refs),
            "uncertainty": list(self.uncertainty),
            "held_out": self.held_out,
        }


@dataclass(frozen=True, slots=True)
class BehavioralAssessment:
    accepted: bool
    reason: str
    evidence: tuple[HeldOutTaskEvidence, ...]

    def detail(self) -> str:
        """Use the existing evaluation-check detail field as the durable record."""
        return canonical_json(
            {
                "accepted": self.accepted,
                "reason": self.reason,
                "evidence": [item.to_dict() for item in self.evidence],
            }
        ).decode("utf-8")


def assess_behavioral_improvement(
    candidate_digest: str, evidence: tuple[HeldOutTaskEvidence, ...] | list[HeldOutTaskEvidence]
) -> BehavioralAssessment:
    """Accept only resolved, correct, held-out results bound to this candidate."""
    digest(candidate_digest, "candidate_digest")
    if type(evidence) not in (tuple, list) or len(evidence) > 32:
        raise ContractError("behavior evidence exceeds bound")
    records = tuple(evidence)
    if not records:
        return BehavioralAssessment(False, "missing-held-out-evidence", records)
    if any(type(item) is not HeldOutTaskEvidence for item in records):
        raise ContractError("behavior evidence must contain HeldOutTaskEvidence values")
    if any(item.candidate_digest != candidate_digest for item in records):
        return BehavioralAssessment(False, "candidate-binding-mismatch", records)
    if any(not item.held_out for item in records):
        return BehavioralAssessment(False, "not-held-out", records)
    if any(item.uncertainty for item in records):
        return BehavioralAssessment(False, "unresolved-uncertainty", records)
    if any(not item.correct for item in records):
        return BehavioralAssessment(False, "wrong-answer", records)
    return BehavioralAssessment(True, "held-out-performance-accepted", records)


__all__ = ["BehavioralAssessment", "HeldOutTaskEvidence", "assess_behavioral_improvement"]
