"""Strict explicit direction-decision receipts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from .sessions import ContractError
from .skill_values import (
    digest,
    digest_json,
    exact_mapping,
    identifier,
    optional_digest,
    schema,
    text,
)


def _finish(payload: dict[str, object], supplied: object, label: str) -> str:
    actual = digest_json(payload)
    if supplied is not None and digest(supplied, label) != actual:
        raise ContractError(f"{label} does not match canonical content")
    return actual


DECISION_SCHEMA = "ahc.direction-decision.v1"
DECISIONS = (
    "accept-current",
    "accept-alternative",
    "revise-goal",
    "narrow-scope",
    "stop",
    "defer",
    "policy-refused",
)


@dataclass(frozen=True, slots=True)
class DirectionDecisionReceipt:
    schema_version: str
    operation_key: str
    assessment_digest: str
    prior_goal_digest: str
    decision: str
    owner_kind: str
    owner_id: str
    resulting_goal_digest: str | None
    order: int
    rationale: str
    digest: str | None = None

    def __post_init__(self) -> None:
        schema(self.schema_version, DECISION_SCHEMA, "direction decision")
        identifier(self.operation_key, "operation_key")
        digest(self.assessment_digest, "assessment_digest")
        digest(self.prior_goal_digest, "prior_goal_digest")
        if self.decision not in DECISIONS:
            raise ContractError("direction decision is unsupported")
        if self.owner_kind not in ("user", "policy"):
            raise ContractError("decision owner_kind must be user or policy")
        identifier(self.owner_id, "owner_id")
        if type(self.order) is not int or self.order < 1:
            raise ContractError("decision order must be positive")
        rationale = text(self.rationale, "rationale", 2_048, multiline=True)
        resulting = optional_digest(self.resulting_goal_digest, "resulting_goal_digest")
        material = self.decision in ("accept-alternative", "revise-goal", "narrow-scope")
        if material and resulting is None:
            raise ContractError("material direction decisions require a successor goal")
        if material and resulting == self.prior_goal_digest:
            raise ContractError("material direction decision must bind a distinct successor goal")
        if self.decision == "accept-current" and resulting != self.prior_goal_digest:
            raise ContractError("accept-current must bind the prior goal")
        if self.decision in ("stop", "defer", "policy-refused") and resulting is not None:
            raise ContractError("stop, defer, and policy-refused cannot select a resulting goal")
        if self.decision == "policy-refused" and self.owner_kind != "policy":
            raise ContractError("policy-refused requires policy ownership")
        payload = {
            "schema_version": self.schema_version,
            "operation_key": self.operation_key,
            "assessment_digest": self.assessment_digest,
            "prior_goal_digest": self.prior_goal_digest,
            "decision": self.decision,
            "owner_kind": self.owner_kind,
            "owner_id": self.owner_id,
            "resulting_goal_digest": resulting,
            "order": self.order,
            "rationale": rationale,
        }
        actual = _finish(payload, self.digest, "decision digest")
        object.__setattr__(self, "resulting_goal_digest", resulting)
        object.__setattr__(self, "rationale", rationale)
        object.__setattr__(self, "digest", actual)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "operation_key": self.operation_key,
            "assessment_digest": self.assessment_digest,
            "prior_goal_digest": self.prior_goal_digest,
            "decision": self.decision,
            "owner_kind": self.owner_kind,
            "owner_id": self.owner_id,
            "resulting_goal_digest": self.resulting_goal_digest,
            "order": self.order,
            "rationale": self.rationale,
            "digest": self.digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> DirectionDecisionReceipt:
        keys = frozenset(
            {
                "schema_version",
                "operation_key",
                "assessment_digest",
                "prior_goal_digest",
                "decision",
                "owner_kind",
                "owner_id",
                "resulting_goal_digest",
                "order",
                "rationale",
                "digest",
            }
        )
        raw = exact_mapping(value, keys, "direction decision")
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["operation_key"]),
            cast(str, raw["assessment_digest"]),
            cast(str, raw["prior_goal_digest"]),
            cast(str, raw["decision"]),
            cast(str, raw["owner_kind"]),
            cast(str, raw["owner_id"]),
            cast(str | None, raw["resulting_goal_digest"]),
            cast(int, raw["order"]),
            cast(str, raw["rationale"]),
            cast(str, raw["digest"]),
        )
