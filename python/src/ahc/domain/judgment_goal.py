"""Strict goal-contract values."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from .sessions import ContractError
from .skill_values import (
    bounded_text_items,
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


GOAL_SCHEMA = "ahc.goal-contract.v1"


def _items(value: object, label: str) -> tuple[str, ...]:
    return bounded_text_items(value, label, 32)


@dataclass(frozen=True, slots=True)
class GoalContract:
    schema_version: str
    goal_id: str
    revision: int
    predecessor_digest: str | None
    why: str
    what: str
    success_criteria: tuple[str, ...]
    in_scope: tuple[str, ...]
    non_goals: tuple[str, ...]
    authority_boundaries: tuple[str, ...]
    assumptions: tuple[str, ...]
    acknowledgement: str
    digest: str | None = None

    def __post_init__(self) -> None:
        schema(self.schema_version, GOAL_SCHEMA, "goal contract")
        identifier(self.goal_id, "goal_id")
        if type(self.revision) is not int or self.revision < 1 or isinstance(self.revision, bool):
            raise ContractError("goal revision must be a positive integer")
        predecessor = optional_digest(self.predecessor_digest, "predecessor_digest")
        if (self.revision > 1) != (predecessor is not None):
            raise ContractError("predecessor_digest is required exactly for successor goals")
        why, what = (
            text(self.why, "why", 2_048, multiline=True),
            text(self.what, "what", 2_048, multiline=True),
        )
        criteria, scope = (
            _items(self.success_criteria, "success_criteria"),
            _items(self.in_scope, "in_scope"),
        )
        non_goals, boundaries, assumptions = (
            _items(self.non_goals, "non_goals"),
            _items(self.authority_boundaries, "authority_boundaries"),
            _items(self.assumptions, "assumptions"),
        )
        if self.acknowledgement not in ("acknowledged", "proposed"):
            raise ContractError("acknowledgement must be acknowledged or proposed")
        payload = {
            "schema_version": self.schema_version,
            "goal_id": self.goal_id,
            "revision": self.revision,
            "predecessor_digest": predecessor,
            "why": why,
            "what": what,
            "success_criteria": list(criteria),
            "in_scope": list(scope),
            "non_goals": list(non_goals),
            "authority_boundaries": list(boundaries),
            "assumptions": list(assumptions),
            "acknowledgement": self.acknowledgement,
        }
        actual = _finish(payload, self.digest, "goal digest")
        for name, value in (
            ("predecessor_digest", predecessor),
            ("why", why),
            ("what", what),
            ("success_criteria", criteria),
            ("in_scope", scope),
            ("non_goals", non_goals),
            ("authority_boundaries", boundaries),
            ("assumptions", assumptions),
            ("digest", actual),
        ):
            object.__setattr__(self, name, value)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "goal_id": self.goal_id,
            "revision": self.revision,
            "predecessor_digest": self.predecessor_digest,
            "why": self.why,
            "what": self.what,
            "success_criteria": list(self.success_criteria),
            "in_scope": list(self.in_scope),
            "non_goals": list(self.non_goals),
            "authority_boundaries": list(self.authority_boundaries),
            "assumptions": list(self.assumptions),
            "acknowledgement": self.acknowledgement,
            "digest": self.digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> GoalContract:
        keys = frozenset(
            {
                "schema_version",
                "goal_id",
                "revision",
                "predecessor_digest",
                "why",
                "what",
                "success_criteria",
                "in_scope",
                "non_goals",
                "authority_boundaries",
                "assumptions",
                "acknowledgement",
                "digest",
            }
        )
        raw = exact_mapping(value, keys, "goal contract")
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["goal_id"]),
            cast(int, raw["revision"]),
            cast(str | None, raw["predecessor_digest"]),
            cast(str, raw["why"]),
            cast(str, raw["what"]),
            cast(tuple[str, ...], raw["success_criteria"]),
            cast(tuple[str, ...], raw["in_scope"]),
            cast(tuple[str, ...], raw["non_goals"]),
            cast(tuple[str, ...], raw["authority_boundaries"]),
            cast(tuple[str, ...], raw["assumptions"]),
            cast(str, raw["acknowledgement"]),
            cast(str, raw["digest"]),
        )
