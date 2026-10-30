"""Strict intent-frame and premise values."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from .sessions import ContractError, VirtualSessionIdentity, _identifier
from .skill_values import (
    bounded_text_items,
    digest,
    digest_json,
    exact_mapping,
    schema,
    text,
)


def _finish(payload: dict[str, object], supplied: object, label: str) -> str:
    actual = digest_json(payload)
    if supplied is not None and digest(supplied, label) != actual:
        raise ContractError(f"{label} does not match canonical content")
    return actual


INTENT_SCHEMA = "ahc.intent-frame.v1"
PROVENANCES = ("user-asserted", "tool-observed", "source-cited", "assumption")


def _items(value: object, label: str) -> tuple[str, ...]:
    return bounded_text_items(value, label, 32)


@dataclass(frozen=True, slots=True)
class Premise:
    statement: str
    provenance: str
    reference: str | None = None

    def __post_init__(self) -> None:
        text(self.statement, "premise.statement", 2_048, multiline=True)
        if self.provenance not in PROVENANCES:
            raise ContractError("premise provenance is unsupported")
        if self.reference is not None:
            text(self.reference, "premise.reference", 512)

    def to_dict(self) -> dict[str, object]:
        return {
            "statement": self.statement,
            "provenance": self.provenance,
            "reference": self.reference,
        }

    @classmethod
    def from_dict(cls, value: object) -> Premise:
        raw = exact_mapping(value, frozenset({"statement", "provenance", "reference"}), "premise")
        return cls(
            cast(str, raw["statement"]),
            cast(str, raw["provenance"]),
            cast(str | None, raw["reference"]),
        )


@dataclass(frozen=True, slots=True)
class IntentFrame:
    schema_version: str
    session: VirtualSessionIdentity
    turn_id: str
    stated_request: str
    inferred_outcome: str
    constraints: tuple[str, ...]
    non_goals: tuple[str, ...]
    premises: tuple[Premise, ...]
    uncertainty_notes: tuple[str, ...]
    authority_boundaries: tuple[str, ...]
    digest: str | None = None

    def __post_init__(self) -> None:
        schema(self.schema_version, INTENT_SCHEMA, "intent frame")
        if type(self.session) is not VirtualSessionIdentity:
            raise ContractError("intent session must be a VirtualSessionIdentity")
        _identifier(self.turn_id, "intent turn_id")
        request = text(self.stated_request, "stated_request", 16_384, multiline=True)
        outcome = text(self.inferred_outcome, "inferred_outcome", 16_384, multiline=True)
        constraints, non_goals = (
            _items(self.constraints, "constraints"),
            _items(self.non_goals, "non_goals"),
        )
        premises = tuple(self.premises)
        if len(premises) > 32 or any(type(item) is not Premise for item in premises):
            raise ContractError("premises must contain 0 to 32 Premise values")
        uncertainty, boundaries = (
            _items(self.uncertainty_notes, "uncertainty_notes"),
            _items(self.authority_boundaries, "authority_boundaries"),
        )
        payload = {
            "schema_version": self.schema_version,
            "session": self.session.to_dict(),
            "turn_id": self.turn_id,
            "stated_request": request,
            "inferred_outcome": outcome,
            "constraints": list(constraints),
            "non_goals": list(non_goals),
            "premises": [item.to_dict() for item in premises],
            "uncertainty_notes": list(uncertainty),
            "authority_boundaries": list(boundaries),
        }
        actual = _finish(payload, self.digest, "intent digest")
        for name, value in (
            ("stated_request", request),
            ("inferred_outcome", outcome),
            ("constraints", constraints),
            ("non_goals", non_goals),
            ("premises", premises),
            ("uncertainty_notes", uncertainty),
            ("authority_boundaries", boundaries),
            ("digest", actual),
        ):
            object.__setattr__(self, name, value)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "session": self.session.to_dict(),
            "turn_id": self.turn_id,
            "stated_request": self.stated_request,
            "inferred_outcome": self.inferred_outcome,
            "constraints": list(self.constraints),
            "non_goals": list(self.non_goals),
            "premises": [item.to_dict() for item in self.premises],
            "uncertainty_notes": list(self.uncertainty_notes),
            "authority_boundaries": list(self.authority_boundaries),
            "digest": self.digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> IntentFrame:
        keys = frozenset(
            {
                "schema_version",
                "session",
                "turn_id",
                "stated_request",
                "inferred_outcome",
                "constraints",
                "non_goals",
                "premises",
                "uncertainty_notes",
                "authority_boundaries",
                "digest",
            }
        )
        raw = exact_mapping(value, keys, "intent frame")
        return cls(
            cast(str, raw["schema_version"]),
            VirtualSessionIdentity.from_dict(cast(dict[str, object], raw["session"])),
            cast(str, raw["turn_id"]),
            cast(str, raw["stated_request"]),
            cast(str, raw["inferred_outcome"]),
            cast(tuple[str, ...], raw["constraints"]),
            cast(tuple[str, ...], raw["non_goals"]),
            tuple(Premise.from_dict(item) for item in cast(list[object], raw["premises"])),
            cast(tuple[str, ...], raw["uncertainty_notes"]),
            cast(tuple[str, ...], raw["authority_boundaries"]),
            cast(str, raw["digest"]),
        )
