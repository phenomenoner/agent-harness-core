"""Strict deterministic direction-signal values."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from .sessions import ContractError
from .skill_values import digest, digest_json, exact_mapping, ordered_text_items, schema


def _finish(payload: dict[str, object], supplied: object, label: str) -> str:
    actual = digest_json(payload)
    if supplied is not None and digest(supplied, label) != actual:
        raise ContractError(f"{label} does not match canonical content")
    return actual


SIGNAL_SCHEMA = "ahc.direction-signal.v1"
SIGNAL_KINDS = (
    "repeated-same-cause",
    "review-without-outcome",
    "budget-overrun",
    "no-user-visible-outcome",
    "non-goal-conflict",
    "new-authority-owner",
    "disproven-premise",
    "scope-effect-drift",
    "simpler-path-available",
    "user-correction",
)


def _refs(value: object, label: str) -> tuple[str, ...]:
    return ordered_text_items(value, label, 0, 32)


@dataclass(frozen=True, slots=True)
class DirectionSignal:
    schema_version: str
    kind: str
    observed: int
    threshold: int
    condition_met: bool
    evidence_refs: tuple[str, ...]
    digest: str | None = None

    def __post_init__(self) -> None:
        schema(self.schema_version, SIGNAL_SCHEMA, "direction signal")
        if self.kind not in SIGNAL_KINDS:
            raise ContractError("direction signal kind is unsupported")
        if (
            type(self.observed) is not int
            or self.observed < 0
            or type(self.threshold) is not int
            or self.threshold < 1
        ):
            raise ContractError("signal observed/threshold must be bounded integers")
        if type(self.condition_met) is not bool:
            raise ContractError("signal condition_met must be boolean")
        refs = _refs(self.evidence_refs, "signal evidence_refs")
        payload = {
            "schema_version": self.schema_version,
            "kind": self.kind,
            "observed": self.observed,
            "threshold": self.threshold,
            "condition_met": self.condition_met,
            "evidence_refs": list(refs),
        }
        actual = _finish(payload, self.digest, "signal digest")
        object.__setattr__(self, "evidence_refs", refs)
        object.__setattr__(self, "digest", actual)

    @property
    def fired(self) -> bool:
        return self.condition_met or self.observed >= self.threshold

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind,
            "observed": self.observed,
            "threshold": self.threshold,
            "condition_met": self.condition_met,
            "evidence_refs": list(self.evidence_refs),
            "digest": self.digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> DirectionSignal:
        raw = exact_mapping(
            value,
            frozenset(
                {
                    "schema_version",
                    "kind",
                    "observed",
                    "threshold",
                    "condition_met",
                    "evidence_refs",
                    "digest",
                }
            ),
            "direction signal",
        )
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["kind"]),
            cast(int, raw["observed"]),
            cast(int, raw["threshold"]),
            cast(bool, raw["condition_met"]),
            cast(tuple[str, ...], raw["evidence_refs"]),
            cast(str, raw["digest"]),
        )
