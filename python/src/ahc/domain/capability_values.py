"""Immutable scalar values for the M2.3-Lite capability authority."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .profile_values import ProfileResourceKind
from .sessions import ContractError, _identifier


class EffectClass(StrEnum):
    READ_ONLY = "READ_ONLY"
    REVERSIBLE_LOCAL = "REVERSIBLE_LOCAL"
    EXTERNAL_OR_IRREVERSIBLE = "EXTERNAL_OR_IRREVERSIBLE"


class ApprovalMode(StrEnum):
    NONE = "NONE"
    REQUIRED = "REQUIRED"


class DecisionKind(StrEnum):
    ALLOW = "ALLOW"
    DENY = "DENY"
    REQUIRES_APPROVAL = "REQUIRES_APPROVAL"


class DecisionReason(StrEnum):
    ALLOWED = "allowed"
    APPROVAL_REQUIRED = "approval-required"
    CAPABILITY_UNKNOWN = "capability-unknown"
    CAPABILITY_NOT_GRANTED = "capability-not-granted"
    RESOURCE_NOT_OWNED = "resource-not-owned"
    EFFECT_MISMATCH = "effect-mismatch"


@dataclass(frozen=True, slots=True)
class CapabilityDefinition:
    capability_id: str
    resource_kind: ProfileResourceKind
    effect: EffectClass
    approval_mode: ApprovalMode

    def __post_init__(self) -> None:
        _identifier(self.capability_id, "capability_id")
        if type(self.resource_kind) is not ProfileResourceKind:
            raise ContractError("resource_kind must be a ProfileResourceKind")
        if type(self.effect) is not EffectClass:
            raise ContractError("effect must be an EffectClass")
        if type(self.approval_mode) is not ApprovalMode:
            raise ContractError("approval_mode must be an ApprovalMode")
        if (
            self.effect is EffectClass.EXTERNAL_OR_IRREVERSIBLE
            and self.approval_mode is not ApprovalMode.REQUIRED
        ):
            raise ContractError("external or irreversible capabilities require approval")


@dataclass(frozen=True, slots=True)
class CapabilityGrant:
    agent_id: str
    resource_id: str
    capability_id: str

    def __post_init__(self) -> None:
        _identifier(self.agent_id, "agent_id")
        _identifier(self.resource_id, "resource_id")
        _identifier(self.capability_id, "capability_id")


_ALLOWED_DECISIONS = frozenset(
    {
        (DecisionKind.ALLOW, DecisionReason.ALLOWED, True),
        (DecisionKind.REQUIRES_APPROVAL, DecisionReason.APPROVAL_REQUIRED, True),
        (DecisionKind.DENY, DecisionReason.CAPABILITY_UNKNOWN, False),
        (DecisionKind.DENY, DecisionReason.CAPABILITY_NOT_GRANTED, True),
        (DecisionKind.DENY, DecisionReason.RESOURCE_NOT_OWNED, True),
        (DecisionKind.DENY, DecisionReason.EFFECT_MISMATCH, True),
    }
)


@dataclass(frozen=True, slots=True)
class CapabilityDecision:
    kind: DecisionKind
    reason: DecisionReason
    capability_id: str
    effect: EffectClass | None

    def __post_init__(self) -> None:
        if type(self.kind) is not DecisionKind:
            raise ContractError("kind must be a DecisionKind")
        if type(self.reason) is not DecisionReason:
            raise ContractError("reason must be a DecisionReason")
        _identifier(self.capability_id, "capability_id")
        if self.effect is not None and type(self.effect) is not EffectClass:
            raise ContractError("effect must be an EffectClass or None")
        if (self.kind, self.reason, self.effect is not None) not in _ALLOWED_DECISIONS:
            raise ContractError("decision kind, reason, and effect are inconsistent")


__all__ = [
    "ApprovalMode",
    "CapabilityDecision",
    "CapabilityDefinition",
    "CapabilityGrant",
    "DecisionKind",
    "DecisionReason",
    "EffectClass",
]
