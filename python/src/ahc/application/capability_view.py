"""Opaque agent-bound pure decisions for M2.3-Lite."""

from __future__ import annotations

from dataclasses import dataclass, field

from ahc.domain.capability_values import (
    ApprovalMode,
    CapabilityDecision,
    CapabilityDefinition,
    DecisionKind,
    DecisionReason,
    EffectClass,
)
from ahc.domain.sessions import ContractError, _identifier

_VIEW_ISSUER = object()


@dataclass(frozen=True, slots=True, init=False)
class AgentCapabilityView:
    __definitions: tuple[CapabilityDefinition, ...] = field(repr=False)
    __resource_by_capability: tuple[tuple[str, str], ...] = field(repr=False)

    def __init__(
        self,
        issuer: object,
        definitions: tuple[CapabilityDefinition, ...],
        resource_by_capability: tuple[tuple[str, str], ...],
    ) -> None:
        if issuer is not _VIEW_ISSUER:
            raise TypeError("AgentCapabilityView is issued only by capability composition")
        object.__setattr__(self, "_AgentCapabilityView__definitions", definitions)
        object.__setattr__(
            self,
            "_AgentCapabilityView__resource_by_capability",
            resource_by_capability,
        )

    def decide(
        self,
        capability_id: str,
        resource_id: str,
        requested_effect: EffectClass,
        /,
    ) -> CapabilityDecision:
        _identifier(capability_id, "capability_id")
        _identifier(resource_id, "resource_id")
        if type(requested_effect) is not EffectClass:
            raise ContractError("requested_effect must be an EffectClass")
        definition = next(
            (item for item in self.__definitions if item.capability_id == capability_id),
            None,
        )
        if definition is None:
            return CapabilityDecision(
                DecisionKind.DENY,
                DecisionReason.CAPABILITY_UNKNOWN,
                capability_id,
                None,
            )
        bound_resource_id = next(
            (
                configured_resource_id
                for configured_capability_id, configured_resource_id in (
                    self.__resource_by_capability
                )
                if configured_capability_id == capability_id
            ),
            None,
        )
        if bound_resource_id is None:
            return CapabilityDecision(
                DecisionKind.DENY,
                DecisionReason.CAPABILITY_NOT_GRANTED,
                capability_id,
                definition.effect,
            )
        if resource_id != bound_resource_id:
            return CapabilityDecision(
                DecisionKind.DENY,
                DecisionReason.RESOURCE_NOT_OWNED,
                capability_id,
                definition.effect,
            )
        if requested_effect is not definition.effect:
            return CapabilityDecision(
                DecisionKind.DENY,
                DecisionReason.EFFECT_MISMATCH,
                capability_id,
                definition.effect,
            )
        if definition.approval_mode is ApprovalMode.REQUIRED:
            return CapabilityDecision(
                DecisionKind.REQUIRES_APPROVAL,
                DecisionReason.APPROVAL_REQUIRED,
                capability_id,
                definition.effect,
            )
        return CapabilityDecision(
            DecisionKind.ALLOW,
            DecisionReason.ALLOWED,
            capability_id,
            definition.effect,
        )


def _issue_agent_capability_view(
    definitions: tuple[CapabilityDefinition, ...],
    resource_by_capability: tuple[tuple[str, str], ...],
) -> AgentCapabilityView:
    return AgentCapabilityView(_VIEW_ISSUER, definitions, resource_by_capability)


__all__ = ["AgentCapabilityView"]
