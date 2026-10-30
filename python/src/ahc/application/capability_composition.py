"""Trusted M2.3-Lite composition against accepted M2.1 authority."""

from __future__ import annotations

from dataclasses import dataclass, field

from ahc.domain.capability_policy import CapabilityPolicy
from ahc.domain.capability_values import (
    ApprovalMode,
    CapabilityDefinition,
    CapabilityGrant,
)
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError, _identifier

from .capability_doctor import (
    AgentCapabilitySnapshot,
    CapabilityPolicyStatus,
    CapabilitySnapshot,
)
from .capability_view import AgentCapabilityView, _issue_agent_capability_view


@dataclass(frozen=True, slots=True)
class CapabilityComposition:
    __views: tuple[tuple[str, AgentCapabilityView], ...] = field(repr=False)
    snapshot: CapabilitySnapshot

    def view(self, agent_id: str, /) -> AgentCapabilityView:
        _identifier(agent_id, "agent_id")
        for configured_agent_id, view in self.__views:
            if configured_agent_id == agent_id:
                return view
        raise ContractError(f"unknown configured agent {agent_id!r}")


@dataclass(frozen=True, slots=True)
class OptionalCapabilityComposition:
    composition: CapabilityComposition | None
    snapshot: CapabilitySnapshot


def compose_capability_views(
    *,
    profile_registry: AgentProfileRegistry,
    definitions: list[CapabilityDefinition] | tuple[CapabilityDefinition, ...],
    grants: list[CapabilityGrant] | tuple[CapabilityGrant, ...],
) -> CapabilityComposition:
    if type(profile_registry) is not AgentProfileRegistry:
        raise ContractError("profile_registry must be an AgentProfileRegistry")
    policy = CapabilityPolicy.detached(definitions, grants)
    definitions_by_id = {item.capability_id: item for item in policy.definitions}
    resources_by_id = {item.resource_id: item for item in profile_registry.resources}

    grants_by_agent: dict[str, list[CapabilityGrant]] = {
        profile.agent_id: [] for profile in profile_registry.profiles
    }
    for grant in policy.grants:
        definition = definitions_by_id.get(grant.capability_id)
        if definition is None:
            raise ContractError(f"grant identifies unknown capability {grant.capability_id!r}")
        try:
            profile = profile_registry.profile(grant.agent_id)
        except ContractError as exc:
            raise ContractError(f"grant identifies unknown agent {grant.agent_id!r}") from exc
        resource = resources_by_id.get(grant.resource_id)
        if resource is None:
            raise ContractError(f"grant identifies unknown resource {grant.resource_id!r}")
        if resource.owner_agent_id != grant.agent_id:
            raise ContractError("grant crosses the resource owner boundary")
        if resource.kind is not definition.resource_kind:
            raise ContractError("grant resource kind does not match capability definition")
        referenced = {
            (resource_id, required_kind)
            for _field_name, resource_id, required_kind in profile.references()
        }
        if (grant.resource_id, definition.resource_kind) not in referenced:
            raise ContractError("grant resource is not referenced by the exact agent profile")
        grants_by_agent[grant.agent_id].append(grant)

    views: list[tuple[str, AgentCapabilityView]] = []
    summaries: list[AgentCapabilitySnapshot] = []
    definition_count = len(policy.definitions)
    for profile in profile_registry.profiles:
        agent_grants = tuple(grants_by_agent[profile.agent_id])
        granted_ids = frozenset(item.capability_id for item in agent_grants)
        resource_by_capability = tuple(
            (item.capability_id, item.resource_id) for item in agent_grants
        )
        approval_required = sum(
            definitions_by_id[item.capability_id].approval_mode is ApprovalMode.REQUIRED
            for item in agent_grants
        )
        views.append(
            (
                profile.agent_id,
                _issue_agent_capability_view(policy.definitions, resource_by_capability),
            )
        )
        summaries.append(
            AgentCapabilitySnapshot(
                profile.agent_id,
                len(granted_ids),
                definition_count - len(granted_ids),
                approval_required,
            )
        )
    snapshot = CapabilitySnapshot(CapabilityPolicyStatus.CONFIGURED, tuple(summaries))
    return CapabilityComposition(tuple(views), snapshot)


def compose_optional_capability_views(
    *,
    profile_registry: AgentProfileRegistry,
    definitions: list[CapabilityDefinition] | tuple[CapabilityDefinition, ...] | None,
    grants: list[CapabilityGrant] | tuple[CapabilityGrant, ...] | None,
) -> OptionalCapabilityComposition:
    if type(profile_registry) is not AgentProfileRegistry:
        raise ContractError("profile_registry must be an AgentProfileRegistry")
    if definitions is None and grants is None:
        return OptionalCapabilityComposition(
            None,
            _denied_snapshot(profile_registry, CapabilityPolicyStatus.NOT_CONFIGURED, 0),
        )
    definition_count = _safe_definition_count(definitions)
    try:
        if definitions is None or grants is None:
            raise ContractError("definitions and grants must both be present")
        composition = compose_capability_views(
            profile_registry=profile_registry,
            definitions=definitions,
            grants=grants,
        )
    except ContractError:
        return OptionalCapabilityComposition(
            None,
            _denied_snapshot(
                profile_registry,
                CapabilityPolicyStatus.INVALID_POLICY,
                definition_count,
            ),
        )
    return OptionalCapabilityComposition(composition, composition.snapshot)


def _safe_definition_count(value: object) -> int:
    if type(value) not in (list, tuple):
        return 0
    items = list(value)
    if any(type(item) is not CapabilityDefinition for item in items):
        return 0
    identifiers = [item.capability_id for item in items]
    return len(set(identifiers)) if len(set(identifiers)) == len(identifiers) else 0


def _denied_snapshot(
    profile_registry: AgentProfileRegistry,
    status: CapabilityPolicyStatus,
    denied_count: int,
) -> CapabilitySnapshot:
    return CapabilitySnapshot(
        status,
        tuple(
            AgentCapabilitySnapshot(profile.agent_id, 0, denied_count, 0)
            for profile in profile_registry.profiles
        ),
    )


__all__ = [
    "CapabilityComposition",
    "OptionalCapabilityComposition",
    "compose_capability_views",
    "compose_optional_capability_views",
]
