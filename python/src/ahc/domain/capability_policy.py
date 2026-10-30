"""Complete immutable policy validation for M2.3-Lite."""

from __future__ import annotations

from dataclasses import dataclass

from .capability_values import CapabilityDefinition, CapabilityGrant
from .sessions import ContractError


@dataclass(frozen=True, slots=True)
class CapabilityPolicy:
    definitions: tuple[CapabilityDefinition, ...]
    grants: tuple[CapabilityGrant, ...]

    def __post_init__(self) -> None:
        if type(self.definitions) is not tuple:
            raise ContractError("definitions must detach to a tuple")
        if any(type(item) is not CapabilityDefinition for item in self.definitions):
            raise ContractError("definitions must contain only CapabilityDefinition values")
        if type(self.grants) is not tuple:
            raise ContractError("grants must detach to a tuple")
        if any(type(item) is not CapabilityGrant for item in self.grants):
            raise ContractError("grants must contain only CapabilityGrant values")

        definition_ids = [item.capability_id for item in self.definitions]
        duplicate_definition = _first_duplicate(definition_ids)
        if duplicate_definition is not None:
            raise ContractError(f"duplicate capability definition {duplicate_definition!r}")

        grant_keys = [(item.agent_id, item.capability_id) for item in self.grants]
        duplicate_grant = _first_duplicate(grant_keys)
        if duplicate_grant is not None:
            raise ContractError(
                f"duplicate agent-capability grant {duplicate_grant[0]!r}/{duplicate_grant[1]!r}"
            )

        object.__setattr__(
            self,
            "definitions",
            tuple(sorted(self.definitions, key=lambda item: item.capability_id)),
        )
        object.__setattr__(
            self,
            "grants",
            tuple(sorted(self.grants, key=lambda item: (item.agent_id, item.capability_id))),
        )

    @classmethod
    def detached(
        cls,
        definitions: list[CapabilityDefinition] | tuple[CapabilityDefinition, ...],
        grants: list[CapabilityGrant] | tuple[CapabilityGrant, ...],
    ) -> CapabilityPolicy:
        if type(definitions) not in (list, tuple):
            raise ContractError("definitions must be a list or tuple")
        if type(grants) not in (list, tuple):
            raise ContractError("grants must be a list or tuple")
        detached_definitions = tuple(_copy_definition(item) for item in definitions)
        detached_grants = tuple(_copy_grant(item) for item in grants)
        return cls(detached_definitions, detached_grants)


def _copy_definition(value: object) -> CapabilityDefinition:
    if type(value) is not CapabilityDefinition:
        raise ContractError("definitions must contain only CapabilityDefinition values")
    return CapabilityDefinition(
        value.capability_id,
        value.resource_kind,
        value.effect,
        value.approval_mode,
    )


def _copy_grant(value: object) -> CapabilityGrant:
    if type(value) is not CapabilityGrant:
        raise ContractError("grants must contain only CapabilityGrant values")
    return CapabilityGrant(value.agent_id, value.resource_id, value.capability_id)


def _first_duplicate(values: list[object]) -> object | None:
    seen: set[object] = set()
    for value in values:
        if value in seen:
            return value
        seen.add(value)
    return None


__all__ = ["CapabilityPolicy"]
