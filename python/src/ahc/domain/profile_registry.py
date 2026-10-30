"""Whole-document authority validation for M2.1 agent profiles."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from .profile_values import AGENT_PROFILE_REGISTRY_SCHEMA_VERSION, duplicate_identifier
from .profiles import AgentProfile, ProfileResource
from .sessions import ContractError, _exact_mapping, _identifier, _schema_version

_REGISTRY_FIELDS = frozenset({"schema_version", "default_agent_id", "profiles", "resources"})


@dataclass(frozen=True, slots=True)
class AgentProfileRegistry:
    """The all-or-nothing canonical profile and resource authority."""

    schema_version: str
    default_agent_id: str
    profiles: tuple[AgentProfile, ...]
    resources: tuple[ProfileResource, ...]

    def __post_init__(self) -> None:
        _schema_version(
            self.schema_version,
            AGENT_PROFILE_REGISTRY_SCHEMA_VERSION,
            "agent profile registry",
        )
        _identifier(self.default_agent_id, "default_agent_id")
        if type(self.profiles) is not tuple or not self.profiles:
            raise ContractError("profiles must be a non-empty tuple of AgentProfile values")
        if any(type(profile) is not AgentProfile for profile in self.profiles):
            raise ContractError("profiles must contain only AgentProfile values")
        if type(self.resources) is not tuple or not self.resources:
            raise ContractError("resources must be a non-empty tuple of ProfileResource values")
        if any(type(resource) is not ProfileResource for resource in self.resources):
            raise ContractError("resources must contain only ProfileResource values")
        self._validate_authority()

    def _validate_authority(self) -> None:
        duplicate = duplicate_identifier(profile.agent_id for profile in self.profiles)
        if duplicate is not None:
            raise ContractError(f"profiles duplicate agent_id {duplicate!r}")
        duplicate = duplicate_identifier(resource.resource_id for resource in self.resources)
        if duplicate is not None:
            raise ContractError(f"resources duplicate resource_id {duplicate!r}")

        profile_ids = {profile.agent_id for profile in self.profiles}
        if self.default_agent_id not in profile_ids:
            raise ContractError("default_agent_id does not identify a configured profile")
        resources = {resource.resource_id: resource for resource in self.resources}
        for resource in self.resources:
            if resource.owner_agent_id not in profile_ids:
                raise ContractError(
                    f"resource {resource.resource_id!r} owner does not identify "
                    "a configured profile"
                )

        referenced: list[str] = []
        for profile in self.profiles:
            for field_name, resource_id, required_kind in profile.references():
                resource = resources.get(resource_id)
                if resource is None:
                    raise ContractError(
                        f"profile {profile.agent_id!r} {field_name} references unknown resource "
                        f"{resource_id!r}"
                    )
                if resource.kind is not required_kind:
                    raise ContractError(
                        f"profile {profile.agent_id!r} {field_name} requires kind "
                        f"{required_kind.value!r}"
                    )
                if resource.owner_agent_id != profile.agent_id:
                    raise ContractError(
                        f"profile {profile.agent_id!r} {field_name} crosses owner boundary to "
                        f"{resource.owner_agent_id!r}"
                    )
                referenced.append(resource_id)

        counts = Counter(referenced)
        repeated = next((resource_id for resource_id, count in counts.items() if count != 1), None)
        if repeated is not None:
            raise ContractError(f"resource {repeated!r} must be referenced exactly once")
        unreferenced = next(
            (
                resource.resource_id
                for resource in self.resources
                if resource.resource_id not in counts
            ),
            None,
        )
        if unreferenced is not None:
            raise ContractError(f"resource {unreferenced!r} is not referenced by its owner profile")

    def profile(self, agent_id: str) -> AgentProfile:
        _identifier(agent_id, "agent_id")
        for profile in self.profiles:
            if profile.agent_id == agent_id:
                return profile
        raise ContractError(f"unknown agent profile {agent_id!r}")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "default_agent_id": self.default_agent_id,
            "profiles": [profile.to_dict() for profile in self.profiles],
            "resources": [resource.to_dict() for resource in self.resources],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> AgentProfileRegistry:
        raw = _exact_mapping(value, _REGISTRY_FIELDS, "agent profile registry")
        raw_profiles = raw["profiles"]
        raw_resources = raw["resources"]
        if type(raw_profiles) is not list:
            raise ContractError("profiles must be a JSON array")
        if type(raw_resources) is not list:
            raise ContractError("resources must be a JSON array")
        try:
            profiles = tuple(
                AgentProfile.from_dict(cast(Mapping[str, object], item)) for item in raw_profiles
            )
        except ContractError as exc:
            raise ContractError(f"profiles: {exc}") from exc
        try:
            resources = tuple(
                ProfileResource.from_dict(cast(Mapping[str, object], item))
                for item in raw_resources
            )
        except ContractError as exc:
            raise ContractError(f"resources: {exc}") from exc
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["default_agent_id"]),
            profiles,
            resources,
        )


__all__ = ["AgentProfileRegistry"]
