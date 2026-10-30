"""Immutable M2.1 agent-profile and opaque-resource contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from .profile_values import (
    AGENT_PROFILE_SCHEMA_VERSION,
    LIST_REFERENCE_KINDS,
    PROFILE_RESOURCE_SCHEMA_VERSION,
    SCALAR_REFERENCE_KINDS,
    ProfileResourceKind,
    strict_reference_tuple,
)
from .sessions import ContractError, _exact_mapping, _identifier, _schema_version

_PROFILE_FIELDS = frozenset(
    {
        "schema_version",
        "agent_id",
        *(name for name, _ in SCALAR_REFERENCE_KINDS),
        *(name for name, _ in LIST_REFERENCE_KINDS),
    }
)
_RESOURCE_FIELDS = frozenset({"schema_version", "resource_id", "kind", "owner_agent_id"})


@dataclass(frozen=True, slots=True)
class ProfileResource:
    """One secret-free opaque resource identity owned by exactly one agent."""

    schema_version: str
    resource_id: str
    kind: ProfileResourceKind
    owner_agent_id: str

    def __post_init__(self) -> None:
        _schema_version(self.schema_version, PROFILE_RESOURCE_SCHEMA_VERSION, "profile resource")
        _identifier(self.resource_id, "resource_id")
        if type(self.kind) is not ProfileResourceKind:
            raise ContractError("kind must be a ProfileResourceKind")
        _identifier(self.owner_agent_id, "owner_agent_id")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "resource_id": self.resource_id,
            "kind": self.kind.value,
            "owner_agent_id": self.owner_agent_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ProfileResource:
        raw = _exact_mapping(value, _RESOURCE_FIELDS, "profile resource")
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["resource_id"]),
            ProfileResourceKind.parse(raw["kind"]),
            cast(str, raw["owner_agent_id"]),
        )


@dataclass(frozen=True, slots=True)
class AgentProfile:
    """One canonical agent identity and its explicit typed references."""

    schema_version: str
    agent_id: str
    persona_ref: str
    workspace_ref: str
    backend_profile_ref: str
    credential_ref: str
    working_memory_ref: str
    durable_memory_ref: str
    plugin_refs: tuple[str, ...]
    channel_refs: tuple[str, ...]
    subagent_policy_ref: str
    cron_policy_ref: str

    def __post_init__(self) -> None:
        _schema_version(self.schema_version, AGENT_PROFILE_SCHEMA_VERSION, "agent profile")
        _identifier(self.agent_id, "agent_id")
        for field_name, _ in SCALAR_REFERENCE_KINDS:
            _identifier(getattr(self, field_name), field_name)
        for field_name, _ in LIST_REFERENCE_KINDS:
            refs = strict_reference_tuple(getattr(self, field_name), field_name)
            object.__setattr__(self, field_name, refs)

    def references(self) -> tuple[tuple[str, str, ProfileResourceKind], ...]:
        scalar = tuple(
            (field_name, cast(str, getattr(self, field_name)), kind)
            for field_name, kind in SCALAR_REFERENCE_KINDS
        )
        listed = tuple(
            (field_name, ref, kind)
            for field_name, kind in LIST_REFERENCE_KINDS
            for ref in cast(tuple[str, ...], getattr(self, field_name))
        )
        return scalar + listed

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "agent_id": self.agent_id,
            "persona_ref": self.persona_ref,
            "workspace_ref": self.workspace_ref,
            "backend_profile_ref": self.backend_profile_ref,
            "credential_ref": self.credential_ref,
            "working_memory_ref": self.working_memory_ref,
            "durable_memory_ref": self.durable_memory_ref,
            "plugin_refs": list(self.plugin_refs),
            "channel_refs": list(self.channel_refs),
            "subagent_policy_ref": self.subagent_policy_ref,
            "cron_policy_ref": self.cron_policy_ref,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> AgentProfile:
        raw = _exact_mapping(value, _PROFILE_FIELDS, "agent profile")
        return cls(
            schema_version=cast(str, raw["schema_version"]),
            agent_id=cast(str, raw["agent_id"]),
            persona_ref=cast(str, raw["persona_ref"]),
            workspace_ref=cast(str, raw["workspace_ref"]),
            backend_profile_ref=cast(str, raw["backend_profile_ref"]),
            credential_ref=cast(str, raw["credential_ref"]),
            working_memory_ref=cast(str, raw["working_memory_ref"]),
            durable_memory_ref=cast(str, raw["durable_memory_ref"]),
            plugin_refs=strict_reference_tuple(raw["plugin_refs"], "plugin_refs"),
            channel_refs=strict_reference_tuple(raw["channel_refs"], "channel_refs"),
            subagent_policy_ref=cast(str, raw["subagent_policy_ref"]),
            cron_policy_ref=cast(str, raw["cron_policy_ref"]),
        )


__all__ = ["AgentProfile", "ProfileResource"]
