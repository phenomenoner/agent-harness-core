"""Strict values shared by the M2.1 profile authority contracts."""

from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum
from typing import cast

from .sessions import ContractError, _identifier

PROFILE_RESOURCE_SCHEMA_VERSION = "ahc.profile-resource.v1"
AGENT_PROFILE_SCHEMA_VERSION = "ahc.agent-profile.v1"
AGENT_PROFILE_REGISTRY_SCHEMA_VERSION = "ahc.agent-profile-registry.v1"


class ProfileResourceKind(StrEnum):
    """Opaque resource kinds that later adapters may consume."""

    PERSONA = "persona"
    WORKSPACE = "workspace"
    BACKEND_PROFILE = "backend-profile"
    CREDENTIAL = "credential"
    WORKING_MEMORY = "working-memory"
    DURABLE_MEMORY = "durable-memory"
    PLUGIN = "plugin"
    CHANNEL = "channel"
    SUBAGENT_POLICY = "subagent-policy"
    CRON_POLICY = "cron-policy"

    @classmethod
    def parse(cls, value: object, label: str = "kind") -> ProfileResourceKind:
        if type(value) is not str:
            raise ContractError(f"{label} must be a profile resource kind string")
        try:
            return cls(value)
        except ValueError as exc:
            raise ContractError(f"{label} is not a supported profile resource kind") from exc


SCALAR_REFERENCE_KINDS: tuple[tuple[str, ProfileResourceKind], ...] = (
    ("persona_ref", ProfileResourceKind.PERSONA),
    ("workspace_ref", ProfileResourceKind.WORKSPACE),
    ("backend_profile_ref", ProfileResourceKind.BACKEND_PROFILE),
    ("credential_ref", ProfileResourceKind.CREDENTIAL),
    ("working_memory_ref", ProfileResourceKind.WORKING_MEMORY),
    ("durable_memory_ref", ProfileResourceKind.DURABLE_MEMORY),
    ("subagent_policy_ref", ProfileResourceKind.SUBAGENT_POLICY),
    ("cron_policy_ref", ProfileResourceKind.CRON_POLICY),
)
LIST_REFERENCE_KINDS: tuple[tuple[str, ProfileResourceKind], ...] = (
    ("plugin_refs", ProfileResourceKind.PLUGIN),
    ("channel_refs", ProfileResourceKind.CHANNEL),
)


def strict_reference_tuple(value: object, label: str) -> tuple[str, ...]:
    """Normalize one JSON array or tuple without accepting other iterables."""

    if type(value) not in (list, tuple):
        raise ContractError(f"{label} must be an array of reference identifiers")
    refs = tuple(
        _identifier(item, f"{label}[]") for item in cast(list[object] | tuple[object, ...], value)
    )
    if len(set(refs)) != len(refs):
        raise ContractError(f"{label} must not contain duplicate references")
    return refs


def duplicate_identifier(values: Iterable[str]) -> str | None:
    """Return the first duplicate identifier, preserving deterministic input order."""

    seen: set[str] = set()
    for value in values:
        if value in seen:
            return value
        seen.add(value)
    return None


__all__ = [
    "AGENT_PROFILE_REGISTRY_SCHEMA_VERSION",
    "AGENT_PROFILE_SCHEMA_VERSION",
    "LIST_REFERENCE_KINDS",
    "PROFILE_RESOURCE_SCHEMA_VERSION",
    "ProfileResourceKind",
    "SCALAR_REFERENCE_KINDS",
]
