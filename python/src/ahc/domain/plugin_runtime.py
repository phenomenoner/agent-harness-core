"""Strict binding, readiness, request, and receipt contracts for native plugins."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from .plugin_values import (
    NATIVE_PLUGIN_BINDING_SCHEMA_VERSION,
    NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION,
    NATIVE_PLUGIN_INVOCATION_REQUEST_SCHEMA_VERSION,
    NATIVE_PLUGIN_READINESS_SCHEMA_VERSION,
    PLUGIN_INVOCATION_OUTCOMES,
    PLUGIN_READINESS_STATUSES,
    PLUGIN_SCOPES,
    PluginEffect,
    member,
    optional_identifier,
    sorted_identifiers,
    strict_bool,
)
from .sessions import ContractError
from .skill_values import digest, exact_mapping, identifier, schema


@dataclass(frozen=True, slots=True)
class PluginBinding:
    schema_version: str
    binding_id: str
    plugin_id: str
    manifest_digest: str
    scope: str
    agent_id: str | None
    plugin_ref: str | None
    enabled: bool
    grants: tuple[str, ...]

    def __post_init__(self) -> None:
        schema(self.schema_version, NATIVE_PLUGIN_BINDING_SCHEMA_VERSION, "plugin binding")
        identifier(self.binding_id, "binding_id")
        identifier(self.plugin_id, "plugin_id")
        digest(self.manifest_digest, "manifest_digest")
        scope = member(self.scope, PLUGIN_SCOPES, "binding scope")
        agent = optional_identifier(self.agent_id, "agent_id")
        plugin_ref = optional_identifier(self.plugin_ref, "plugin_ref")
        if scope == "agent" and (agent is None or plugin_ref is None):
            raise ContractError("agent scope requires agent_id and plugin_ref")
        if scope == "global" and (agent is not None or plugin_ref is not None):
            raise ContractError("global scope requires null agent_id and plugin_ref")
        strict_bool(self.enabled, "enabled")
        grants = sorted_identifiers(self.grants, "grants", minimum=0)
        object.__setattr__(self, "scope", scope)
        object.__setattr__(self, "agent_id", agent)
        object.__setattr__(self, "plugin_ref", plugin_ref)
        object.__setattr__(self, "grants", grants)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "binding_id": self.binding_id,
            "plugin_id": self.plugin_id,
            "manifest_digest": self.manifest_digest,
            "scope": self.scope,
            "agent_id": self.agent_id,
            "plugin_ref": self.plugin_ref,
            "enabled": self.enabled,
            "grants": list(self.grants),
        }

    @classmethod
    def from_dict(cls, value: object) -> PluginBinding:
        keys = frozenset(
            {
                "schema_version",
                "binding_id",
                "plugin_id",
                "manifest_digest",
                "scope",
                "agent_id",
                "plugin_ref",
                "enabled",
                "grants",
            }
        )
        raw = exact_mapping(value, keys, "plugin binding")
        if type(raw["grants"]) is not list:
            raise ContractError("grants must be a JSON array")
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["binding_id"]),
            cast(str, raw["plugin_id"]),
            cast(str, raw["manifest_digest"]),
            cast(str, raw["scope"]),
            cast(str | None, raw["agent_id"]),
            cast(str | None, raw["plugin_ref"]),
            cast(bool, raw["enabled"]),
            cast(tuple[str, ...], raw["grants"]),
        )


@dataclass(frozen=True, slots=True)
class PluginReadiness:
    schema_version: str
    binding_id: str
    plugin_id: str | None
    manifest_digest: str | None
    status: str
    reason_code: str

    def __post_init__(self) -> None:
        schema(self.schema_version, NATIVE_PLUGIN_READINESS_SCHEMA_VERSION, "plugin readiness")
        identifier(self.binding_id, "binding_id")
        plugin_id = optional_identifier(self.plugin_id, "plugin_id")
        if self.manifest_digest is not None:
            digest(self.manifest_digest, "manifest_digest")
        status = member(self.status, PLUGIN_READINESS_STATUSES, "readiness status")
        identifier(self.reason_code, "reason_code")
        if (plugin_id is None) != (self.manifest_digest is None):
            raise ContractError(
                "plugin_id and manifest_digest must both be null or both be present"
            )
        known = plugin_id is not None
        if status == "absent" and known:
            raise ContractError("absent readiness must not identify a plugin")
        if status != "absent" and not known:
            raise ContractError("known readiness requires plugin_id and manifest_digest")
        object.__setattr__(self, "plugin_id", plugin_id)
        object.__setattr__(self, "status", status)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "binding_id": self.binding_id,
            "plugin_id": self.plugin_id,
            "manifest_digest": self.manifest_digest,
            "status": self.status,
            "reason_code": self.reason_code,
        }

    @classmethod
    def from_dict(cls, value: object) -> PluginReadiness:
        raw = exact_mapping(
            value,
            frozenset(
                {
                    "schema_version",
                    "binding_id",
                    "plugin_id",
                    "manifest_digest",
                    "status",
                    "reason_code",
                }
            ),
            "plugin readiness",
        )
        return cls(*(raw[key] for key in (
            "schema_version", "binding_id", "plugin_id", "manifest_digest", "status", "reason_code"
        )))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class PluginInvocationRequest:
    schema_version: str
    invocation_id: str
    agent_id: str
    binding_id: str
    capability_id: str
    payload_digest: str

    def __post_init__(self) -> None:
        schema(
            self.schema_version,
            NATIVE_PLUGIN_INVOCATION_REQUEST_SCHEMA_VERSION,
            "plugin request",
        )
        identifier(self.invocation_id, "invocation_id")
        identifier(self.agent_id, "agent_id")
        identifier(self.binding_id, "binding_id")
        identifier(self.capability_id, "capability_id")
        digest(self.payload_digest, "payload_digest")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "invocation_id": self.invocation_id,
            "agent_id": self.agent_id,
            "binding_id": self.binding_id,
            "capability_id": self.capability_id,
            "payload_digest": self.payload_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> PluginInvocationRequest:
        keys = (
            "schema_version",
            "invocation_id",
            "agent_id",
            "binding_id",
            "capability_id",
            "payload_digest",
        )
        raw = exact_mapping(value, frozenset(keys), "plugin invocation request")
        return cls(*(cast(str, raw[key]) for key in keys))


@dataclass(frozen=True, slots=True)
class PluginInvocationReceipt:
    schema_version: str
    invocation_id: str
    agent_id: str
    binding_id: str
    plugin_id: str
    manifest_digest: str
    capability_id: str
    effect: PluginEffect
    request_digest: str
    result_digest: str | None
    outcome: str
    reason_code: str

    def __post_init__(self) -> None:
        schema(
            self.schema_version,
            NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION,
            "plugin receipt",
        )
        for label, value in (
            ("invocation_id", self.invocation_id), ("agent_id", self.agent_id),
            ("binding_id", self.binding_id), ("plugin_id", self.plugin_id),
            ("capability_id", self.capability_id), ("reason_code", self.reason_code),
        ):
            identifier(value, label)
        digest(self.manifest_digest, "manifest_digest")
        digest(self.request_digest, "request_digest")
        if self.result_digest is not None:
            digest(self.result_digest, "result_digest")
        try:
            effect = PluginEffect(self.effect)
        except (TypeError, ValueError) as exc:
            raise ContractError("effect is unsupported") from exc
        outcome = member(self.outcome, PLUGIN_INVOCATION_OUTCOMES, "invocation outcome")
        if (outcome == "succeeded") != (self.result_digest is not None):
            raise ContractError("result_digest is required only for succeeded outcome")
        object.__setattr__(self, "effect", effect)
        object.__setattr__(self, "outcome", outcome)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version, "invocation_id": self.invocation_id,
            "agent_id": self.agent_id, "binding_id": self.binding_id,
            "plugin_id": self.plugin_id, "manifest_digest": self.manifest_digest,
            "capability_id": self.capability_id, "effect": self.effect.value,
            "request_digest": self.request_digest, "result_digest": self.result_digest,
            "outcome": self.outcome, "reason_code": self.reason_code,
        }

    @classmethod
    def from_dict(cls, value: object) -> PluginInvocationReceipt:
        keys = (
            "schema_version", "invocation_id", "agent_id", "binding_id", "plugin_id",
            "manifest_digest", "capability_id", "effect", "request_digest", "result_digest",
            "outcome", "reason_code",
        )
        raw = exact_mapping(value, frozenset(keys), "plugin invocation receipt")
        return cls(*(raw[key] for key in keys))  # type: ignore[arg-type]


__all__ = ["PluginBinding", "PluginInvocationReceipt", "PluginInvocationRequest", "PluginReadiness"]
