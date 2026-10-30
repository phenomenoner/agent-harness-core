"""Immutable capability and manifest contracts for native plugins."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from .plugin_values import (
    NATIVE_PLUGIN_MANIFEST_SCHEMA_VERSION,
    PLUGIN_SCOPES,
    PluginEffect,
    member,
    strict_bool,
)
from .sessions import ContractError
from .skill_values import digest, digest_json, exact_mapping, identifier, schema, version


@dataclass(frozen=True, slots=True)
class PluginCapability:
    """One AHC-owned capability with an exact effect and approval rule."""

    capability_id: str
    effect: PluginEffect
    approval_required: bool

    def __post_init__(self) -> None:
        identifier(self.capability_id, "capability_id")
        try:
            effect = PluginEffect(self.effect)
        except (TypeError, ValueError) as exc:
            raise ContractError("effect is unsupported") from exc
        strict_bool(self.approval_required, "approval_required")
        object.__setattr__(self, "effect", effect)

    def to_dict(self) -> dict[str, object]:
        return {
            "capability_id": self.capability_id,
            "effect": self.effect.value,
            "approval_required": self.approval_required,
        }

    @classmethod
    def from_dict(cls, value: object) -> PluginCapability:
        raw = exact_mapping(
            value,
            frozenset({"capability_id", "effect", "approval_required"}),
            "plugin capability",
        )
        return cls(
            cast(str, raw["capability_id"]),
            cast(PluginEffect, raw["effect"]),
            cast(bool, raw["approval_required"]),
        )


@dataclass(frozen=True, slots=True)
class NativePluginManifest:
    """Self-digested, transport-neutral manifest admitted by the AHC host."""

    schema_version: str
    plugin_id: str
    api_version: str
    binding_scopes: tuple[str, ...]
    capabilities: tuple[PluginCapability, ...]
    capability_digest: str | None = None
    manifest_digest: str | None = None

    def __post_init__(self) -> None:
        schema(self.schema_version, NATIVE_PLUGIN_MANIFEST_SCHEMA_VERSION, "native plugin manifest")
        identifier(self.plugin_id, "plugin_id")
        version(self.api_version, "api_version")
        if not isinstance(self.binding_scopes, (list, tuple)):
            raise ContractError("binding_scopes must be an array")
        scopes = tuple(member(item, PLUGIN_SCOPES, "binding scope") for item in self.binding_scopes)
        if not 1 <= len(scopes) <= 2:
            raise ContractError("binding_scopes must contain 1 to 2 items")
        if len(set(scopes)) != len(scopes):
            raise ContractError("binding_scopes must not contain duplicate items")
        if scopes != tuple(sorted(scopes)):
            raise ContractError("binding_scopes must be sorted")
        if not isinstance(self.capabilities, (list, tuple)):
            raise ContractError("capabilities must be an array")
        capabilities = tuple(self.capabilities)
        if not 1 <= len(capabilities) <= 64 or any(
            type(item) is not PluginCapability for item in capabilities
        ):
            raise ContractError("capabilities must contain 1 to 64 PluginCapability values")
        ids = tuple(item.capability_id for item in capabilities)
        if len(set(ids)) != len(ids):
            raise ContractError("capabilities must not contain duplicate capability_id values")
        if ids != tuple(sorted(ids)):
            raise ContractError("capabilities must be sorted by capability_id")
        capability_actual = digest_json([item.to_dict() for item in capabilities])
        if self.capability_digest is not None:
            digest(self.capability_digest, "capability_digest")
            if self.capability_digest != capability_actual:
                raise ContractError("capability_digest does not match canonical capabilities")
        payload = {
            "schema_version": self.schema_version,
            "plugin_id": self.plugin_id,
            "api_version": self.api_version,
            "binding_scopes": list(scopes),
            "capabilities": [item.to_dict() for item in capabilities],
            "capability_digest": capability_actual,
        }
        manifest_actual = digest_json(payload)
        if self.manifest_digest is not None:
            digest(self.manifest_digest, "manifest_digest")
            if self.manifest_digest != manifest_actual:
                raise ContractError("manifest_digest does not match canonical manifest")
        object.__setattr__(self, "binding_scopes", scopes)
        object.__setattr__(self, "capabilities", capabilities)
        object.__setattr__(self, "capability_digest", capability_actual)
        object.__setattr__(self, "manifest_digest", manifest_actual)

    def capability(self, capability_id: str) -> PluginCapability:
        identifier(capability_id, "capability_id")
        for item in self.capabilities:
            if item.capability_id == capability_id:
                return item
        raise ContractError("unknown capability_id")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "plugin_id": self.plugin_id,
            "api_version": self.api_version,
            "binding_scopes": list(self.binding_scopes),
            "capabilities": [item.to_dict() for item in self.capabilities],
            "capability_digest": self.capability_digest,
            "manifest_digest": self.manifest_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> NativePluginManifest:
        raw = exact_mapping(
            value,
            frozenset(
                {
                    "schema_version",
                    "plugin_id",
                    "api_version",
                    "binding_scopes",
                    "capabilities",
                    "capability_digest",
                    "manifest_digest",
                }
            ),
            "native plugin manifest",
        )
        if raw["capability_digest"] is None or raw["manifest_digest"] is None:
            raise ContractError("parsed manifest digests must be present")
        if type(raw["binding_scopes"]) is not list:
            raise ContractError("binding_scopes must be a JSON array")
        capabilities_raw = raw["capabilities"]
        if type(capabilities_raw) is not list:
            raise ContractError("capabilities must be a JSON array")
        return cls(
            cast(str, raw["schema_version"]),
            cast(str, raw["plugin_id"]),
            cast(str, raw["api_version"]),
            cast(tuple[str, ...], raw["binding_scopes"]),
            tuple(PluginCapability.from_dict(item) for item in capabilities_raw),
            cast(str, raw["capability_digest"]),
            cast(str, raw["manifest_digest"]),
        )


__all__ = ["NativePluginManifest", "PluginCapability"]
