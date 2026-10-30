"""Shared strict values for the transport-neutral native plugin host."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from .sessions import ContractError
from .skill_values import canonical_json, digest_json, identifier

NATIVE_PLUGIN_API_VERSION = "1.0.0"
NATIVE_PLUGIN_MANIFEST_SCHEMA_VERSION = "ahc.native-plugin-manifest.v1"
NATIVE_PLUGIN_BINDING_SCHEMA_VERSION = "ahc.native-plugin-binding.v1"
NATIVE_PLUGIN_READINESS_SCHEMA_VERSION = "ahc.native-plugin-readiness.v1"
NATIVE_PLUGIN_INVOCATION_REQUEST_SCHEMA_VERSION = "ahc.native-plugin-invocation-request.v1"
NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION = "ahc.native-plugin-invocation-receipt.v1"

PLUGIN_SCOPES = frozenset({"agent", "global"})
PLUGIN_READINESS_STATUSES = frozenset(
    {"absent", "disabled", "incompatible", "unavailable", "ready", "degraded"}
)
PLUGIN_INVOCATION_OUTCOMES = frozenset({"succeeded", "rejected", "failed", "unknown"})


class PluginEffect(StrEnum):
    """AHC-owned observable effect class for one capability."""

    READ = "read"
    WRITE = "write"


def strict_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise ContractError(f"{label} must be a strict boolean")
    return value


def member(value: object, allowed: frozenset[str], label: str) -> str:
    if type(value) is not str or value not in allowed:
        raise ContractError(f"{label} is unsupported")
    return value


def optional_identifier(value: object, label: str) -> str | None:
    if value is None:
        return None
    return identifier(value, label)


def sorted_identifiers(
    value: object, label: str, *, minimum: int = 1, maximum: int = 64
) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not minimum <= len(value) <= maximum:
        raise ContractError(f"{label} must contain {minimum} to {maximum} items")
    items = tuple(identifier(item, f"{label} item") for item in value)
    if len(set(items)) != len(items):
        raise ContractError(f"{label} must not contain duplicate items")
    if items != tuple(sorted(items)):
        raise ContractError(f"{label} must be sorted")
    return items


def bounded_json_object(value: object, label: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise ContractError(f"{label} must be a JSON object")
    _strict_json(value, label)
    encoded = canonical_json(value)
    if len(encoded) > 8_192:
        raise ContractError(f"{label} exceeds 8192 canonical JSON bytes")
    return value


def _strict_json(value: object, label: str) -> None:
    if value is None or type(value) in {str, int, float, bool}:
        canonical_json(value)
        return
    if type(value) is list:
        for item in value:
            _strict_json(item, label)
        return
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise ContractError(f"{label} must use string keys at every depth")
        for item in value.values():
            _strict_json(item, label)
        return
    raise ContractError(f"{label} contains a non-JSON value")


__all__ = [
    "NATIVE_PLUGIN_API_VERSION",
    "NATIVE_PLUGIN_BINDING_SCHEMA_VERSION",
    "NATIVE_PLUGIN_INVOCATION_RECEIPT_SCHEMA_VERSION",
    "NATIVE_PLUGIN_INVOCATION_REQUEST_SCHEMA_VERSION",
    "NATIVE_PLUGIN_MANIFEST_SCHEMA_VERSION",
    "NATIVE_PLUGIN_READINESS_SCHEMA_VERSION",
    "PLUGIN_INVOCATION_OUTCOMES",
    "PLUGIN_READINESS_STATUSES",
    "PLUGIN_SCOPES",
    "PluginEffect",
    "bounded_json_object",
    "digest_json",
    "member",
    "optional_identifier",
    "sorted_identifiers",
    "strict_bool",
]
