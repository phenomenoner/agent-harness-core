"""Private hosted-runtime context installed only by the native AHC host."""

from __future__ import annotations

from types import MappingProxyType
from typing import cast

_STATUS_SCHEMA_VERSION = "ahc.credential-effect-boundary-status.v1"
_SNAPSHOT_FIELDS = frozenset({"schema_version", "status", "purpose"})
_NATIVE_CONTEXT_TYPE: type[object] | None = None
_HOSTED_CONTEXT: object | None = None
_HOSTED_SNAPSHOT: MappingProxyType[str, object] | None = None


def _commit_native_context(context: object, snapshot: object) -> dict[str, object]:
    """Commit a native-only context after Rust has consumed the exact capsule."""

    global _HOSTED_CONTEXT, _HOSTED_SNAPSHOT
    if _NATIVE_CONTEXT_TYPE is None or type(context) is not _NATIVE_CONTEXT_TYPE:
        raise TypeError("context must be the exact native hosted-context type")
    if _HOSTED_CONTEXT is not None:
        raise RuntimeError("native hosted context is already installed")
    if type(snapshot) is not dict or set(cast(dict[object, object], snapshot)) != _SNAPSHOT_FIELDS:
        raise TypeError("native hosted snapshot has invalid fields")
    typed = cast(dict[str, object], snapshot)
    if typed != {
        "schema_version": _STATUS_SCHEMA_VERSION,
        "status": "unavailable",
        "purpose": "model.invoke",
    }:
        raise ValueError("native hosted snapshot is unsupported")
    frozen = MappingProxyType(dict(typed))
    _HOSTED_SNAPSHOT = frozen
    _HOSTED_CONTEXT = context
    return dict(frozen)


def inspect_hosted_runtime() -> dict[str, object]:
    """Return a secret-free snapshot without exposing the capsule or native context."""

    if _HOSTED_CONTEXT is None or _HOSTED_SNAPSHOT is None:
        return {
            "schema_version": _STATUS_SCHEMA_VERSION,
            "status": "not-configured",
            "purpose": None,
        }
    return dict(_HOSTED_SNAPSHOT)


__all__ = ["inspect_hosted_runtime"]
