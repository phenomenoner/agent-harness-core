"""Pure sanitized readiness projection for the native plugin host."""

from __future__ import annotations

from .plugin_host import NativePluginHost

NATIVE_PLUGIN_DOCTOR_SCHEMA_VERSION = "ahc.native-plugin-doctor.v1"


def inspect_native_plugin_host(host: NativePluginHost | None) -> dict[str, object]:
    """Project current readiness without construction, opening, repair, or invocation."""

    if host is None:
        return {
            "schema_version": NATIVE_PLUGIN_DOCTOR_SCHEMA_VERSION,
            "status": "not-configured",
            "bindings": [],
        }
    if type(host) is not NativePluginHost:
        raise TypeError("host must be a NativePluginHost or None")
    return {
        "schema_version": NATIVE_PLUGIN_DOCTOR_SCHEMA_VERSION,
        "status": "configured",
        "bindings": [item.to_dict() for item in host.readinesses()],
    }


__all__ = ["NATIVE_PLUGIN_DOCTOR_SCHEMA_VERSION", "inspect_native_plugin_host"]
