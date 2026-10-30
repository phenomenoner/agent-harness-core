"""Private ephemeral state carrier for the native plugin host."""

from __future__ import annotations

from dataclasses import dataclass

from ahc.domain import NativePluginManifest, PluginBinding, PluginReadiness

from .plugin_port import NativePluginAdapter


@dataclass(slots=True)
class _BindingState:
    binding: PluginBinding
    manifest: NativePluginManifest
    readiness: PluginReadiness
    adapter: NativePluginAdapter | None = None


__all__: list[str] = []
