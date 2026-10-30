"""Deterministic fixtures for M2.3 native-plugin host tests."""

from __future__ import annotations

from profile_test_support import profile_registry_payload

from ahc.domain import (
    NATIVE_PLUGIN_API_VERSION,
    NATIVE_PLUGIN_BINDING_SCHEMA_VERSION,
    NATIVE_PLUGIN_MANIFEST_SCHEMA_VERSION,
    AgentProfileRegistry,
    NativePluginManifest,
    PluginBinding,
    PluginCapability,
    PluginEffect,
)


def profile_registry() -> AgentProfileRegistry:
    return AgentProfileRegistry.from_dict(profile_registry_payload())


def plugin_manifest(
    plugin_id: str = "test.memory",
    *,
    api_version: str = NATIVE_PLUGIN_API_VERSION,
    binding_scopes: tuple[str, ...] = ("agent", "global"),
) -> NativePluginManifest:
    return NativePluginManifest(
        schema_version=NATIVE_PLUGIN_MANIFEST_SCHEMA_VERSION,
        plugin_id=plugin_id,
        api_version=api_version,
        binding_scopes=binding_scopes,
        capabilities=(
            PluginCapability("memory.read", PluginEffect.READ, False),
            PluginCapability("memory.write", PluginEffect.WRITE, True),
        ),
        capability_digest=None,
        manifest_digest=None,
    )


def agent_binding(
    manifest: NativePluginManifest,
    *,
    binding_id: str = "hexi.memory",
    agent_id: str = "hexi",
    plugin_ref: str = "hexi.plugin",
    enabled: bool = True,
    grants: tuple[str, ...] = ("memory.read", "memory.write"),
) -> PluginBinding:
    return PluginBinding(
        schema_version=NATIVE_PLUGIN_BINDING_SCHEMA_VERSION,
        binding_id=binding_id,
        plugin_id=manifest.plugin_id,
        manifest_digest=manifest.manifest_digest,
        scope="agent",
        agent_id=agent_id,
        plugin_ref=plugin_ref,
        enabled=enabled,
        grants=grants,
    )


def global_binding(
    manifest: NativePluginManifest,
    *,
    binding_id: str = "global.memory",
    enabled: bool = True,
) -> PluginBinding:
    return PluginBinding(
        schema_version=NATIVE_PLUGIN_BINDING_SCHEMA_VERSION,
        binding_id=binding_id,
        plugin_id=manifest.plugin_id,
        manifest_digest=manifest.manifest_digest,
        scope="global",
        agent_id=None,
        plugin_ref=None,
        enabled=enabled,
        grants=("memory.read",),
    )
