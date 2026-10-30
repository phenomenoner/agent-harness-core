"""Package-owned immutable native plugin catalogs."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from ahc.domain import (
    NATIVE_PLUGIN_API_VERSION,
    NATIVE_PLUGIN_MANIFEST_SCHEMA_VERSION,
    NativePluginManifest,
    PluginCapability,
    PluginEffect,
)

from .fake_plugin import FakePluginFactory
from .plugin_port import NativePluginFactory

_EMPTY_CATALOG = "empty"
_M23_FAKE_CATALOG = "m2.3-fakes"

_FAKE_SPECS = (
    ("test.degraded", NATIVE_PLUGIN_API_VERSION, "degraded", True),
    ("test.disabled", NATIVE_PLUGIN_API_VERSION, "ready", True),
    ("test.future", "2.0.0", "ready", True),
    ("test.memory", NATIVE_PLUGIN_API_VERSION, "ready", True),
    ("test.missing", NATIVE_PLUGIN_API_VERSION, "ready", False),
    ("test.open-exception", NATIVE_PLUGIN_API_VERSION, "raise", True),
    ("test.open-malformed", NATIVE_PLUGIN_API_VERSION, "malformed", True),
    ("test.ready", NATIVE_PLUGIN_API_VERSION, "ready", True),
    ("test.unavailable", NATIVE_PLUGIN_API_VERSION, "unavailable", True),
)


def _manifest(plugin_id: str, api_version: str) -> NativePluginManifest:
    return NativePluginManifest(
        NATIVE_PLUGIN_MANIFEST_SCHEMA_VERSION,
        plugin_id,
        api_version,
        ("agent", "global"),
        (
            PluginCapability("memory.read", PluginEffect.READ, False),
            PluginCapability("memory.write", PluginEffect.WRITE, True),
        ),
    )


def _load_package_catalog(
    catalog_id: str,
) -> tuple[tuple[NativePluginManifest, ...], Mapping[str, NativePluginFactory]]:
    """Issue only exact package-defined manifests and callable identities."""

    if catalog_id == _EMPTY_CATALOG:
        return (), MappingProxyType({})
    if catalog_id != _M23_FAKE_CATALOG:
        raise ValueError("unknown package catalog")
    manifests = tuple(
        _manifest(plugin_id, api_version)
        for plugin_id, api_version, _, _ in _FAKE_SPECS
    )
    factories: dict[str, NativePluginFactory] = {
        plugin_id: FakePluginFactory(open_status=open_status)
        for plugin_id, _, open_status, has_factory in _FAKE_SPECS
        if has_factory
    }
    return manifests, MappingProxyType(factories)


__all__: list[str] = []
