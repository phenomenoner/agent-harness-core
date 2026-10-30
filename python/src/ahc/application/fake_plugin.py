"""Deterministic no-I/O native plugin adapter for M2.3 acceptance."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast

from ahc.domain import AgentProfileRegistry, ContractError, NativePluginManifest, PluginBinding

from .plugin_port import AdapterCallError, AdapterOpenResult, NativePluginAdapter

if TYPE_CHECKING:
    from .plugin_host import NativePluginHost

_OPEN_BEHAVIORS = {"ready", "degraded", "unavailable", "raise", "malformed"}
_FACTORY_PLUGIN_IDS = (
    "test.degraded",
    "test.disabled",
    "test.future",
    "test.memory",
    "test.open-exception",
    "test.open-malformed",
    "test.ready",
    "test.unavailable",
)


class FakePluginAdapter:
    def __init__(self, open_status: str) -> None:
        if open_status not in _OPEN_BEHAVIORS:
            raise ContractError("fake open_status is unsupported")
        self._open_status = open_status
        self.open_count = 0
        self.invoke_count = 0
        self.close_count = 0

    def open(self) -> AdapterOpenResult:
        self.open_count += 1
        if self._open_status == "raise":
            raise RuntimeError("RAW-PATH-SENTINEL")
        if self._open_status == "malformed":
            return cast(AdapterOpenResult, object())
        return AdapterOpenResult(self._open_status, f"fake-{self._open_status}")

    def invoke(self, capability_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.invoke_count += 1
        if payload.get("mode") == "fail-before":
            raise AdapterCallError("fake-failed", effect_may_have_occurred=False)
        if payload.get("mode") == "fail-after":
            raise AdapterCallError("fake-unknown", effect_may_have_occurred=True)
        if payload.get("mode") == "raise-generic":
            raise RuntimeError("RAW-SECRET-SENTINEL D:\\private")
        if payload.get("mode") == "non-json-result":
            return {"outer": {1: "integer-key"}}  # type: ignore[dict-item]
        return {"capability_id": capability_id, "payload": deepcopy(payload)}

    def close(self) -> None:
        self.close_count += 1


class FakePluginFactory:
    """Package-owned callable whose counters prove doctor/admission are effect-free."""

    def __init__(self, *, open_status: str = "ready") -> None:
        if open_status not in _OPEN_BEHAVIORS:
            raise ContractError("fake open_status is unsupported")
        self._open_status = open_status
        self.construct_count = 0
        self._adapters: list[FakePluginAdapter] = []

    def __call__(
        self, manifest: NativePluginManifest, binding: PluginBinding
    ) -> NativePluginAdapter:
        if manifest.plugin_id != binding.plugin_id:
            raise ContractError("fake factory manifest and binding differ")
        adapter = FakePluginAdapter(self._open_status)
        self.construct_count += 1
        self._adapters.append(adapter)
        return adapter

    @property
    def last_adapter(self) -> FakePluginAdapter:
        if not self._adapters:
            raise ContractError("fake adapter has not been constructed")
        return self._adapters[-1]


def create_fake_plugin_host(
    profile_registry: AgentProfileRegistry,
) -> tuple[NativePluginHost, Mapping[str, FakePluginFactory]]:
    """Create the exact package-owned M2.3 fake catalog; no caller callable is accepted."""

    from .plugin_host import NativePluginHost

    host = NativePluginHost._from_package_catalog(profile_registry, "m2.3-fakes")
    factories = {
        plugin_id: cast(FakePluginFactory, host._package_factory(plugin_id))
        for plugin_id in _FACTORY_PLUGIN_IDS
    }
    return host, MappingProxyType(factories)


__all__ = ["FakePluginAdapter", "FakePluginFactory", "create_fake_plugin_host"]
