"""Configuration-only binding of a profile's explicit snapshot recall provider."""

import math
from pathlib import PureWindowsPath

from ahc.adapters.memory_pack_snapshot import SnapshotPack
from ahc.application.memory_pack_service import MemoryPackService
from ahc.application.memory_search_service import MemorySearchService
from ahc.domain.capability_values import CapabilityGrant
from ahc.domain.sessions import ContractError


def parse_pack(value, *, native=False):
    if value.get("kind") in ("openclaw-mem-native-pack", "mempalace-qdrant-pack"):
        if value["kind"] == "mempalace-qdrant-pack" and not native:
            raise ContractError("MemPalace recall requires native Linux")
        fields = {"enabled", "kind", "resource_id", "scope", "granted", "timeout"}
        if (not fields <= set(value) <= fields | {"search_granted"}
                or value["enabled"] is not True or type(value["granted"]) is not bool
                or type(value.get("search_granted", False)) is not bool):
            raise ContractError("native recall configuration fields invalid")
        for field in ("resource_id", "scope"):
            text = value[field]
            if (type(text) is not str or not text or len(text) > 256
                    or any(ord(c) < 32 or 0xD800 <= ord(c) <= 0xDFFF for c in text)):
                raise ContractError("native recall binding text invalid")
        timeout = value["timeout"]
        if (type(timeout) not in (int, float) or not math.isfinite(timeout)
                or not 0 < timeout <= 60):
            raise ContractError("native recall timeout invalid")
        return {"memory": dict(value)}
    fields = {"enabled", "kind", "resource_id", "scope", "snapshot", "provider_root",
              "granted", "timeout"}
    if (not fields <= set(value) <= fields | {"search_granted"} or value["enabled"] is not True
            or value["kind"] != "openclaw-mem-snapshot-pack"
            or type(value["granted"]) is not bool):
        raise ContractError("memory Pack configuration fields invalid")
    if type(value.get("search_granted", False)) is not bool:
        raise ContractError("memory search grant invalid")
    for field in ("resource_id", "scope", "snapshot", "provider_root"):
        text = value[field]
        if (type(text) is not str or not text or len(text) > 4096
                or any(ord(c) < 32 or 0xD800 <= ord(c) <= 0xDFFF for c in text)):
            raise ContractError("memory Pack configuration text invalid")
    for field in ("snapshot", "provider_root"):
        if native:
            from .foreground_config_v2 import _posix_path
            _posix_path(value[field], "memory." + field)
        elif not PureWindowsPath(value[field]).is_absolute():
            raise ContractError("memory Pack paths must be absolute host paths")
    timeout = value["timeout"]
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 60:
        raise ContractError("memory Pack timeout invalid")
    return {"memory": dict(value)}


def compose_memory_pack(config, registry):
    value = config.to_dict().get("memory", {})
    if not value.get("enabled"):
        return None
    agent = config.session.agent_id
    resource = value["resource_id"]
    return MemoryPackService(
        registry=registry, agent_id=agent, resource_id=resource, scope=value["scope"],
        grants=(CapabilityGrant(agent, resource, "memory.pack"),) if value["granted"] else (),
        pack=_adapter(config, registry, value),
    )


def preview_memory(config):
    value = config.to_dict().get("memory")
    if value is None or not value["enabled"]:
        return {} if value is None else {"memory": {"enabled": False}}
    return {"memory": {"enabled": True, "kind": value["kind"], "granted": value["granted"],
                       "operations": ["pack"] + (["search"] if value.get("search_granted") else []),
                       "observation_scope": "configuration-only",
                       "provider_status": "not-inspected"}}


def compose_memory_search(config, registry):
    value = config.to_dict().get("memory", {})
    if not value.get("search_granted"):
        return None
    agent, resource = config.session.agent_id, value["resource_id"]
    adapter = _adapter(config, registry, value)
    return MemorySearchService(registry=registry, agent_id=agent, resource_id=resource,
                               scope=value["scope"], search=adapter.search,
                               grants=(CapabilityGrant(agent, resource, "memory.search"),))


def _adapter(config, registry, value):
    if value["kind"] == "mempalace-qdrant-pack":
        from ahc.adapters.mempalace_memory_process import MemPalaceMemoryProcess

        from .foreground_native_memory import read_native_binding
        from .mempalace_memory_binding import MemPalaceMemoryBinding

        binding = read_native_binding(config, registry)
        if (not isinstance(binding, MemPalaceMemoryBinding)
                or binding.resource_id != value["resource_id"] or binding.scope != value["scope"]):
            raise ContractError("MemPalace recall differs from native memory binding")
        return MemPalaceMemoryProcess(binding=binding, scratch_root=config.runtime_root)
    if value["kind"] == "openclaw-mem-native-pack":
        from ahc.adapters.memory_live_pack import LiveMemoryPack

        from .foreground_native_memory import read_native_binding

        binding = read_native_binding(config, registry)
        if (binding is None or binding.resource_id != value["resource_id"]
                or binding.scope != value["scope"]):
            raise ContractError("native recall differs from native memory binding")
        return LiveMemoryPack(binding=binding, scratch_root=config.runtime_root,
                              timeout=value["timeout"])
    return SnapshotPack(snapshot=value["snapshot"], provider_root=value["provider_root"],
                        timeout=value["timeout"])
