"""Opt-in native memory composition, independently bound from snapshot recall."""

import re
from pathlib import PureWindowsPath

from ahc.adapters.memory_native_process import NativeMemoryProcess
from ahc.application.memory_observe_service import MemoryObserveService
from ahc.application.memory_store_service import MemoryStoreService
from ahc.application.workspace_native_memory import WorkspaceNativeMemory
from ahc.domain.capability_values import CapabilityGrant
from ahc.domain.sessions import ContractError
from ahc.infrastructure.memory_store_approvals import MemoryStoreApprovals

from .native_memory_binding import load_native_memory_binding


def parse_native_memory_reference(raw):
    if "native_memory" not in raw:
        return {}
    value = raw["native_memory"]
    if (
        type(value) is not dict
        or set(value) != {"binding_file", "binding_digest"}
        or type(value["binding_file"]) is not str
        or any(ord(c) < 32 for c in value["binding_file"])
        or type(value["binding_digest"]) is not str
        or not re.fullmatch(r"[0-9a-f]{64}", value["binding_digest"])
    ):
        raise ContractError("native memory reference invalid")
    if raw.get("schema_version") == "ahc.foreground.config.v3":
        from .foreground_config_v2 import _native_path
        _native_path(value["binding_file"], "native_memory.binding_file")
    elif not PureWindowsPath(value["binding_file"]).is_absolute():
        raise ContractError("native memory reference invalid")
    return {"native_memory": dict(value)}


def read_native_binding(config, registry):
    reference = config.to_dict().get("native_memory")
    if reference is None:
        return None
    binding = load_native_memory_binding(
        reference["binding_file"], registry=registry, agent_id=config.session.agent_id
    )
    if binding.digest != reference["binding_digest"]:
        raise ContractError("native memory binding content changed")
    from .mempalace_memory_binding import MemPalaceMemoryBinding
    if isinstance(binding, MemPalaceMemoryBinding):
        if config.to_dict()["schema_version"] != "ahc.foreground.config.v3":
            raise ContractError("MemPalace requires the native Linux foreground")
        return binding
    from .foreground_config_v2 import _host_path, _native_path
    locator = (_native_path if config.to_dict()["schema_version"] == "ahc.foreground.config.v3"
               else _host_path)
    locator(binding.database, "native_memory.database")
    locator(binding.provider_root, "native_memory.provider_root")
    return binding


def compose_native_memory(config, registry, repository):
    binding = read_native_binding(config, registry)
    if binding is None:
        return None
    from .mempalace_memory_binding import MemPalaceMemoryBinding
    if isinstance(binding, MemPalaceMemoryBinding):
        from ahc.adapters.mempalace_memory_process import MemPalaceMemoryProcess
        adapter = MemPalaceMemoryProcess(binding=binding, scratch_root=repository.root)
    else:
        adapter = NativeMemoryProcess(
            database=binding.database,
            provider_root=binding.provider_root,
            provider_digest=binding.provider_digest,
            timeout=binding.timeout,
            scratch_root=repository.root,
        )
    grants = (
        tuple(
            CapabilityGrant(binding.agent_id, binding.resource_id, "memory.observe." + name)
            for name in ("get", "status")
        )
        if binding.observe_granted
        else ()
    )
    observe = MemoryObserveService(
        registry=registry,
        agent_id=binding.agent_id,
        resource_id=binding.resource_id,
        scope=binding.scope,
        grants=grants,
        status=adapter.status,
        get=adapter.get,
        resolve=getattr(adapter, "resolve", None),
    )
    store = MemoryStoreService(
        registry=registry,
        agent_id=binding.agent_id,
        resource_id=binding.resource_id,
        scope=binding.scope,
        provider_digest=binding.digest,
        policy=binding.store_policy,
        grants=(CapabilityGrant(binding.agent_id, binding.resource_id, "memory.store"),)
        if binding.store_granted
        else (),
        approvals=MemoryStoreApprovals(repository),
        store=adapter.store,
        reconcile=adapter.reconcile,
    )
    return WorkspaceNativeMemory(observe, store)
