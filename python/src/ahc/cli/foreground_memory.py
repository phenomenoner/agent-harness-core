"""Explicit memory disable/absence without discovering or activating a provider."""

from ahc.adapters.unavailable_memory import UnavailableMemoryPort
from ahc.application.child_diagnostics import child_availability
from ahc.application.memory_diagnostics import inspect_memory
from ahc.domain.memory_availability import MemoryAvailability
from ahc.domain.sessions import ContractError


def parse_memory(raw):
    if "memory" not in raw:
        return {}
    value = raw["memory"]
    if type(value) is dict and value.get("enabled") is True:
        from .foreground_memory_pack import parse_pack

        return parse_pack(value, native=raw.get("schema_version") == "ahc.foreground.config.v3")
    if type(value) is not dict or set(value) != {"enabled"} or value["enabled"] is not False:
        raise ContractError("memory configuration supports explicit disabled only")
    return {"memory": {"enabled": False}}


def compose_foreground_memory(config=None):
    raw = {} if config is None else config.to_dict()
    configured = parse_memory(raw)
    status = ("unavailable" if configured and configured["memory"]["enabled"]
              else "disabled" if configured else "absent")
    return UnavailableMemoryPort(MemoryAvailability(status))


def inspect_foreground_config(path, *, profile_registry=None):
    from .foreground import read_foreground_config

    return inspect_foreground_document(
        read_foreground_config(path), profile_registry=profile_registry)


def inspect_foreground_document(document, *, profile_registry=None):
    from .foreground_config_native import parse_trusted_foreground_config
    from .foreground_memory_pack import compose_memory_pack, compose_memory_search, preview_memory
    from .foreground_native import preview_native
    from .foreground_profiles import bind_profile_configuration

    config = bind_profile_configuration(parse_trusted_foreground_config(document), profile_registry)
    compose_memory_pack(config, profile_registry)  # Validate binding; no provider import or call.
    compose_memory_search(config, profile_registry)
    from .foreground_native_memory import read_native_binding

    native_binding = read_native_binding(config, profile_registry)
    return {
        "configuration": "valid",
        "observation_scope": "configuration-only",
        **({"native_memory": native_binding.preview()} if native_binding is not None else {}),
        "backend": {"enabled": config.backend["enabled"], "status": "not-inspected"},
        "workspace": {"status": "not-inspected"},
        "memory": {**inspect_memory(compose_foreground_memory(config)),
                   **({"pack": preview_memory(config)["memory"]}
                      if config.to_dict().get("memory", {}).get("enabled") else {})},
        "children": child_availability(subagent_configured=config.backend["enabled"],
            minion_configured=config.to_dict().get("native_minion", {}).get(
                "profile", {}).get("enabled", False)),
        **preview_native(config),
    }
