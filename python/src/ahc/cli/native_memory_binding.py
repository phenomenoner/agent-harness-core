"""Explicit native memory configuration; never imports or initializes a provider."""

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath

from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json


@dataclass(frozen=True, slots=True)
class NativeMemoryBinding:
    agent_id: str
    resource_id: str
    scope: str
    database: str
    provider_root: str
    provider_digest: str
    store_policy: str
    observe_granted: bool
    store_granted: bool
    timeout: float

    def preview(self):
        return {
            "kind": "openclaw-mem-native",
            "resource_id": self.resource_id,
            "provider_digest": self.provider_digest,
            "store_policy": self.store_policy,
            "observe_granted": self.observe_granted,
            "store_granted": self.store_granted,
            "observation_scope": "configuration-only",
            "provider_status": "not-inspected",
        }

    @property
    def digest(self):
        return digest_json({name: getattr(self, name) for name in self.__dataclass_fields__})


def parse_native_memory_binding(document, *, registry, agent_id):
    if type(registry) is not AgentProfileRegistry:
        raise ContractError("native memory profile registry required")
    if (type(document) is dict
            and document.get("schema_version") == "ahc.mempalace-memory.bindings.v1"):
        from .mempalace_memory_binding import parse_mempalace_binding
        return parse_mempalace_binding(document, registry=registry, agent_id=agent_id)
    profile = registry.profile(agent_id)
    if (
        type(document) is not dict
        or set(document) != {"schema_version", "bindings"}
        or document["schema_version"] not in ("ahc.native-memory.bindings.v1", "ahc.native-memory.bindings.v2")
        or type(document["bindings"]) is not dict
        or not 1 <= len(document["bindings"]) <= 64
    ):
        raise ContractError("native memory binding document invalid")
    resource = profile.durable_memory_ref
    row = document["bindings"].get(resource)
    fields = {
        "agent_id",
        "scope",
        "database",
        "provider_root",
        "provider_digest",
        "store_policy",
        "observe_granted",
        "store_granted",
        "timeout",
    }
    if type(row) is not dict or set(row) != fields or row["agent_id"] != agent_id:
        raise ContractError("native memory owner binding missing or invalid")
    for name in ("database", "provider_root"):
        value = row[name]
        if (
            type(value) is not str
            or not value
            or len(value) > 4096
            or any(ord(c) < 32 or 0xD800 <= ord(c) <= 0xDFFF for c in value)
        ):
            raise ContractError("native memory locator invalid")
        if document["schema_version"] == "ahc.native-memory.bindings.v2":
            from .foreground_config_v2 import _native_path
            _native_path(value, "native_memory." + name)
        elif not PureWindowsPath(value).is_absolute():
            raise ContractError("native memory locator invalid")
    if (
        type(row["scope"]) is not str
        or not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]{0,255}", row["scope"])
        or type(row["provider_digest"]) is not str
        or not re.fullmatch(r"[0-9a-f]{64}", row["provider_digest"])
        or row["store_policy"] not in ("deny", "review", "trusted")
        or any(type(row[name]) is not bool for name in ("observe_granted", "store_granted"))
        or type(row["timeout"]) not in (int, float)
        or not math.isfinite(row["timeout"])
        or not 0 < row["timeout"] <= 120
    ):
        raise ContractError("native memory policy or bounds invalid")
    return NativeMemoryBinding(resource_id=resource, **row)


def load_native_memory_binding(path, *, registry, agent_id):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError()
        document = json.loads(
            raw,
            object_pairs_hook=unique,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError()),
        )
    except (OSError, ValueError, TypeError, RecursionError):
        raise ContractError("native memory binding file unavailable") from None
    return parse_native_memory_binding(document, registry=registry, agent_id=agent_id)
