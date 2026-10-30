"""Explicit Linux MemPalace binding; a model cannot choose providers or data roots."""

import math
import re
from dataclasses import asdict, dataclass

from ahc.adapters.local_embedding import MODEL_IDENTITY
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json


@dataclass(frozen=True, slots=True)
class MemPalaceMemoryBinding:
    agent_id: str
    resource_id: str
    scope: str
    root: str
    model_root: str
    credential_file: str
    palace_id: str
    qdrant_url: str
    provider_digest: str
    model_identity: str
    store_policy: str
    observe_granted: bool
    store_granted: bool
    timeout: float

    @property
    def digest(self):
        return digest_json(asdict(self))

    def preview(self):
        return {
            "kind": "mempalace-qdrant-local",
            "resource_id": self.resource_id,
            "provider_digest": self.provider_digest,
            "model_identity": self.model_identity,
            "store_policy": self.store_policy,
            "observe_granted": self.observe_granted,
            "store_granted": self.store_granted,
            "observation_scope": "configuration-only",
            "provider_status": "not-inspected",
        }


def parse_mempalace_binding(document, *, registry, agent_id):
    from .foreground_config_v2 import _native_path

    profile = registry.profile(agent_id)
    if (
        type(document) is not dict
        or set(document) != {"schema_version", "bindings"}
        or document["schema_version"] != "ahc.mempalace-memory.bindings.v1"
        or type(document["bindings"]) is not dict
        or not 1 <= len(document["bindings"]) <= 64
    ):
        raise ContractError("MemPalace binding document invalid")
    fields = set(MemPalaceMemoryBinding.__dataclass_fields__) - {"resource_id"}
    row = document["bindings"].get(profile.durable_memory_ref)
    if type(row) is not dict or set(row) != fields or row["agent_id"] != agent_id:
        raise ContractError("MemPalace owner binding invalid")
    for field in ("root", "model_root", "credential_file"):
        _native_path(row[field], "mempalace." + field)
    if (
        type(row["scope"]) is not str
        or not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]{0,255}", row["scope"])
        or type(row["palace_id"]) is not str
        or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", row["palace_id"])
        or type(row["qdrant_url"]) is not str
        or not re.fullmatch(r"http://127\.0\.0\.1:[0-9]{1,5}", row["qdrant_url"])
        or not 1 <= int(row["qdrant_url"].rsplit(":", 1)[1]) <= 65535
        or type(row["provider_digest"]) is not str
        or not re.fullmatch(r"[0-9a-f]{64}", row["provider_digest"])
        or row["model_identity"] != MODEL_IDENTITY
        or row["store_policy"] not in ("deny", "review", "trusted")
        or any(type(row[k]) is not bool for k in ("observe_granted", "store_granted"))
        or type(row["timeout"]) not in (int, float)
        or not math.isfinite(row["timeout"])
        or not 0 < row["timeout"] <= 120
    ):
        raise ContractError("MemPalace policy, provider or model identity invalid")
    return MemPalaceMemoryBinding(resource_id=profile.durable_memory_ref, **row)
