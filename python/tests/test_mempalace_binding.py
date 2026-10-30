"""No-process binding admission and compatibility with existing foreground authority."""

import pytest
from profile_test_support import profile_registry_payload
from test_foreground_config_native import native_config

from ahc.adapters.local_embedding import MODEL_IDENTITY
from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.cli.foreground_memory_pack import parse_pack
from ahc.cli.native_memory_binding import parse_native_memory_binding
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError


def document():
    return {"schema_version": "ahc.mempalace-memory.bindings.v1", "bindings": {
        "hexi.durable-memory": {
            "agent_id": "hexi", "scope": "private-hexi", "root": "/home/ahc/memory",
            "model_root": "/home/ahc/models/embedding", "credential_file": "/home/ahc/keys/qdrant",
            "palace_id": "hexi-memory", "qdrant_url": "http://127.0.0.1:6333",
            "provider_digest": "a" * 64, "model_identity": MODEL_IDENTITY,
            "store_policy": "trusted", "observe_granted": True, "store_granted": True,
            "timeout": 60}}}


def parse(value):
    return parse_native_memory_binding(value,
        registry=AgentProfileRegistry.from_dict(profile_registry_payload()), agent_id="hexi")


def test_explicit_provider_identity_and_no_implicit_old_provider_selection():
    binding = parse(document())
    assert binding.preview()["kind"] == "mempalace-qdrant-local"
    assert binding.preview()["provider_status"] == "not-inspected"
    assert "/home/" not in str(binding.preview())
    changed = document()
    changed["bindings"]["hexi.durable-memory"]["model_root"] = "/home/ahc/models/other"
    assert parse(changed).digest != binding.digest


@pytest.mark.parametrize("field,value", [
    ("agent_id", "lizi"), ("scope", "../foreign scope"), ("root", "D:/memory"),
    ("qdrant_url", "https://example.com:6333"), ("qdrant_url", "http://127.0.0.1:99999"),
    ("qdrant_url", "http://user:password@127.0.0.1:6333"),
    ("model_identity", "b" * 64), ("provider_digest", ""), ("timeout", float("nan")),
    ("store_granted", 1), ("store_policy", "unlimited"),
])
def test_foreign_ambiguous_or_unpinned_binding_rejected(field, value):
    raw = document()
    raw["bindings"]["hexi.durable-memory"][field] = value
    with pytest.raises(ContractError):
        parse(raw)


def test_recall_kind_is_native_only_and_does_not_enable_grants():
    config = native_config()
    memory = {"enabled": True, "kind": "mempalace-qdrant-pack", "resource_id": "hexi.durable-memory",
              "scope": "private-hexi", "granted": False, "search_granted": False, "timeout": 60}
    config["memory"] = memory
    assert parse_foreground_config_native(config).to_dict()["memory"] == memory
    with pytest.raises(ContractError):
        parse_pack(memory, native=False)


@pytest.mark.parametrize("field", ["root", "model_root", "credential_file"])
def test_mempalace_rejects_windows_mounted_core_paths(field):
    raw = document()
    raw["bindings"]["hexi.durable-memory"][field] = "/mnt/c/ahc/memory"
    with pytest.raises(ContractError):
        parse(raw)
