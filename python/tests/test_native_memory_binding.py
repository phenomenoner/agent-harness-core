import json

import pytest
from profile_test_support import profile_registry_payload

from ahc.cli.native_memory_binding import load_native_memory_binding, parse_native_memory_binding
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError


def document():
    return {
        "schema_version": "ahc.native-memory.bindings.v1",
        "bindings": {
            "hexi.durable-memory": {
                "agent_id": "hexi",
                "scope": "hexi",
                "database": "D:/memory/store.sqlite",
                "provider_root": "D:/memory/provider",
                "provider_digest": "a" * 64,
                "store_policy": "review",
                "observe_granted": True,
                "store_granted": False,
                "timeout": 30,
            }
        },
    }


def parse(value):
    return parse_native_memory_binding(
        value, registry=AgentProfileRegistry.from_dict(profile_registry_payload()), agent_id="hexi"
    )


def test_valid_is_immutable_and_preview_hides_locations():
    raw = document()
    binding = parse(raw)
    before = binding.digest
    raw["bindings"]["hexi.durable-memory"]["database"] = "D:/other.sqlite"
    assert binding.digest == before
    assert "D:/" not in str(binding.preview())
    assert binding.preview()["provider_status"] == "not-inspected"


def test_linux_binding_version_preserves_scope_and_requires_posix():
    raw = document()
    raw["schema_version"] = "ahc.native-memory.bindings.v2"
    row = raw["bindings"]["hexi.durable-memory"]
    row.update(database="/home/ahc/data/memory.sqlite", provider_root="/home/ahc/provider")
    binding = parse(raw)
    assert binding.scope == "hexi" and binding.store_policy == "review"
    assert binding.observe_granted and not binding.store_granted
    row["database"] = "D:/memory/store.sqlite"
    with pytest.raises(ContractError):
        parse(raw)


@pytest.mark.parametrize("field", ["database", "provider_root"])
def test_linux_binding_rejects_windows_mounted_provider(field):
    raw = document()
    raw["schema_version"] = "ahc.native-memory.bindings.v2"
    row = raw["bindings"]["hexi.durable-memory"]
    row.update(database="/home/ahc/data/memory.sqlite", provider_root="/home/ahc/provider")
    row[field] = "/mnt/c/ahc/memory"
    with pytest.raises(ContractError):
        parse(raw)


@pytest.mark.parametrize(
    "field,value",
    [
        ("agent_id", "lizi"),
        ("scope", "Foreign Scope"),
        ("provider_digest", "bad"),
        ("store_policy", "auto"),
        ("store_granted", 1),
        ("timeout", True),
        ("database", "relative.sqlite"),
        ("timeout", float("nan")),
    ],
)
def test_bad_binding_denied(field, value):
    raw = document()
    raw["bindings"]["hexi.durable-memory"][field] = value
    with pytest.raises(ContractError):
        parse(raw)


def test_missing_owner_and_duplicates_denied(tmp_path):
    raw = document()
    raw["bindings"]["lizi.durable-memory"] = raw["bindings"].pop("hexi.durable-memory")
    with pytest.raises(ContractError):
        parse(raw)
    path = tmp_path / "binding.json"
    path.write_text('{"schema_version":1,"schema_version":2}')
    with pytest.raises(ContractError):
        load_native_memory_binding(
            path,
            registry=AgentProfileRegistry.from_dict(profile_registry_payload()),
            agent_id="hexi",
        )


def test_file_load_does_not_touch_configured_locations(tmp_path):
    raw = document()
    path = tmp_path / "binding.json"
    path.write_text(json.dumps(raw))
    binding = load_native_memory_binding(
        path, registry=AgentProfileRegistry.from_dict(profile_registry_payload()), agent_id="hexi"
    )
    assert binding.resource_id == "hexi.durable-memory"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("native", [False, True])
def test_foreground_cannot_mix_binding_locator_platforms(monkeypatch, native):
    from types import SimpleNamespace
    from ahc.cli import foreground_native_memory as composition
    raw = document()
    if native:
        raw["schema_version"] = "ahc.native-memory.bindings.v2"
        raw["bindings"]["hexi.durable-memory"].update(
            database="/home/ahc/memory.sqlite", provider_root="/home/ahc/provider")
    binding = parse(raw)
    monkeypatch.setattr(composition, "load_native_memory_binding", lambda *a, **kw: binding)
    value = {"schema_version": "ahc.foreground.config.v3" if native else "ahc.foreground.config.v2",
             "native_memory": {"binding_file": "unused", "binding_digest": binding.digest}}
    config = SimpleNamespace(to_dict=lambda: value, session=SimpleNamespace(agent_id="hexi"))
    assert composition.read_native_binding(config, object()) == binding
    value["schema_version"] = "ahc.foreground.config.v2" if native else "ahc.foreground.config.v3"
    with pytest.raises(ContractError):
        composition.read_native_binding(config, object())
