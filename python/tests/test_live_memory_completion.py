import hashlib
from types import SimpleNamespace

import pytest
from test_foreground_native_memory import configured
from test_memory_store_service import setup_service, success

from ahc.adapters.memory_live_pack import LiveMemoryPack
from ahc.application.memory_pack_service import MemoryPackUnavailable
from ahc.application.workspace_native_memory import WorkspaceNativeMemory
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_memory_pack import compose_memory_pack
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity


def test_native_tool_schema_exposes_nested_identity_and_object_detail():
    from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
    from ahc.domain.sessions import VirtualSessionIdentity

    identity = WorkspaceIdentity(VirtualSessionIdentity.initial("schema", "hexi", "local"), "work")
    request = SimpleNamespace(capabilities=("workspace.memory_native",))
    tools = SimpleNamespace(workspace=SimpleNamespace(identity=identity))
    schema = CodexWorkspaceTools.describe(request, tools)[0]["tools"][0]["inputSchema"]
    session = schema["properties"]["expected"]["properties"]["session"]
    # The observed rejected call invented session.generation and sent detail as text.
    assert session["type"] == "object"
    assert session["additionalProperties"] is False
    assert "generation" not in session["properties"]
    assert session["properties"]["ref"]["properties"]["generation"]["const"] == 1
    observation = schema["properties"]["payload"]["properties"]["observation"]
    assert observation["required"] == ["kind", "summary", "detail"]
    assert observation["properties"]["detail"]["type"] == "object"


@pytest.mark.parametrize("policy,grant", [("deny", True), ("review", True), ("trusted", False)])
def test_trusted_store_is_explicit_and_granted(tmp_path, policy, grant):
    calls = []
    service, _, session, _ = setup_service(tmp_path, lambda **kw: calls.append(kw),
                                          lambda **kw: None, policy=policy, grant=grant)
    with pytest.raises(ContractError):
        service.store_trusted(session, operation_id="one",
                              observation={"kind": "note", "summary": "orchid", "detail": {}})
    assert not calls


def test_trusted_store_once_scope_and_unknown_custody(tmp_path):
    calls = []
    def store(**kw):
        calls.append(kw)
        return success(**kw)
    service, approvals, session, _ = setup_service(tmp_path, store, store, policy="trusted")
    observation = {"kind": "preference", "summary": "orchid", "detail": {}}
    with pytest.raises(ContractError):
        service.store_trusted(session, operation_id="foreign",
                              observation={**observation, "detail": {"scope": "other"}})
    tools = WorkspaceNativeMemory(None, service)
    workspace = SimpleNamespace(identity=WorkspaceIdentity(session, "work"))
    args = {"expected": workspace.identity.to_dict(), "action": "store",
            "payload": {"operation_id": "one", "observation": observation}}
    first = tools.invoke(workspace, args)
    assert first["result"]["status"] == "succeeded"
    assert approvals.query(session, first["result"]["proposal_digest"])["state"] == "succeeded"
    assert tools.invoke(workspace, args)["disposition"] == "rejected"
    assert len(calls) == 1
    service.store = lambda **kw: {"malformed": True}
    result = service.store_trusted(session, operation_id="two", observation=observation)
    assert result["status"] == "unknown"
    assert approvals.query(session, result["proposal_digest"])["state"] == "unknown"
    with pytest.raises(ContractError):
        service.store_trusted(session, operation_id="two", observation=observation)


def test_live_pack_takes_fresh_snapshot_and_rejects_bad_receipt(tmp_path, monkeypatch):
    from ahc.adapters import memory_live_pack
    content = [b"first"]
    corrupt = [False]
    def backup(_, target):
        target.mkdir()
        (target / "memory.sqlite").write_bytes(content[0])
        return {"schema": "openclaw-mem.native.backup.v1", "database": "memory.sqlite",
                "activation_allowed": False, "byte_count": len(content[0]),
                "sha256": "bad" if corrupt[0] else hashlib.sha256(content[0]).hexdigest()}
    monkeypatch.setattr(memory_live_pack.NativeMemoryProcess, "backup", backup)
    class Reader:
        def __init__(self, *, snapshot, **kwargs):
            self.snapshot = snapshot
        def __call__(self, query, **options):
            from pathlib import Path
            return Path(self.snapshot).read_bytes()
        search = __call__
    monkeypatch.setattr(memory_live_pack, "SnapshotPack", Reader)
    binding = SimpleNamespace(database="unused", provider_root="unused", provider_digest="a" * 64)
    reader = LiveMemoryPack(binding=binding, scratch_root=tmp_path, timeout=10)
    assert reader("query") == b"first"
    content[0] = b"committed later"
    assert reader.search("query") == b"committed later"
    assert not list(tmp_path.iterdir())
    corrupt[0] = True
    with pytest.raises(MemoryPackUnavailable):
        reader("query")
    assert not list(tmp_path.iterdir())


def test_native_recall_binding_is_exact_and_doctor_has_no_provider_effect(tmp_path, monkeypatch):
    from ahc.adapters import memory_live_pack
    raw, registry, _, _ = configured(tmp_path)
    raw["memory"] = {"enabled": True, "kind": "openclaw-mem-native-pack",
                     "resource_id": "hexi.durable-memory", "scope": "hexi",
                     "granted": True, "search_granted": True, "timeout": 20}
    monkeypatch.setattr(memory_live_pack.NativeMemoryProcess, "backup",
                        lambda *a: pytest.fail("provider invoked by composition"))
    assert compose_memory_pack(parse_foreground_config_v2(raw), registry) is not None
    raw["memory"]["scope"] = "foreign"
    with pytest.raises(ContractError):
        compose_memory_pack(parse_foreground_config_v2(raw), registry)
