import json
import time
from types import SimpleNamespace

import pytest
from profile_test_support import profile_registry_payload
from skill_test_support import goal
from test_foreground_config_v2 import valid_config
from test_memory_store_service import success
from test_native_memory_binding import document

from ahc.cli.foreground_trusted import compose_trusted_foreground
from ahc.cli.native_memory_binding import parse_native_memory_binding
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.memory_store_approvals import MemoryStoreApprovals
from ahc.infrastructure.runtime_repository import RuntimeRepository


def configured(tmp_path):
    registry = AgentProfileRegistry.from_dict(profile_registry_payload())
    session = VirtualSessionIdentity.initial("native-session", "hexi", "route")
    repo = RuntimeRepository.initialize(tmp_path / "runtime")
    repo.create(WorkingSetSnapshot.initial(session, goal()))
    bindingdoc = document()
    bindingdoc["bindings"]["hexi.durable-memory"]["store_granted"] = True
    path = tmp_path / "binding.json"
    path.write_text(json.dumps(bindingdoc))
    binding = parse_native_memory_binding(bindingdoc, registry=registry, agent_id="hexi")
    raw = valid_config()
    raw["session"] = session.to_dict()
    raw["runtime_root"] = str(repo.root)
    raw["backend"]["enabled"] = True
    raw["policy"]["workspace_granted"] = True
    raw["native_memory"] = {"binding_file": str(path), "binding_digest": binding.digest}
    return raw, registry, session, path


def test_foreground_review_tool_requires_owner_before_dispatch(tmp_path, monkeypatch):
    from ahc.cli import foreground_native_memory

    raw, registry, session, _ = configured(tmp_path)
    calls = []

    class Adapter:
        def __init__(self, **kw):
            pass

        def status(self):
            raise AssertionError("unexpected provider call")

        def get(self, **kw):
            raise AssertionError("unexpected provider call")

        def store(self, **kw):
            calls.append(kw)
            return success(**kw)

        def reconcile(self, **kw):
            raise AssertionError("unexpected provider call")

    monkeypatch.setattr(foreground_native_memory, "NativeMemoryProcess", Adapter)
    runtime = compose_trusted_foreground(raw, profile_registry=registry)
    workspace = SimpleNamespace(
        store=runtime.store,
        identity=WorkspaceIdentity(session, "work"),
        closed=False,
        close=lambda: None,
    )
    runtime.open_workspace = lambda current: workspace
    runtime.start()
    try:
        assert "workspace.memory_native" in runtime.tools.capabilities()

        def invoke(action, payload):
            return runtime.tools.invoke(
                "workspace.memory_native",
                {"expected": workspace.identity.to_dict(), "action": action, "payload": payload},
            )

        proposal = invoke(
            "propose",
            {
                "operation_id": "one",
                "observation": {"kind": "note", "summary": "orchid", "detail": {}},
                "origin": "agent",
                "expires_at": time.time() + 300,
            },
        )
        digest = proposal["result"]["proposal_digest"]
        assert calls == []
        assert invoke("execute", {"proposal_digest": digest})["disposition"] == "rejected"
        assert invoke("approve", {"proposal_digest": digest})["disposition"] == "rejected"
        MemoryStoreApprovals(runtime.store).approve(
            session, digest, approval_expires=time.time() + 120
        )
        result = invoke("execute", {"proposal_digest": digest})
        assert result["result"]["status"] == "succeeded" and len(calls) == 1
        assert invoke("execute", {"proposal_digest": digest})["disposition"] == "rejected"
    finally:
        runtime.close()


def test_changed_binding_denied_before_adapter_construction(tmp_path, monkeypatch):
    from ahc.cli import foreground_native_memory

    raw, registry, _, path = configured(tmp_path)

    def forbidden(**kw):
        pytest.fail("adapter constructed")

    monkeypatch.setattr(foreground_native_memory, "NativeMemoryProcess", forbidden)
    value = json.loads(path.read_text())
    value["bindings"]["hexi.durable-memory"]["database"] = "D:/other.sqlite"
    path.write_text(json.dumps(value))
    with pytest.raises(ContractError):
        compose_trusted_foreground(raw, profile_registry=registry)


def test_doctor_checks_binding_without_provider_construction(tmp_path, monkeypatch):
    from ahc.cli import foreground_native_memory
    from ahc.cli.foreground_memory import inspect_foreground_document

    raw, registry, _, path = configured(tmp_path)

    def forbidden(**kw):
        pytest.fail("provider constructed by doctor")

    monkeypatch.setattr(foreground_native_memory, "NativeMemoryProcess", forbidden)
    result = inspect_foreground_document(raw, profile_registry=registry)
    assert result["native_memory"]["provider_status"] == "not-inspected"
    assert "D:/memory" not in str(result)
    value = json.loads(path.read_text())
    value["bindings"]["hexi.durable-memory"]["store_policy"] = "deny"
    path.write_text(json.dumps(value))
    with pytest.raises(ContractError):
        inspect_foreground_document(raw, profile_registry=registry)


@pytest.mark.parametrize("mode", ["absent", "incompatible"])
def test_unavailable_native_provider_preserves_foreground_and_no_initialization(tmp_path, mode):
    from ahc.adapters.memory_provider_identity import provider_digest
    from ahc.cli.foreground_memory import inspect_foreground_document

    raw, registry, session, path = configured(tmp_path)
    database = tmp_path / "never-created.sqlite"
    provider = tmp_path / "provider"
    bindingdoc = json.loads(path.read_text())
    value = bindingdoc["bindings"]["hexi.durable-memory"]
    value.update(database=str(database), provider_root=str(provider), timeout=5)
    if mode == "incompatible":
        api = provider / "openclaw_mem" / "core" / "api.py"
        api.parent.mkdir(parents=True)
        api.write_text("def unrelated_api(): return None\n")
        value["provider_digest"] = provider_digest(provider)
    path.write_text(json.dumps(bindingdoc))
    raw["native_memory"]["binding_digest"] = parse_native_memory_binding(
        bindingdoc, registry=registry, agent_id="hexi").digest
    before = {str(p): p.read_bytes() for p in provider.rglob("*") if p.is_file()}
    report = inspect_foreground_document(raw, profile_registry=registry)
    assert report["native_memory"]["provider_status"] == "not-inspected"
    assert not database.exists()
    runtime = compose_trusted_foreground(raw, profile_registry=registry)
    workspace = SimpleNamespace(store=runtime.store,
        identity=WorkspaceIdentity(session, "work"), closed=False, close=lambda: None)
    runtime.open_workspace = lambda current: workspace
    runtime.start()
    try:
        result = runtime.tools.invoke("workspace.memory_native", {
            "expected": workspace.identity.to_dict(), "action": "status", "payload": {}})
        assert result == {"disposition": "unavailable", "reason": "native-memory-unavailable",
                          "unresolved_effects": False}
        assert not workspace.closed
    finally:
        runtime.close()
    assert not database.exists()
    assert before == {str(p): p.read_bytes() for p in provider.rglob("*") if p.is_file()}
    assert not list(runtime.store.root.glob("native-memory-*"))
    if mode == "absent":
        assert not provider.exists()
