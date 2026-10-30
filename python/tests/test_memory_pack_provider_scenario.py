"""Disposable public-API Pack probe, separate from an admitted AHC adapter."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from profile_test_support import profile_registry_payload

import ahc

PROBE = r'''
import copy, hashlib, json, os, socket, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
sys.path.insert(0, sys.argv[2])
from ahc.domain.memory_pack_response import validate_pack_response
from ahc.domain.sessions import ContractError
from ahc.domain.sessions import VirtualSessionIdentity
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.capability_values import CapabilityGrant
from ahc.application.memory_pack_service import MemoryPackService
attempts = []
def no_network(event, args):
    if event in ("socket.connect", "socket.getaddrinfo", "subprocess.Popen", "os.system"):
        attempts.append(event)
        raise RuntimeError("probe external effect forbidden")
sys.addaudithook(no_network)
from openclaw_mem.core.api import connect, store_observation, pack
storage = sys.argv[4]
conn = connect(":memory:" if storage == "memory" else "fixture.sqlite")
try:
    for owner in ("agent-a", "agent-b"):
        store_observation(conn, {"kind": "note", "summary": "orchid " + owner,
                                 "detail": {"scope": owner, "importance": 0.8}})
    conn.commit()
    snapshot = None
    if storage == "snapshot":
        conn.close()
        snapshot = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in Path.cwd().iterdir() if p.is_file()}
        os.environ["OPENCLAW_MEM_READONLY_DB"] = "1"
        conn = connect("fixture.sqlite")
    before = hashlib.sha256(conn.serialize()).hexdigest()
    changes = conn.total_changes
    result = pack(conn, "orchid", scope="agent-a", limit=2, budget_tokens=500,
                  model="fixture-no-embeddings", vector_backend="python",
                  use_tracking=False, quota_enabled=False,
                  scoring_profile="relevance", scoring_relevance_enabled=True,
                  scoring_importance_enabled=False, scoring_recency_enabled=False,
                  scoring_use_enabled=False, scoring_state_enabled=False)
    assert result["items"], result
    assert "agent-a" in result["bundle_text"] and "agent-b" not in result["bundle_text"]
    assert result["citations"] and result["context_pack"]
    validated = validate_pack_response(result, query="orchid", scope="agent-a",
                                       budget_tokens=500, max_items=2)
    assert validated.summary()["item_count"] == len(result["items"])
    bound = MemoryPackService(
        registry=AgentProfileRegistry.from_dict(json.loads(sys.argv[3])),
        agent_id="agent-a", resource_id="agent-a.durable-memory", scope="agent-a",
        grants=(CapabilityGrant("agent-a", "agent-a.durable-memory", "memory.pack"),),
        pack=lambda query, **options: pack(conn, query, model="fixture-no-embeddings", **options),
    )
    admitted = bound.pack(VirtualSessionIdentity.initial("session", "agent-a", "route"),
                          "orchid", budget_tokens=500, max_items=2)
    assert admitted.to_dict()["items"] == validated.to_dict()["items"]
    from types import SimpleNamespace
    from ahc.application.workspace_tools import WorkspaceTools
    from ahc.domain.workspace import WorkspaceIdentity
    identity = WorkspaceIdentity(VirtualSessionIdentity.initial("session", "agent-a", "route"),
                                 "memory-workspace")
    tools = WorkspaceTools(SimpleNamespace(identity=identity, closed=False), memory_pack=bound)
    recalled = tools.invoke("workspace.memory_pack", {
        "expected": identity.to_dict(), "query": "orchid", "budget_tokens": 500, "max_items": 2})
    assert recalled["disposition"] == "observed"
    assert recalled["content_authority"] == "untrusted-data"
    expected_pack = admitted.to_dict()
    expected_pack["meta"]["ts"] = recalled["result"]["meta"]["ts"]
    assert recalled["result"] == expected_pack  # Each provider call has its own timestamp.
    changes_to_reject = [
        lambda r: r["citations"][0].update(recordRef="other"),
        lambda r: r["items"][0].update(summary="conflicting"),
        lambda r: r["budget"].update(includedItems=0),
        lambda r: r["lifecycle_write"].update(status="updated"),
        lambda r: r["lifecycle_write"]["mutation"].update(writes_observations=1),
        lambda r: r["lifecycle_write"]["mutation"].update(writes_observations=False),
        lambda r: r["lifecycle_write"]["selection"].update(selection_signature="0"*64),
        lambda r: r["lifecycle_write"]["storage"].update(error_code="OperationalError"),
    ]
    for change in changes_to_reject:
        conflicting = copy.deepcopy(result)
        change(conflicting)
        try:
            validate_pack_response(conflicting, query="orchid", scope="agent-a",
                                   budget_tokens=500, max_items=2)
        except ContractError:
            pass
        else:
            raise AssertionError("conflicting response accepted")
    assert conn.total_changes == changes
    assert hashlib.sha256(conn.serialize()).hexdigest() == before
    assert not attempts, attempts
    print(json.dumps({"status": "PASS", "items": len(result["items"]),
                      "lifecycle_write": result["lifecycle_write"],
                      "database_unchanged": True, "external_attempts": attempts}))
finally:
    conn.close()
if snapshot is not None:
    after_files = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                   for p in Path.cwd().iterdir() if p.is_file()}
    assert snapshot == after_files
'''


@pytest.mark.parametrize("storage", ["memory", "snapshot"])
def test_public_pack_nonempty_scope_and_no_mutation_in_disposable_process(tmp_path, storage):
    provider = pytest.importorskip("openclaw_mem")
    packages = str(Path(provider.__file__).resolve().parent.parent)
    config = tmp_path / "openclaw.json"
    config.write_text("{}", encoding="utf-8")
    mem_config = tmp_path / "memory.toml"
    mem_config.write_text("", encoding="utf-8")
    environment = {k: os.environ[k] for k in ("SystemRoot", "WINDIR") if k in os.environ}
    environment.update({
        "USERPROFILE": str(tmp_path), "HOME": str(tmp_path), "TEMP": str(tmp_path),
        "TMP": str(tmp_path), "OPENCLAW_HOME": str(tmp_path),
        "OPENCLAW_STATE_DIR": str(tmp_path / "state"),
        "OPENCLAW_CONFIG_PATH": str(config), "OPENCLAW_MEM_CONFIG": str(mem_config),
    })
    source = str(Path(ahc.__file__).parent.parent)
    registry = json.dumps(profile_registry_payload(("agent-a", "agent-b")))
    command = [sys._base_executable, "-I", "-B", "-c", PROBE, packages, source, registry, storage]
    result = subprocess.run(command,
                            cwd=tmp_path, env=environment, capture_output=True,
                            text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    receipt = json.loads(result.stdout)
    assert receipt["status"] == "PASS" and receipt["database_unchanged"]
    assert receipt["external_attempts"] == []
