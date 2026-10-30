"""Restore rebinds session authority while external memory remains separately owned."""

import json
from hashlib import sha256
from types import SimpleNamespace

from memory_snapshot_test_support import snapshot_fixture
from profile_test_support import profile_registry_payload
from test_foreground_config_v2 import valid_config
from test_runtime_core import setup

from ahc.application.workspace_tools import WorkspaceTools
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_memory_pack import compose_memory_pack, compose_memory_search
from ahc.cli.runtime_recovery_config import prepare_recovered_config
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_db import WORKING_DB_FILENAME


def test_recovered_config_preserves_external_snapshot_and_rebinds_recall(tmp_path):
    memory, packages = snapshot_fixture(tmp_path, ("agent", "other"))
    memory_hash = sha256(memory.read_bytes()).hexdigest()
    from pathlib import Path

    provider_bytecode = set(Path(packages).rglob("*.pyc"))
    repo, saved, _ = setup(tmp_path)
    raw = valid_config()
    raw["runtime_root"] = str(repo.root)
    raw["memory"] = {"enabled": True, "kind": "openclaw-mem-snapshot-pack",
                     "resource_id": "agent.durable-memory", "scope": "agent",
                     "snapshot": str(memory), "provider_root": packages,
                     "granted": True, "search_granted": True, "timeout": 15}
    parsed = parse_foreground_config_v2(raw)
    original = tmp_path / "original.json"
    original.write_text(json.dumps(raw), encoding="utf-8")
    original_bytes = original.read_bytes()
    backup, prepared, transformed = (tmp_path / n for n in ("backup", "prepared", "transformed"))
    source_digest = _digest(repo.root / WORKING_DB_FILENAME)
    manifest = backup_runtime_database(repo, backup)
    assert not any(p.name == memory.name for p in backup.rglob("*"))
    prepare_runtime_restore(backup, prepared)
    receipt = transform_runtime_restore(prepared, manifest["sha256"], (saved,),
                                        source_digest, transformed)
    activate_runtime_restore(repo, tmp_path / "supervisor.lock", transformed,
                             receipt["sha256"], manifest["sha256"], source_digest)
    output = tmp_path / "recovered-config"
    report = prepare_recovered_config(repo, original, parsed.configuration_digest, output,
                                      checkpoint_digest=None)
    recovered = parse_foreground_config_v2(json.loads((output / "foreground.json").read_text()))
    assert recovered.session != saved.session and not report["started"]
    assert recovered.to_dict()["memory"] == raw["memory"]
    assert not recovered.backend["enabled"]
    assert original.read_bytes() == original_bytes
    registry = AgentProfileRegistry.from_dict(profile_registry_payload(("agent", "other")))
    identity = WorkspaceIdentity(recovered.session, "work")
    tools = WorkspaceTools(SimpleNamespace(identity=identity, closed=False),
                           memory_pack=compose_memory_pack(recovered, registry),
                           memory_search=compose_memory_search(recovered, registry))
    stale = WorkspaceIdentity(saved.session, "work").to_dict()
    for name in ("workspace.memory_pack", "workspace.memory_search"):
        assert tools.invoke(name, {"expected": stale, "query": "orchid"})[
            "disposition"] == "rejected"
        result = tools.invoke(name, {"expected": identity.to_dict(), "query": "orchid"})
        assert result["disposition"] == "observed"
        assert "orchid agent" in str(result["result"])
        assert "orchid other" not in str(result["result"])
    assert sha256(memory.read_bytes()).hexdigest() == memory_hash
    assert set(Path(packages).rglob("*.pyc")) == provider_bytecode
