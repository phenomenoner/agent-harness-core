"""Configured public snapshot recall, with no model or native workspace effects."""

import json
import os
import subprocess
import sys
from hashlib import sha256
from pathlib import Path

import pytest
from profile_test_support import profile_registry_payload
from test_foreground_config_v2 import valid_config
from test_foreground_trusted import config_file
from test_workspace_tools import Port

from ahc.adapters.memory_pack_snapshot import SnapshotPack
from ahc.application.memory_pack_service import MemoryPackUnavailable
from ahc.cli import foreground_trusted
from ahc.cli.foreground import compose_foreground_document
from ahc.cli.foreground_memory import inspect_foreground_document
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_inbox import RuntimeInbox


def memory_config(tmp_path, *, granted=True):
    return {"enabled": True, "kind": "openclaw-mem-snapshot-pack",
            "resource_id": "agent.durable-memory", "scope": "agent",
            "snapshot": str(tmp_path / "snapshot.sqlite"),
            "provider_root": str(tmp_path / "packages"), "granted": granted, "timeout": 15}


def registry():
    return AgentProfileRegistry.from_dict(profile_registry_payload(("agent", "other")))


@pytest.mark.parametrize("case", ["missing", "sidecar", "timeout", "provider-missing"])
def test_snapshot_failure_is_bounded_and_sanitized(tmp_path, monkeypatch, case):
    path = tmp_path / "snapshot.sqlite"
    calls = []
    if case != "missing":
        path.write_bytes(b"controlled-placeholder")
    if case == "sidecar":
        Path(str(path) + "-wal").write_bytes(b"held")
    if case != "provider-missing":
        def run(*args, **options):
            calls.append(options)
            assert options["timeout"] == 0.1
            raise subprocess.TimeoutExpired("private-command", 0.1)
        monkeypatch.setattr(subprocess, "run", run)
    adapter = SnapshotPack(snapshot=str(path), provider_root=str(tmp_path / "missing-provider"),
                           timeout=0.1 if case == "timeout" else 15)
    with pytest.raises(MemoryPackUnavailable) as error:
        adapter("orchid")
    assert "private" not in str(error.value) and str(tmp_path) not in str(error.value)
    assert len(calls) == (1 if case == "timeout" else 0)


def test_doctor_binds_pack_without_opening_paths_or_spawning(tmp_path, monkeypatch):
    raw = valid_config()
    raw["memory"] = memory_config(tmp_path)
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: pytest.fail("diagnostic spawn"))
    report = inspect_foreground_document(raw, profile_registry=registry())
    assert report["memory"]["pack"]["provider_status"] == "not-inspected"
    assert str(tmp_path) not in json.dumps(report)
    with pytest.raises(ContractError):
        inspect_foreground_document(raw)
    raw["memory"]["resource_id"] = "other.durable-memory"
    with pytest.raises(ContractError):
        inspect_foreground_document(raw, profile_registry=registry())
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("granted", [True, False])
def test_enabled_foreground_recalls_public_snapshot_through_admitted_turn(
        tmp_path, monkeypatch, granted):
    from ahc.adapters import wsl_python_workspace, wsl_stream_runner

    provider = pytest.importorskip("openclaw_mem")
    packages = str(Path(provider.__file__).resolve().parent.parent)
    provider_root = Path(provider.__file__).resolve().parent
    provider_files = {path.relative_to(provider_root): sha256(path.read_bytes()).hexdigest()
                      for path in provider_root.rglob("*") if path.is_file()}
    original, request, raw, _ = config_file(tmp_path, enabled=True)
    raw["memory"] = memory_config(tmp_path, granted=granted)
    raw["memory"]["provider_root"] = packages
    snapshot = Path(raw["memory"]["snapshot"])
    config = tmp_path / "openclaw.json"
    config.write_text("{}", encoding="utf-8")
    env = {k: os.environ[k] for k in ("SystemRoot", "WINDIR") if k in os.environ}
    env.update({"HOME": str(tmp_path), "USERPROFILE": str(tmp_path),
                "OPENCLAW_CONFIG_PATH": str(config), "OPENCLAW_HOME": str(tmp_path)})
    prepare = (
        "import sys;sys.path.insert(0,sys.argv[1]);"
        "from openclaw_mem.core.api import connect,store_observation;"
        "c=connect(sys.argv[2]);"
        "store_observation(c,{'kind':'note','summary':'orchid owner',"
        "'detail':{'scope':'agent'}});"
        "store_observation(c,{'kind':'note','summary':'orchid other',"
        "'detail':{'scope':'other'}});c.commit();c.close()"
    )
    prepared = subprocess.run([sys.executable, "-I", "-B", "-c", prepare, packages, str(snapshot)],
                              cwd=tmp_path, env=env, capture_output=True, timeout=15,
                              creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    assert prepared.returncode == 0, prepared.stderr
    assert provider_files == {
        path.relative_to(provider_root): sha256(path.read_bytes()).hexdigest()
        for path in provider_root.rglob("*") if path.is_file()
    }, "snapshot preparation mutated the provider installation"
    before = sha256(snapshot.read_bytes()).hexdigest()
    observations = []

    class Factory:
        def __init__(self, store, config):
            self.config = config

        def recover(self, *args):
            pass

        def binding(self, tools):
            return "a" * 64, self.config.configuration_digest

        def __call__(self, request, tools, *, cancelled):
            class Backend:
                def invoke(self, admitted):
                    observations.append(tools.invoke("workspace.memory_pack", {
                        "expected": tools.workspace.identity.to_dict(), "query": "orchid",
                        "budget_tokens": 500, "max_items": 2}))
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                "thread", "turn", "done")

                def close(self):
                    pass

            return Backend()

    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", lambda *a, **kw: object())
    monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace",
                        lambda store, runner, identity, **kw: Port(identity, kw["checkpoint"]))
    runtime = compose_foreground_document(raw, profile_registry=registry())
    RuntimeInbox(original.store).submit(request.session, "recall", request.goal_digest, "orchid")
    if not granted:
        monkeypatch.setattr(subprocess, "Popen",
                            lambda *a, **kw: pytest.fail("denied recall spawn"))
    runtime.start()
    try:
        assert "workspace.memory_search" not in runtime.tools.capabilities()
        assert runtime.step().result["accepted"]
        assert runtime.step() is None
    finally:
        runtime.close()
    assert len(observations) == 1
    if granted:
        assert observations[0]["disposition"] == "observed"
        text = observations[0]["result"]["bundle_text"]
        assert "orchid owner" in text and "orchid other" not in text
    else:
        assert observations[0]["disposition"] == "rejected"
    assert sha256(snapshot.read_bytes()).hexdigest() == before
    assert not Path(str(snapshot) + "-wal").exists()
