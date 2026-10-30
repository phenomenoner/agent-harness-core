"""Configuration diagnostics and composed standby do not probe a memory provider."""

import json
import socket
import subprocess

import pytest
from test_foreground_config_v2 import valid_config
from test_foreground_trusted import config_file

from ahc.application.child_diagnostics import child_availability
from ahc.application.memory_diagnostics import inspect_memory
from ahc.cli import main
from ahc.cli.foreground import compose_foreground
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("status", ["absent", "disabled"])
def test_doctor_observes_configured_absence_without_opening_runtime(
        tmp_path, monkeypatch, capsys, status):
    config = tmp_path / "app.toml"
    config.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    document = valid_config()
    if status == "disabled":
        document["memory"] = {"enabled": False}
    document["backend"]["enabled"] = True
    document["runtime_root"] = str(tmp_path / "missing-runtime")
    path = tmp_path / "runtime.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}

    def forbidden(*args, **kwargs):
        pytest.fail("configuration doctor attempted runtime or provider access")

    monkeypatch.setattr(RuntimeRepository, "__init__", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    assert main(["doctor", "--config", str(config), "--runtime-config", str(path)]) == 0
    output = capsys.readouterr().out
    report = json.loads(output)["runtime"]
    assert report == {
        "configuration": "valid",
        "observation_scope": "configuration-only",
        "backend": {"enabled": True, "status": "not-inspected"},
        "workspace": {"status": "not-inspected"},
        "memory": {"status": status, "reason_code": "memory-" + status},
        "children": child_availability(subagent_configured=True),
    }
    assert document["backend"]["codex_home"] not in output
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


@pytest.mark.parametrize("status", ["absent", "disabled"])
def test_composed_standby_exposes_same_absence_without_memory_effects(
        tmp_path, monkeypatch, status):
    _, _, document, path = config_file(tmp_path)
    if status == "disabled":
        document["memory"] = {"enabled": False}
        path.write_text(json.dumps(document), encoding="utf-8")
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: pytest.fail("unexpected process"))
    runtime = compose_foreground(path)
    runtime.start()
    try:
        assert runtime.memory_status() == {"status": status, "reason_code": "memory-" + status}
        assert runtime.memory.read().to_dict()["outcome"] == "unavailable"
        assert runtime.step() is None
    finally:
        runtime.close()


@pytest.mark.parametrize("value", [True, None, {}, {"enabled": True}, {"enabled": 0},
                                  {"enabled": False, "provider": "private"}])
def test_memory_config_rejects_unadmitted_provider_or_coerced_disable(value):
    from ahc.cli.foreground_config_v2 import parse_foreground_config_v2

    with pytest.raises(ContractError, match="memory"):
        parse_foreground_config_v2({**valid_config(), "memory": value})


def test_uncomposed_memory_does_not_claim_absence_or_health():
    assert inspect_memory(None)["status"] == "not-inspected"

    class Invalid:
        def status(self):
            return {"status": "healthy"}

    with pytest.raises(ContractError):
        inspect_memory(Invalid())
