import importlib.util
import json
import socket
import subprocess
from pathlib import Path

import pytest
from skill_test_support import goal

from ahc.cli import main
from ahc.cli.foreground import compose_foreground
from ahc.domain.sessions import ContractError

spec = importlib.util.spec_from_file_location(
    "prepare_foreground", Path(__file__).parents[1] / "tools/prepare_foreground.py"
)
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)


def options():
    return dict(goal=goal().to_dict(), agent_id="agent", session_id="session", route_id="local",
                bundle="/bundle", codex_home="/home/ahc/codex", model="fixture", cwd="/work")


def test_workspace_derives_bundled_paths_and_retains_luna_max(tmp_path, monkeypatch):
    from ahc.adapters.codex_linux_bundle import LINUX_CODEX_PIN

    values = options()
    del values["bundle"], values["codex_home"]
    values.update(model="gpt-5.6-luna", reasoning_effort="max")
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: pytest.fail("unexpected launch"))
    target = tmp_path / "prepared"
    tool.prepare(target, **values)
    backend = json.loads((target / "foreground.json").read_text())["backend"]
    release = LINUX_CODEX_PIN["version"] + "-" + LINUX_CODEX_PIN["target"]
    assert backend["bundle"] == "/work/.ahc/tools/codex/releases/" + release
    assert backend["codex_home"] == "/work/.ahc/codex-home"
    assert backend["model"] == "gpt-5.6-luna" and backend["reasoning_effort"] == "max"
    assert backend["enabled"] is False


def test_prepared_files_compose_and_doctor_without_external_effect(tmp_path, monkeypatch, capsys):
    def forbidden(*a, **kw):
        pytest.fail("preparation or disabled foreground attempted external effect")
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    target = tmp_path / "prepared"
    receipt = tool.prepare(target, **options())
    assert receipt["backend_enabled"] is False and receipt["workspace_granted"] is False
    assert receipt["native_paths_verified"] is False
    runtime = compose_foreground(target / "foreground.json")
    try:
        runtime.start()
        assert runtime.step() is None
        current = runtime.store.read_current("session")
        assert current.session.to_dict() == json.loads((target / "session.json").read_text())
        assert current.goal.digest == receipt["goal_digest"]
    finally:
        runtime.close()
    assert main(["doctor", "--config", str(target / "app.toml"), "--runtime-config",
                 str(target / "foreground.json")]) == 0
    assert json.loads(capsys.readouterr().out)["runtime"]["configuration"] == "valid"


def test_existing_destination_is_never_reused(tmp_path):
    marker = tmp_path / "keep"
    marker.write_bytes(b"user work")
    with pytest.raises(FileExistsError):
        tool.prepare(tmp_path, **options())
    assert list(tmp_path.iterdir()) == [marker] and marker.read_bytes() == b"user work"


@pytest.mark.parametrize("field,value", [("model", ""), ("bundle", "relative")])
def test_invalid_config_creates_nothing(tmp_path, field, value):
    target = tmp_path / "invalid"
    with pytest.raises(ContractError):
        tool.prepare(target, **{**options(), field: value})
    assert not target.exists()
