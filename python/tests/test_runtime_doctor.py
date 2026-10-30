"""Opt-in doctor observes exact stored authority without initialization or live probes."""

import hashlib
import json
import socket
import subprocess

import pytest
from test_foreground_config_v2 import valid_config
from test_runtime_core import setup

from ahc.cli import main
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("mode", ["observed", "absent", "blocked", "degraded", "indeterminate"])
def test_opted_in_doctor_reads_only_exact_runtime(tmp_path, monkeypatch, capsys, mode):
    repo, snapshot, request = setup(tmp_path)
    repo.begin(snapshot, request)
    raw = valid_config()
    raw.update(runtime_root=str(repo.root), session=snapshot.session.to_dict())
    if mode == "absent":
        raw["runtime_root"] = str(tmp_path / "missing")
    elif mode == "blocked":
        raw["session"] = snapshot.session.successor().to_dict()
    elif mode == "degraded":
        repo._mutate(lambda c: (c.execute("DROP TABLE workspace_heads"),
                               c.execute("CREATE TABLE workspace_heads (invalid TEXT)")))
    elif mode == "indeterminate":
        repo.db_path.write_bytes(b"not a database")
    app = tmp_path / "app.toml"
    app.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    config = tmp_path / "foreground.json"
    config.write_text(json.dumps(raw))

    def inventory():
        return {str(p.relative_to(tmp_path)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in tmp_path.rglob("*") if p.is_file()}

    before = inventory()

    def forbidden(*args, **kwargs):
        pytest.fail("doctor attempted initialization, write, or live probe")

    monkeypatch.setattr(RuntimeRepository, "initialize", forbidden)
    monkeypatch.setattr(RuntimeRepository, "_mutate", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    assert main(["doctor", "--config", str(app), "--runtime-config", str(config),
                 "--inspect-runtime"]) == 0
    report = json.loads(capsys.readouterr().out)["runtime"]["persistence"]
    assert report["status"] == mode and report["live_probe"] is False
    if mode in ("observed", "degraded"):
        assert report["domains"]["turns"]["state_counts"] == {"submitted": 1}
    if mode == "degraded":
        assert report["domains"]["workspaces"] == {"status": "unreadable"}
    if mode == "blocked":
        assert "domains" not in report
    assert inventory() == before
    assert "question" not in json.dumps(report)


def test_cli_reports_two_platform_credentials_without_resolving_tokens(
        tmp_path, monkeypatch, capsys):
    from ahc.domain.channel_ingress import ChannelRoute
    from ahc.domain.sessions import VirtualSessionIdentity

    raw = valid_config()
    raw["runtime_root"] = str(tmp_path / "absent")
    session = VirtualSessionIdentity.from_dict(raw["session"])
    raw["channels"] = [
        {"route": ChannelRoute(platform, "123", "456", "789", session).to_dict(),
         "enabled": True, "token_env": key}
        for platform, key in (("telegram", "AHC_DOCTOR_TELEGRAM"),
                              ("discord", "AHC_DOCTOR_DISCORD"))]
    monkeypatch.delenv("AHC_DOCTOR_TELEGRAM", raising=False)
    monkeypatch.setenv("AHC_DOCTOR_DISCORD", "private-fixture-token")
    app = tmp_path / "app.toml"
    app.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    config = tmp_path / "foreground.json"
    config.write_text(json.dumps(raw))

    def forbidden(*args, **kwargs):
        pytest.fail("doctor attempted runtime initialization or platform access")

    monkeypatch.setattr(RuntimeRepository, "initialize", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    assert main(["doctor", "--config", str(app), "--runtime-config", str(config),
                 "--inspect-runtime"]) == 0
    output = capsys.readouterr().out
    report = json.loads(output)["runtime"]
    assert report["persistence"]["status"] == "absent"
    channels = report["channel_credentials"]
    assert channels["status"] == "blocked"
    assert [row["credential"] for row in channels["bindings"]] == [
        "missing-or-empty", "present-unverified"]
    assert "private-fixture-token" not in output
    assert "AHC_DOCTOR_" not in output
    assert not (tmp_path / "absent").exists()


@pytest.mark.parametrize("present", [False, True])
def test_doctor_workspace_preflight_reports_same_missing_launcher_without_execution(
        tmp_path, monkeypatch, capsys, present):
    raw = valid_config()
    raw["backend"]["enabled"] = True
    raw["runtime_root"] = str(tmp_path / "missing-runtime")
    launcher = tmp_path / "launcher.fixture"
    if present:
        launcher.write_text("not an executable")
    raw["workspace"]["wsl_executable"] = str(launcher)
    app = tmp_path / "app.toml"
    app.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    config = tmp_path / "foreground.json"
    config.write_text(json.dumps(raw))

    def forbidden(*args, **kwargs):
        pytest.fail("doctor preflight attempted initialization or execution")

    monkeypatch.setattr(RuntimeRepository, "initialize", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    assert main(["doctor", "--config", str(app), "--runtime-config", str(config),
                 "--inspect-runtime"]) == 0
    report = json.loads(capsys.readouterr().out)["runtime"]["workspace"]
    assert report["status"] == ("present-unverified" if present else "unavailable")
    assert report["execution_verified"] is False
    assert report["reason_code"] == (None if present else "host-launcher-unavailable")
    assert not (tmp_path / "missing-runtime").exists()
