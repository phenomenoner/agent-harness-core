"""A store-only status read cannot infer optional service availability."""

import json
from hashlib import sha256

from skill_test_support import goal

from ahc.cli.main import main
from ahc.domain.sessions import VirtualSessionIdentity


def test_status_never_probes_backend_or_changes_durable_files(tmp_path, capsys, monkeypatch):
    root = tmp_path / "runtime"
    owner = VirtualSessionIdentity.initial("status-session", "agent", "route")
    args = ["--runtime-root", str(root), "--session-json", json.dumps(owner.to_dict())]
    assert main(["runtime-open", *args, "--goal-json", json.dumps(goal().to_dict())]) == 0
    capsys.readouterr()

    def forbidden(*args, **kwargs):
        raise AssertionError("status must not construct or probe a backend")

    monkeypatch.setattr("ahc.cli.runtime._backend", forbidden)
    monkeypatch.setattr("subprocess.Popen", forbidden)

    def persisted():
        return {p.name: sha256(p.read_bytes()).hexdigest() for p in root.rglob("*") if p.is_file()}

    before = persisted()
    assert main(["runtime-status", *args]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["working_set"]["session"] == owner.to_dict()
    assert set(result["capabilities"]) == {
        "live_backend",
        "workspace",
        "memory",
        "channel_delivery",
    }
    assert all(value == "not-inspected" for value in result["capabilities"].values())
    assert persisted() == before
