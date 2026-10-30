"""Generated recovery configuration drives the real local foreground supervisor."""

import json

from test_foreground_config_v2 import valid_config
from test_restore_interpreter_scenario import KernelPort
from test_runtime_core import setup

from ahc.adapters import wsl_python_workspace, wsl_stream_runner
from ahc.adapters.workspace_kernel import KERNEL
from ahc.cli import foreground_trusted, main
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.runtime_recovery_config import prepare_recovered_config
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.workspace import WorkspaceCheckpoint
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_db import WORKING_DB_FILENAME


def test_recovered_foreground_no_old_generation_replay(tmp_path, monkeypatch, capsys):
    repo, saved, _ = setup(tmp_path)
    owner = repo.reserve_workspace_generation(saved, "work")
    checkpoint = WorkspaceCheckpoint(owner, '{"value":41}')
    repo.save_workspace_checkpoint(saved, checkpoint)
    inbox = RuntimeInbox(repo)
    inbox.submit(saved.session, "old-pending", saved.goal.digest, "old work")
    original = tmp_path / "old.json"
    config = valid_config()
    config["runtime_root"] = str(repo.root)
    config["backend"]["enabled"] = True
    original.write_text(json.dumps(config), encoding="utf-8")
    config_digest = parse_foreground_config_v2(config).configuration_digest
    source_digest = _digest(repo.root / WORKING_DB_FILENAME)
    backup, prep, transformed = (tmp_path / name for name in ("backup", "prep", "new"))
    manifest = backup_runtime_database(repo, backup)
    prepare_runtime_restore(backup, prep)
    receipt = transform_runtime_restore(prep, manifest["sha256"], (saved,),
                                        source_digest, transformed)
    activate_runtime_restore(repo, tmp_path / "supervisor.lock", transformed,
                             receipt["sha256"], manifest["sha256"], source_digest)
    recovered = repo.read_current("session")
    prepared = tmp_path / "config"
    prepare_recovered_config(repo, original, config_digest, prepared,
                              checkpoint_digest=checkpoint.digest, enable_backend=True)
    inbox.submit(recovered.session, "new-work", recovered.goal.digest, "increment restored value")
    calls, ports = [], []

    class Factory:
        def __init__(self, store, config):
            self.config = config

        def recover(self, *_):
            return ()

        def binding(self, tools):
            return "a" * 64, self.config.configuration_digest

        def __call__(self, bound, tools, *, cancelled):
            class Backend:
                def invoke(self, admitted):
                    calls.append(admitted.operation_id)
                    assert admitted.operation_id == "new-work"
                    result = tools.invoke("workspace.execute", {
                        "expected": tools.workspace.identity.to_dict(),
                        "code": "value += 1\nprint(value)"})
                    assert result["disposition"] == "observed"
                    assert result["result"]["output"].strip() == "42"
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                output_text="42")
            return Backend()

    def workspace(store, runner, identity, **options):
        port = KernelPort(identity, options["checkpoint"], KERNEL)
        ports.append(port)
        return port

    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", lambda *a, **kw: object())
    monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace", workspace)
    app = tmp_path / "ahc.toml"
    app.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    args = ["--config", str(app), "run", "--max-runtime", "0.2", "--runtime-config"]
    assert main([*args, str(original)]) == 2
    capsys.readouterr()
    assert not ports and not calls
    for _ in range(2):
        assert main([*args, str(prepared / "foreground.json")]) == 0
        assert json.loads(capsys.readouterr().out)["runtime"]["state"] == "closed"
    assert calls == ["new-work"]
    assert inbox.query(saved.session, "old-pending")["status"] == "pending"
    assert inbox.query(recovered.session, "new-work")["status"] == "terminal"
    assert repo.read_turn(recovered.session, "new-work").result["output_text"] == "42"
    assert len(ports) == 2 and all(port.process.returncode == 0 for port in ports)
