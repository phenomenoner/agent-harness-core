"""Actual concurrent CLI processes with independent profile status and stop."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from profile_test_support import profile_registry_payload
from test_prepare_foreground import options, tool

import ahc
from ahc.application.lifecycle import inspect_status
from ahc.cli import main
from ahc.cli.profile_lifecycle import profile_config
from ahc.infrastructure.config import load_config


def test_stop_one_profile_leaves_other_foreground_running(tmp_path, capsys):
    for agent in ("alpha", "beta"):
        tool.prepare(tmp_path / agent, **{**options(), "agent_id": agent, "session_id": agent})
    (tmp_path / "profiles.json").write_text(
        json.dumps(profile_registry_payload(("alpha", "beta"))), encoding="utf-8")
    app = tmp_path / "app.toml"
    app.write_text('config_version="ahc.standalone.config.v2"\nstate_dir="state"\n'
                   'profile_registry="profiles.json"\nservices=[]\n[foreground_configs]\n'
                   'alpha="alpha/foreground.json"\nbeta="beta/foreground.json"\n', encoding="utf-8")
    config = load_config(app)
    scoped = {agent: profile_config(config, agent) for agent in ("alpha", "beta")}
    assert scoped["alpha"].state_dir != scoped["beta"].state_dir != config.state_dir
    environment = dict(os.environ, PYTHONPATH=str(Path(ahc.__file__).parent.parent))
    processes = {}
    try:
        for agent in scoped:
            processes[agent] = subprocess.Popen(
                [sys.executable, "-B", "-m", "ahc", "run", "--config", str(app),
                 "--agent", agent, "--max-runtime", "15"], env=environment,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
        # One bounded readiness window, at most100 observations; no process restart.
        for _ in range(100):
            if all(inspect_status(value)[1].get("status") == "running"
                   for value in scoped.values()):
                break
            assert all(process.poll() is None for process in processes.values())
            time.sleep(0.05)
        assert all(inspect_status(value)[1]["status"] == "running" for value in scoped.values())
        assert main(["stop", "--config", str(app), "--agent", "alpha", "--wait-seconds", "3"]) == 0
        assert json.loads(capsys.readouterr().out)["report"]["status"] == "stopped"
        assert processes["alpha"].wait(timeout=4) == 0
        assert processes["beta"].poll() is None
        assert main(["status", "--config", str(app), "--agent", "beta"]) == 0
        assert json.loads(capsys.readouterr().out)["report"]["status"] == "running"
        assert main(["stop", "--config", str(app), "--agent", "beta", "--wait-seconds", "3"]) == 0
        capsys.readouterr()
        assert processes["beta"].wait(timeout=4) == 0
        assert not config.lease_path.exists()
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.terminate()
            process.communicate(timeout=5)
