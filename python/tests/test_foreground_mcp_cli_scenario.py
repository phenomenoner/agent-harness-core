import json
import os
import sys

import pytest
from test_foreground_trusted import config_file
from test_mcp_stdio import SERVER
from test_workspace_tools import Port

from ahc.adapters import wsl_python_workspace, wsl_stream_runner
from ahc.cli import foreground_trusted as composition
from ahc.cli import main
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.mcp_profiles import McpStdioProfile
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.runtime_inbox import RuntimeInbox


@pytest.mark.skipif(os.name != "nt", reason="Windows managed fixture")
def test_cli_run_configured_mcp_and_receipt_readback(tmp_path, monkeypatch, capsys):
    pytest.importorskip("jsonschema")
    original, request, config, path = config_file(tmp_path, enabled=True)
    env_names = tuple(k for k in ("SystemRoot", "WINDIR") if k in os.environ)
    digest = digest_json({"server_id": "fixture", "protocol": "2025-11-25",
        "tools": [{"name": "echo", "inputSchema": {"type": "object"}}]})
    profile = McpStdioProfile("fixture", request.session.agent_id,
        (sys.executable, "-u", "-c", SERVER, "echo"), str(tmp_path), env_names,
        ("echo",), digest, True)
    config["mcp"] = {"profiles": [profile.to_dict()], "credential_environment": {}}
    path.write_text(json.dumps(config), encoding="utf-8")
    app = tmp_path / "ahc.toml"
    app.write_text('config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n')
    calls = []

    class Runner:
        def __init__(self, *args, **kwargs):
            pass

    class Factory:
        def __init__(self, store, config):
            self.config = config

        def recover(self, *args):
            return ()

        def binding(self, tools):
            return "a" * 64, self.config.configuration_digest

        def __call__(self, bound, tools, *, cancelled):
            class Backend:
                def invoke(self, admitted):
                    result = tools.invoke("workspace.mcp", {
                        "expected": tools.workspace.identity.to_dict(), "server_id": "fixture",
                        "tool": "echo", "call_id": "cli-call", "arguments": {"text": "cli-echo"}})
                    calls.append(result)
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                "thread", "turn", "done")
            return Backend()

    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", Runner)
    monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace",
                        lambda store, runner, identity, **kw: Port(identity, None))
    RuntimeInbox(original.store).submit(request.session, "cli-op", request.goal_digest, "input")
    assert main(["--config", str(app), "run", "--runtime-config", str(path),
                 "--max-runtime", "2"]) == 0
    run = json.loads(capsys.readouterr().out)
    assert run["runtime"]["state"] == "closed"
    assert len(calls) == 1 and calls[0]["disposition"] == "observed"
    assert calls[0]["result"]["result"]["content"][0]["text"] == "cli-echo"
    assert main(["runtime-mcp-receipts", "--runtime-root", str(original.store.root),
                 "--session-json", json.dumps(request.session.to_dict()),
                 "--operation-id", "cli-op"]) == 0
    rows = json.loads(capsys.readouterr().out)["receipts"]
    assert len(rows) == 1 and rows[0]["status"] == "observed"
    assert RuntimeInbox(original.store).next_pending(request.session) is None
