import json
import sys

import pytest
from skill_test_support import revision
from test_foreground_trusted import config_file
from test_skill_graph_convergence import _activate_with_report
from test_workspace_tools import Port

from ahc.adapters import (
    native_process_runner,
    native_python_workspace,
    wsl_python_workspace,
    wsl_stream_runner,
)
from ahc.cli import foreground_trusted
from ahc.cli.foreground import compose_foreground
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.infrastructure.runtime_inbox import RuntimeInbox


@pytest.mark.parametrize("operations", [
    ("analyze", "other"),
    ("channel-" + "a" * 64, "channel-" + "b" * 64),
])
def test_trusted_foreground_keeps_selection_and_reads_bodies_on_demand_after_reopen(
    tmp_path, monkeypatch, repository, operations
):
    original, request, payload, path = config_file(tmp_path, enabled=True)
    candidate = revision()
    _activate_with_report(repository, candidate, "activate")
    payload["skill_root"] = str(repository.root)
    if sys.platform == "linux":
        payload["schema_version"] = "ahc.foreground.config.v3"
        payload["workspace"]["kind"] = "linux-python"
        payload["workspace"]["interpreter"] = sys.executable
        for key in ("wsl_executable", "distribution", "user"):
            del payload["workspace"][key]
        payload["backend"]["kind"] = "codex-linux-trusted"
    path.write_text(json.dumps(payload))
    inbox = RuntimeInbox(original.store)
    observed = []

    class Factory:
        def __init__(self, store, config):
            self.config = config

        def recover(self, *args):
            return ()

        def binding(self, tools):
            return "a" * 64, "b" * 64

        def __call__(self, bound, tools, **kwargs):
            class Backend:
                def invoke(self, selected):
                    assert selected.effective_policy.approval_mode == "manual"
                    observed.append((selected.revision_digests, selected.skill_bodies))
                    return AgentTurnObservation(selected, "completed", "fixture", 1, 1)

            return Backend()

    monkeypatch.setattr(foreground_trusted, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", lambda *args, **kw: object())
    monkeypatch.setattr(
        wsl_python_workspace,
        "WslPythonWorkspace",
        lambda store, runner, identity, **kw: Port(identity, kw["checkpoint"]),
    )
    if sys.platform == "linux":
        monkeypatch.setattr(native_process_runner, "NativeProcessRunner", lambda **kw: object())
        monkeypatch.setattr(native_python_workspace, "NativePythonWorkspace",
            lambda store, runner, identity, **kw: Port(identity, kw["checkpoint"]))
    for operation, task in zip(operations, ("analyze task", "summarize task"), strict=True):
        inbox.submit(request.session, operation, request.goal_digest, task)
        runtime = compose_foreground(path)
        runtime.start()
        try:
            assert runtime.step().result["accepted"]
            assert runtime.step() is None
            assert inbox.query(request.session, operation)["status"] == "terminal"
        finally:
            runtime.close()
    assert observed == [
        ((candidate.revision_digest,), ()),
        ((), ()),
    ]
    assert repository.get_episode(operations[0]).surfaced_revisions == (candidate.revision_digest,)
