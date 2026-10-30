"""Configured foreground with real local kernel/store; OS and model peers controlled."""

import json
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from test_foreground_trusted import config_file
from test_schedules import _schedule
from test_wsl_python_workspace import Custody

from ahc.adapters import wsl_python_workspace, wsl_stream_runner
from ahc.adapters.workspace_kernel import KERNEL
from ahc.cli import foreground_trusted as composition
from ahc.cli.foreground import compose_foreground
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.batch_receipt_repository import BatchReceiptRepository
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository


class LocalTransport:
    def __init__(self, kernel=KERNEL):
        self.process = subprocess.Popen(
            [sys._base_executable, "-I", "-u", "-c", kernel],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8",
        )
        self.queue = queue.Queue()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        for line in self.process.stdout:
            self.queue.put(json.loads(line))

    def send(self, payload, *, deadline):
        self.process.stdin.write(json.dumps(payload) + "\n")
        self.process.stdin.flush()

    def receive(self, *, deadline):
        return self.queue.get(timeout=max(0.01, deadline - time.monotonic()))

    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        self.reader.join(timeout=1)
        self.process.stdout.close()
        self.process.stderr.close()


@pytest.mark.parametrize("engine", ["python", "ipython"])
@pytest.mark.parametrize("effectful", [False, True])
def test_two_foreground_turns_batch_reduce_publish_reopen_without_replay(
    tmp_path, monkeypatch, engine, effectful,
):
    original, request, config, path = config_file(tmp_path, enabled=True)
    kernel = KERNEL
    if engine == "ipython":
        ipython = pytest.importorskip("IPython")
        from ahc.adapters.ipython_kernel import IPYTHON_KERNEL
        packages = str(Path(ipython.__file__).resolve().parent.parent)
        kernel = f"import sys;sys.path.insert(0, {packages!r});exec({IPYTHON_KERNEL!r})"
        config["workspace"]["kind"] = "wsl-ipython"
    config["batch"] = {"agent_id": request.session.agent_id,
                       "capabilities": ["runtime.artifact.manifest"]}
    if effectful:
        config["batch"]["capabilities"].append("runtime.schedule.pause")
        ScheduleRepository(original.store).create(_schedule(
            session=request.session, goal_digest=request.goal_digest))
    path.write_text(json.dumps(config), encoding="utf-8")
    transports, invoked, published = [], [], []
    source_id = []

    class Runner:
        def __init__(self, executable, distribution, *, persistent):
            self.distribution = distribution
            assert persistent is True

        def attach(self, unit, custody, lease):
            transport = LocalTransport(kernel)
            transports.append(transport)
            return transport

    class Factory:
        def __init__(self, store, config):
            self.config = config

        def recover(self, *args):
            return ()

        def binding(self, tools):
            return "a" * 64, self.config.configuration_digest

        def __call__(self, bound, tools, *, cancelled):
            invoked.append(bound.operation_id)

            class Backend:
                def invoke(self, admitted):
                    calls = [{"call_id": f"item{i}", "capability_id": "runtime.artifact.manifest",
                              "resource_id": "runtime", "arguments": {"artifact_id": source_id[0]}}
                             for i in range(2)]
                    if effectful:
                        calls += [{"call_id": name, "capability_id": "runtime.schedule.pause",
                            "resource_id": "runtime", "arguments": {
                                "schedule_id": "schedule-main", "expected_revision": 1}}
                            for name in ("pause", "stale")]
                    concurrency = 1 if effectful else 2
                    code = (f"rows=ahc_batch({calls!r},concurrency={concurrency}); "
                            "total=sum(v['byte_count'] for v in rows['values'][:2]); print(total)")
                    if admitted.operation_id == "second":
                        code = "total+=1; report=str(total); print(total)"
                    if engine == "ipython":
                        code = "%precision 3\n" + code
                        code += ("\nfrom IPython.display import display, HTML"
                                 "\ndisplay(HTML('<b>ok</b>'))")
                    result = tools.invoke("workspace.execute", {
                        "expected": tools.workspace.identity.to_dict(), "code": code})
                    assert result["disposition"] == "observed" and result["result"]["ok"]
                    if engine == "ipython":
                        assert result["result"]["displays"][0]["text/html"] == "<b>ok</b>"
                        assert result["result"]["display_truncated"] is False
                    assert result["result"]["output"] == (
                        "10\n" if admitted.operation_id == "first" else "11\n")
                    if admitted.operation_id == "second":
                        current = tools.workspace.identity.to_dict()
                        inspection = tools.invoke("workspace.inspect", {
                            "expected": current, "names": ["total", "report"]})
                        assert inspection["disposition"] == "observed"
                        assert inspection["result"]["variables"] == [
                            {"name": "total", "kind": "integer", "size": None},
                            {"name": "report", "kind": "string", "size": 2},
                        ]
                        assert tools.workspace.identity.to_dict() == current
                        result = tools.invoke("workspace.artifact", {
                            "expected": tools.workspace.identity.to_dict(), "name": "report",
                            "media_type": "text/plain", "lineage": [source_id[0]]})
                        assert result["disposition"] == "observed"
                        published.append(result["result"]["artifact_id"])
                    return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                                "thread", "turn", "done")

                def close(self):
                    pass

            return Backend()

    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", Runner)
    monkeypatch.setattr(wsl_python_workspace, "BackendCustody", Custody)
    runtime = compose_foreground(path)
    inbox = RuntimeInbox(original.store)
    runtime.start()
    try:
        manifest = ArtifactRepository(runtime.store).publish(
            runtime.workspace.identity, b"hello", "text/plain")
        source_id.append(manifest.artifact_id)
        for operation in ("first", "second"):
            inbox.submit(request.session, operation, request.goal_digest, "input")
            assert runtime.step().result["accepted"]
            assert runtime.step() is None
        assert runtime.workspace.identity.revision == 2
    finally:
        runtime.close()
    assert len(transports) == 1 and transports[0].process.poll() == 0
    assert invoked == ["first", "second"]
    reopened = RuntimeRepository(original.store.root)
    receipts = BatchReceiptRepository(reopened).read(request.session, "first")
    assert [row["state"] for row in receipts] == (
        ["success", "success", "success", "rejected"] if effectful else ["success", "success"])
    if effectful:
        saved = ScheduleRepository(reopened).read(request.session, "schedule-main")
        assert saved.state == "paused" and saved.revision == 2
    assert ArtifactRepository(reopened).read(request.session, published[0])[1] == b"11"
    assert RuntimeInbox(reopened).next_pending(request.session) is None
