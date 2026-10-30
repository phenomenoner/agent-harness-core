import json
import queue
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest
from test_runtime_core import setup

from ahc.adapters.workspace_batch_callback import receive_reply
from ahc.adapters.workspace_kernel import KERNEL
from ahc.application.capability_view import _issue_agent_capability_view
from ahc.application.tool_batch import ReadOnlyBatch
from ahc.domain.capability_values import ApprovalMode, CapabilityDefinition, EffectClass
from ahc.domain.profile_values import ProfileResourceKind
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.runtime_query_port import RuntimeQueryPort


def test_actual_kernel_batches_real_store_and_retains_reduction(tmp_path):
    store, snapshot, _ = setup(tmp_path)
    identity = store.reserve_workspace_generation(snapshot, "work")
    artifact = ArtifactRepository(store).publish(identity, b"hello", "text/plain")
    capability = "runtime.artifact.manifest"
    view = _issue_agent_capability_view(
        (CapabilityDefinition(capability, ProfileResourceKind.WORKSPACE,
                              EffectClass.READ_ONLY, ApprovalMode.NONE),),
        ((capability, "runtime"),),
    )
    batch = ReadOnlyBatch(identity.session, view, RuntimeQueryPort(store, identity.session))
    process = subprocess.Popen([sys._base_executable, "-I", "-u", "-c", KERNEL],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, encoding="utf-8")
    received = queue.Queue()

    def read():
        for line in process.stdout:
            received.put(json.loads(line))

    reader = threading.Thread(target=read, daemon=True)
    reader.start()

    def send(payload, *, deadline):
        process.stdin.write(json.dumps(payload) + "\n")
        process.stdin.flush()

    workspace = SimpleNamespace(
        _sequence=1, _batch=batch, identity=identity, _cancelled=lambda: False,
        _receive=lambda deadline: received.get(timeout=max(0.01, deadline-time.monotonic())),
        _transport=SimpleNamespace(send=send),
    )
    calls = [{"call_id": f"item{i}", "capability_id": capability,
              "resource_id": "runtime", "arguments": {"artifact_id": artifact.artifact_id}}
             for i in range(2)]
    code = (f"rows=ahc_batch({calls!r}, concurrency=2); "
            "total=sum(v['byte_count'] for v in rows['values']); print(total)")
    request = {"id": 1, "operation": "execute", "batch_enabled": True, "code": code}
    try:
        deadline = time.monotonic() + 5
        send(request, deadline=deadline)
        reply = receive_reply(workspace, request, deadline)
        assert reply["ok"] is True
        assert reply["output"] == "10\n"
        assert "hello" not in str(reply)
        workspace._sequence = 2
        later = {"id": 2, "operation": "execute",
                 "code": "print(total); print('ahc_batch' in globals())"}
        send(later, deadline=deadline)
        reply = receive_reply(workspace, later, deadline)
        assert reply["output"] == "10\nFalse\n"
    finally:
        process.stdin.close()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        reader.join(timeout=1)
        process.stdout.close()
        process.stderr.close()


@pytest.mark.parametrize("operation,batch", [("execute", None), ("inspect", object())])
def test_unadmitted_callback_is_not_dispatched(operation, batch):
    workspace = SimpleNamespace(_batch=batch, _sequence=1,
        _receive=lambda _: {"id": 1, "batch": [], "concurrency": 1})
    with pytest.raises(ValueError):
        receive_reply(workspace, {"operation": operation}, time.monotonic() + 1)
