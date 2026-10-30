"""Real interpreter + Codex peer + SQLite/outbox, including a distinct next task.

The provider and destination acknowledgements are controlled peers, not live
model or platform evidence. The kernel export/control framing is production code.
"""

from dataclasses import replace
import json
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from local_ipython_port import LocalIPythonPort
from test_codex_client import Peer
from test_runtime_core import setup

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.ipython_kernel import IPYTHON_KERNEL
from ahc.adapters.wsl_python_workspace import WslPythonWorkspace
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_attachment_delivery import ChannelAttachmentDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


class Interpreter(LocalIPythonPort):
    _reply = WslPythonWorkspace._reply
    _raise_unavailable = WslPythonWorkspace._raise_unavailable
    execute = WslPythonWorkspace.execute
    validate_execute = WslPythonWorkspace.validate_execute
    export_artifact = WslPythonWorkspace.export_artifact
    turn_scope = WslPythonWorkspace.turn_scope
    checkpoint = WslPythonWorkspace.checkpoint

    @property
    def identity(self):
        return self._identity

    @identity.setter
    def identity(self, value):
        self._identity = value

    def __init__(self, identity, checkpoint):
        self.identity, self.calls, self.closed = identity, 0, False
        self._sequence = 0
        self._batch, self._transport, self._cancelled = None, self, lambda: False
        self.process = subprocess.Popen([sys.executable, "-I", "-u", "-c", IPYTHON_KERNEL],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, encoding="utf-8")
        self.reader = ThreadPoolExecutor(max_workers=1)
        self._lock, self._turn_deadline, self._timeout = threading.RLock(), None, 15
        if checkpoint is not None:
            self._request(operation="restore", values=json.loads(checkpoint.values_json))

    def _require(self, expected):
        assert not self.closed and expected == self.identity

    def _terminate_indeterminate(self):
        self.close()


def call(index, operation, workspace, **arguments):
    return {"id": index, "method": "item/tool/call", "params": {
        "threadId": "thread", "turnId": "turn", "namespace": "workspace",
        "tool": operation, "callId": "call-"+str(index), "arguments": {
            "expected": workspace.identity.to_dict(), **arguments}}}


def client_and_request(repo, workspace, tools, peer, template, event):
    ChannelIngress(RuntimeInbox(repo), event.route).submit(event, template.goal_digest)
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", workspace_tools=tools)
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text,
        workspace_scope="work", deadline=time.monotonic()+60,
        artifact_digest=client.executable_sha256, configuration_digest=client.configuration_digest,
        capabilities=tools.capabilities())

    class Backend:
        def invoke(self, bound):
            with workspace.turn_scope(bound.deadline):
                return client.run_admitted(bound)

    return Backend(), request


@pytest.mark.parametrize("code,name", [
    ("not_portable=object()", "not_portable"),
    ("not_portable={1,2}", "not_portable"),
    ("pass", "missing"),
    ("not_portable='x'*65536", "not_portable"),
    ("hook_calls=[]\nclass T(list):\n    def __iter__(self):\n        hook_calls.append('called')\n        return super().__iter__()\nnot_portable=T([1])", "not_portable"),
])
def test_known_kernel_checkpoint_rejection_allows_correction_and_useful_final(tmp_path, code, name):
    repo, snapshot, template = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Interpreter)
    tools = WorkspaceTools(workspace)
    route = ChannelRoute("telegram", "account", "channel", "user", snapshot.session)

    class ModelPeer(Peer):
        def send(self, message, **kwargs):
            super().send(message, **kwargs)
            if "result" not in message or message["id"] < 7:
                return
            value = json.loads(message["result"]["contentItems"][0]["text"])
            if message["id"] == 7:
                following = call(8, "checkpoint", workspace, names=[name])
            elif message["id"] == 8:
                assert value["disposition"] == "rejected" and value["unresolved_effects"] is False
                assert not workspace.closed and workspace.identity.revision == 1
                following = call(9, "execute", workspace,
                    code="assert globals().get('hook_calls',[])==[]; answer=42; print(answer)")
            elif message["id"] == 9:
                following = call(10, "checkpoint", workspace, names=["answer"])
            elif message["id"] == 10:
                assert value["disposition"] == "observed" and value["result"]["digest"]
                return
            else:
                return
            self.messages.insert(0, following)

    peer = ModelPeer()
    peer.messages.insert(3, call(7, "execute", workspace, code=code))
    try:
        event = ChannelInboundEvent(route, "checkpoint", "save context then answer")
        backend, request = client_and_request(repo, workspace, tools, peer, template, event)
        result = RuntimeService(repo, backend).execute(snapshot, request)
        assert result.result["accepted"] and result.result["unresolved_effects"] is False
        assert workspace.identity.revision == 2 and not workspace.closed
    finally:
        workspace.close()
    assert workspace.port.process.poll() is not None


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("fault", ["admission-busy", "receipt-busy", "receipt-ack-lost"])
def test_workspace_contention_recovers_completion_delivery_and_next_task(tmp_path, platform, fault):
    from ahc.infrastructure.working_repository import WorkingWriterBusy

    repo, snapshot, template = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Interpreter)
    tools = WorkspaceTools(workspace)
    route = ChannelRoute(platform, "account", "channel", "user", snapshot.session)
    code = "execution_count=globals().get('execution_count',0)+1; print(execution_count)"
    executions, receipt_writes, feedback, sends = [], [], [], []
    execute = workspace.port.execute
    def counted(*args):
        executions.append(1)
        return execute(*args)
    workspace.port.execute = counted
    if fault == "admission-busy":
        begin = repo.begin_workspace_step
        attempts = []
        def admit(identity):
            attempts.append(identity)
            if len(attempts) == 1:
                raise WorkingWriterBusy("PRIVATE admission contention")
            return begin(identity)
        repo.begin_workspace_step = admit
    else:
        finish = repo.finish_workspace_step
        def save(identity):
            receipt_writes.append(identity)
            if len(receipt_writes) == 1:
                if fault == "receipt-ack-lost":
                    finish(identity)
                raise WorkingWriterBusy("PRIVATE receipt contention")
            return finish(identity)
        repo.finish_workspace_step = save

    class ModelPeer(Peer):
        def send(self, message, **kwargs):
            super().send(message, **kwargs)
            if "result" not in message or message["id"] < 7:
                return
            value = json.loads(message["result"]["contentItems"][0]["text"])
            feedback.append(value)
            if fault == "admission-busy" and message["id"] == 7:
                assert value["reason"] == "workspace-not-executed"
                assert value["unresolved_effects"] is False and executions == []
                self.messages.insert(0, call(8, "execute", workspace, code=code))

    class Destination:
        def send(self, actual, payload, operation):
            assert actual == route and payload["output_text"] == "answer"
            sends.append(operation)
            return "receipt-" + str(len(sends))

    peer = ModelPeer()
    peer.messages.insert(3, call(7, "execute", workspace, code=code))
    try:
        event = ChannelInboundEvent(route, "contention", "do work and answer")
        backend, request = client_and_request(repo, workspace, tools, peer, template, event)
        result = RuntimeService(repo, backend).execute(snapshot, request)
        assert result.result["accepted"] and result.result["unresolved_effects"] is False
        assert len(executions) == 1 and workspace.identity.revision == 1
        assert feedback[-1]["result"]["output"].strip() == "1"
        if fault != "admission-busy":
            assert feedback[-1]["workspace_recovery"] == "receipt-reconciled"
            assert len(receipt_writes) == (1 if fault == "receipt-ack-lost" else 2)
        assert "PRIVATE" not in json.dumps(result.result)
        assert ChannelDelivery(ChannelOutbox(repo), route, Destination()).deliver(
            request.operation_id, "first-claim")["state"] == "sent"

        next_peer = Peer()
        next_peer.messages.insert(3, call(7, "execute", workspace, code=code))
        next_event = ChannelInboundEvent(route, "contention-next", "continue")
        next_backend, next_request = client_and_request(
            repo, workspace, tools, next_peer, template, next_event)
        following = RuntimeService(repo, next_backend).execute(
            repo.read_current(snapshot.session.ref.virtual_session_id), next_request)
        assert following.result["accepted"] and workspace.identity.revision == 2
        assert len(executions) == 2
        value = next(message for message in next_peer.sent if message.get("id") == 7)
        assert json.loads(value["result"]["contentItems"][0]["text"])["result"]["output"].strip() == "2"
        assert ChannelDelivery(ChannelOutbox(repo), route, Destination()).deliver(
            next_request.operation_id, "next-claim")["state"] == "sent"
        reopened = RuntimeRepository(repo.root)
        assert reopened.read_turn(route.session, request.operation_id) == result
        assert reopened.read_turn(route.session, next_request.operation_id) == following
        assert sends == [request.operation_id, next_request.operation_id]
    finally:
        workspace.close()
    assert workspace.port.process.poll() is not None


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_known_export_error_recovers_zip_and_next_task_on_the_original_route(tmp_path, platform):
    repo, snapshot, template = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Interpreter)
    artifacts = ArtifactRepository(repo)
    tools = WorkspaceTools(workspace, artifacts=artifacts)
    route = ChannelRoute(platform, "account", "channel", "user", snapshot.session)
    artifact_id, feedback = [], []

    class ModelPeer(Peer):
        def send(self, message, **kwargs):
            super().send(message, **kwargs)
            if "result" not in message or message["id"] < 7:
                return
            value = json.loads(message["result"]["contentItems"][0]["text"])
            feedback.append(value)
            if message["id"] == 7:
                next_call = call(8, "artifact", workspace, name="wrong", media_type="application/zip", lineage=[])
            elif message["id"] == 8:
                assert value["disposition"] == "rejected" and not value["unresolved_effects"]
                next_call = call(9, "artifact", workspace, name="bundle_bytes", media_type="application/zip", lineage=[])
            elif message["id"] == 9:
                artifact_id.append(value["result"]["artifact_id"])
                next_call = call(10, "checkpoint", workspace, names=["resume_context"])
            elif message["id"] == 10:
                next_call = call(11, "attach", workspace, artifact_id=artifact_id[0], filename="bundle.zip")
            elif message["id"] == 11:
                next_call = call(12, "execute", workspace,
                    code="assert len(bundle_bytes)==28773; print('confirmed ZIP and saved context')")
            else:
                return
            self.messages.insert(0, next_call)

    peer = ModelPeer()
    code = ("import io, zipfile\nfrom pathlib import Path\nbuf=io.BytesIO()\n"
            "with zipfile.ZipFile(buf,'w',compression=zipfile.ZIP_STORED) as z:\n"
            "    z.writestr('plan.md','x'*28661)\n"
            "bundle_bytes=buf.getvalue(); assert len(bundle_bytes)==28773\n"
            "Path('bundle.zip').write_bytes(bundle_bytes)\n"
            "wrong={}; resume_context={'file':'bundle.zip','bytes':28773}")
    # Avoid task files in the repository: the process uses this test's tmp cwd.
    peer.messages.insert(3, call(7, "execute", workspace, code=f"import os; os.chdir({str(tmp_path)!r})\n"+code))
    try:
        event = ChannelInboundEvent(route, "first", "create report and ZIP")
        backend, request = client_and_request(repo, workspace, tools, peer, template, event)
        first = RuntimeService(repo, backend).execute(snapshot, request)
        assert first.result["accepted"] and not first.result["unresolved_effects"], json.dumps(first.result)
        assert len(feedback) == 6 and not workspace.closed
        assert artifacts.read(snapshot.session, artifact_id[0])[1] == (tmp_path/"bundle.zip").read_bytes()
        assert first.result["artifacts"][0]["artifact_id"] == artifact_id[0]
        with repo._read() as connection:
            assert connection.execute("SELECT count(*) FROM runtime_channel_attachments").fetchone()[0] == 1

        class Destination:
            calls = []
            def send(self, observed_route, payload, logical_delivery_id):
                self.calls.append((observed_route, payload, logical_delivery_id))
                return "confirmed-peer-receipt"
        destination = Destination()
        delivered = ChannelDelivery(ChannelOutbox(repo), route, destination).deliver(request.operation_id, "first-token")
        assert delivered["state"] == "sent" and destination.calls[0][0] == route

        class AttachmentDestination:
            calls = []
            def send(self, upload):
                from email import policy
                from email.parser import BytesParser

                message = BytesParser(policy=policy.default).parsebytes(
                    f"Content-Type: {upload.content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode()+upload.body)
                assert list(message.iter_parts())[-1].get_payload(decode=True) == (tmp_path/"bundle.zip").read_bytes()
                assert upload.route == route and upload.byte_count == 28773
                self.calls.append(upload.artifact_id)
                return {"peer_receipt": "verified-upload", "artifact_id": upload.artifact_id}
        attachment_destination = AttachmentDestination()
        attachment = ChannelAttachmentDelivery(ChannelAttachmentOutbox(repo), route,
            attachment_destination).deliver(request.operation_id, 0, "attachment-token")
        assert attachment["state"] == "sent" and attachment_destination.calls == artifact_id

        next_peer = Peer()
        next_peer.messages.insert(3, call(7, "execute", workspace,
            code="assert resume_context['bytes']==28773; assert Path('bundle.zip').read_bytes()==bundle_bytes"))
        next_event = ChannelInboundEvent(route, "next", "continue from saved report")
        next_backend, next_request = client_and_request(repo, workspace, tools, next_peer, template, next_event)
        second = RuntimeService(repo, next_backend).execute(repo.read_current("session"), next_request)
        assert second.result["accepted"] and workspace.identity.revision == 3
        reopened = RuntimeRepository(repo.root)
        assert reopened.read_turn(snapshot.session, request.operation_id) == first
        assert reopened.read_turn(snapshot.session, next_request.operation_id) == second
        assert len(destination.calls) == 1
        assert attachment_destination.calls == artifact_id
    finally:
        workspace.close()
    assert workspace.port.process.poll() is not None


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_actual_execution_unknown_preserves_confirmed_work_without_replay_or_success(tmp_path, platform):
    repo, snapshot, template = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Interpreter)
    artifacts = ArtifactRepository(repo)
    tools = WorkspaceTools(workspace, artifacts=artifacts)
    route = ChannelRoute(platform, "account", "channel", "user", snapshot.session)
    execute, attempts = workspace.port.execute, []
    def lose_execution_reply(expected, code):
        value = execute(expected, code)
        if "uncertain_marker" in code:
            attempts.append(1)
            raise RuntimeError("PRIVATE reply lost after actual execution")
        return value
    workspace.port.execute = lose_execution_reply

    class ModelPeer(Peer):
        def send(self, message, **kwargs):
            super().send(message, **kwargs)
            if "result" not in message or message["id"] < 7:
                return
            if message["id"] == 7:
                following = call(8, "checkpoint", workspace, names=["resume_context"])
            elif message["id"] == 8:
                following = call(9, "artifact", workspace, name="report", media_type="text/plain", lineage=[])
            elif message["id"] == 9:
                following = call(10, "execute", workspace,
                    code="uncertain_marker=Path('marker.txt').write_text('once')")
            elif message["id"] == 10:
                feedback = json.loads(message["result"]["contentItems"][0]["text"])
                assert feedback["reporting"]["mode"] == "final-report"
                confirmed = feedback["reporting"]["confirmed_work"]
                assert confirmed["checkpoint"]["digest"] and len(confirmed["artifacts"]) == 1
                self.messages[-1]["params"]["turn"]["items"][0]["text"] = (
                    "已確認保存任務檢查點與 1 份本地附件；尚不代表已送達。"
                    "寫入 marker 的結果回條遺失，先停下寫入；下一步請核對原檔。")
                return
            else:
                return
            self.messages.insert(0, following)

    peer = ModelPeer()
    peer.messages.insert(3, call(7, "execute", workspace,
        code=f"import os; os.chdir({str(tmp_path)!r})\nfrom pathlib import Path\n"
             "report=b'confirmed local report'; resume_context={'file':'marker.txt'}"))
    try:
        event = ChannelInboundEvent(route, "unknown", "continue a task")
        backend, request = client_and_request(repo, workspace, tools, peer, template, event)
        result = RuntimeService(repo, backend).execute(snapshot, request)
        assert result.result["accepted"] is False and result.result["unresolved_effects"], result.result
        assert result.result["confirmed_work"]["checkpoint"]["digest"]
        assert len(result.result["confirmed_work"]["artifacts"]) == 1
        assert (tmp_path/"marker.txt").read_text() == "once" and attempts == [1]
        payload = ChannelOutbox(repo).read(route)[0]["payload"]["output_text"]
        assert "已確認保存任務檢查點" in payload and "1 份本地附件" in payload
        assert "尚不代表已送達" in payload and "PRIVATE" not in payload
        assert RuntimeService(repo, backend).execute(snapshot, request) == result
        assert attempts == [1] and not peer.messages
        assert result.result["report_accepted"] is True  # Report accepted; task remains unknown.
        assert RuntimeRepository(repo.root).read_turn(snapshot.session, request.operation_id) == result
    finally:
        workspace.close()
    assert workspace.port.process.poll() is not None
