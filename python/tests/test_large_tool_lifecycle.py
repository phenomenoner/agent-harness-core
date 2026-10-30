"""Real child pipes, parent acceptance, SQLite custody, delivery and next-turn checks."""

import json
import subprocess
import sys
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_channel_outbox import state
from test_codex_client import Peer

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.codex_transport import CodexStdioTransport
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

_PEER = """
import json, sys
from pathlib import Path
settings=json.loads(Path(sys.argv[1]).read_text())
messages=settings['messages']
for raw in sys.stdin.buffer:
    message=json.loads(raw)
    method=message.get('method')
    if method in {'initialize','thread/start','turn/start'}:
        index={'initialize':0,'thread/start':1,'turn/start':2}[method]
        sys.stdout.write(json.dumps(messages[index])+'\\n');sys.stdout.flush()
    if method=='turn/start':
        event={'method':'item/started','params':{'threadId':'thread','turnId':'turn','item':{
            'id':'large','type':'mcpToolCall','server':'fixture','tool':'create_tree',
            'status':'inProgress','arguments':{'content':'x'*settings['content_bytes']}}}}
        sys.stdout.write(json.dumps(event)+'\\n');sys.stdout.flush()
        sys.stdout.write(json.dumps(messages[-1])+'\\n');sys.stdout.flush()
"""


class OwnedPeer:
    def __init__(self, process):
        self.process = process
        self.calls = 0

    def cleanup(self, seconds):
        self.calls += 1
        if self.process.poll() is None:
            self.process.terminate()
        self.process.wait(timeout=seconds)
        return SimpleNamespace(complete=True, remaining_count=0)


@pytest.mark.parametrize("oversized", [False, True], ids=["large-success", "bounded-refusal"])
def test_real_pipe_turn_delivery_reopen_and_continuation(tmp_path, oversized):
    repo, snapshot, template, route = state(tmp_path)
    fixture = tmp_path / "peer.json"
    fixture.write_text(json.dumps({"messages": Peer().messages,
        "content_bytes": CodexStdioTransport.MAX_FRAME + 1 if oversized else 6_541_197}))
    process = subprocess.Popen([sys.executable, "-I", "-B", "-c", _PEER, str(fixture)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        cwd=tmp_path)
    owned = OwnedPeer(process)
    wire = CodexStdioTransport(owned)
    client = CodexProtocolClient(wire, cwd="workspace", model="fixture")
    request = replace(template, deadline=time.monotonic() + 15,
        artifact_digest=client.executable_sha256, configuration_digest=client.configuration_digest)
    try:
        result = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
            snapshot, request)
        assert result.result["accepted"] is (not oversized)
        assert result.result["disposition"] == ("unknown" if oversized else "completed")
        assert result.result["unresolved_effects"] is oversized
        outbox = ChannelOutbox(repo)
        text = outbox.read(route)[0]["payload"]["output_text"]
        assert "x" * 100 not in text
        if oversized:
            assert "單筆通訊資料超過接收大小上限" in text
            assert result.result["backend_diagnostic"]["transport_failure"][
                "reason"] == "frame-size-limit"
            assert repo.read_answer_seal(route.session, request.operation_id) is None
        else:
            assert result.result["output_text"] == "answer"
        calls = []
        destination = SimpleNamespace(send=lambda *args: calls.append(args) or "receipt")
        assert ChannelDelivery(outbox, route, destination).deliver(request.operation_id, "one")[
            "state"] == "sent"
        reopened = RuntimeRepository(repo.root)
        assert reopened.read_turn(route.session, request.operation_id) == result
        assert reopened.read_current("session").active_turn_id is None
        if not oversized:
            event = ChannelInboundEvent(route, "next", "next independent task")
            ChannelIngress(RuntimeInbox(reopened), route).submit(event, snapshot.goal.digest)
            following = replace(template, operation_id=event.operation_id,
                                input_text=event.input_text)
            backend = SimpleNamespace(invoke=lambda bound: AgentTurnObservation(
                bound, "completed", "controlled", 1, 1, output_text="next answer"))
            assert RuntimeService(reopened, backend).execute(
                reopened.read_current("session"), following).result["accepted"]
            assert ChannelDelivery(ChannelOutbox(reopened), route, destination).deliver(
                event.operation_id, "two")["state"] == "sent"
        assert len(calls) == (1 if oversized else 2)
        assert wire._closed and not wire._reader.is_alive()
        assert process.poll() is not None and owned.calls == 1
    finally:
        if process.poll() is None:
            owned.cleanup(5)
        process.stdin.close()
        process.stdout.close()
