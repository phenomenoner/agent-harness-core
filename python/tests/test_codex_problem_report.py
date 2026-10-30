"""A tool fault concludes with a sealed model report, not task-success evidence."""

import json
from dataclasses import replace

import pytest
from test_codex_client import Peer
from test_workspace_tools import setup

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.runtime_turn import accept_observation
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.runtime_conversation_context import conversation_context
from ahc.domain.sessions import ContractError


REPORT = "已完成資料盤點；寫入結果尚未確認，因此停止寫入。下一步先核對原檔再接續。"


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("fault", ["lost-result", "adapter-exception", "resource-unavailable",
                                 "malformed-result", "invalid-wire-result"])
def test_fault_returns_to_model_seals_report_delivers_once_and_keeps_custody(
    tmp_path, platform, fault
):
    repo, workspace, tools = setup(tmp_path)
    snapshot = repo.read_current("session")
    route = ChannelRoute(platform, "account", "channel", "user", snapshot.session)
    event = ChannelInboundEvent(route, "fault-report", "do work and report")
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    peer = Peer()
    client = CodexProtocolClient(
        peer, cwd="workspace", model="fixture", clock=lambda: 0, workspace_tools=tools
    )
    request = AgentTurnRequest(
        snapshot.session, event.operation_id, snapshot.goal.digest, event.input_text, (),
        client.executable_sha256, client.configuration_digest, "work", 100,
        capabilities=tools.capabilities(),
    )
    calls = []
    invoke = tools.invoke

    def failing(name, args):
        calls.append(name)
        if fault == "adapter-exception":
            raise RuntimeError("PRIVATE exception text")
        if fault == "malformed-result":
            return None
        if fault == "invalid-wire-result":
            return {"disposition": "observed", "unresolved_effects": False,
                    "result": {"private_nan": float("nan")}}
        if fault == "resource-unavailable":
            return {"disposition": "rejected", "reason": "checkpoint-capture-unavailable",
                    "unresolved_effects": False, "resource_available": False}
        return {"disposition": "unknown", "reason": "workspace-operation-unverified",
                "unresolved_effects": True,
                "workspace_failure": {"phase": "receive", "code": "eof"}}

    tools.invoke = failing
    for number in (7, 8):
        peer.messages.insert(number - 4, {
            "id": number, "method": "item/tool/call", "params": {
                "threadId": "thread", "turnId": "turn", "namespace": "workspace",
                "tool": "execute", "callId": "fault-" + str(number),
                "arguments": {"expected": workspace.identity.to_dict(), "code": "x=42"},
            },
        })
    peer.messages[-1]["params"]["turn"]["items"][0]["text"] = REPORT

    class Backend:
        def invoke(self, bound):
            return client.run_admitted(bound)

    class Destination:
        sent = []

        def send(self, actual, payload, operation):
            assert actual == route and set(payload) == {"output_text"}
            assert payload["output_text"].endswith(REPORT)
            self.sent.append(operation)
            return "ack"

    try:
        turn = RuntimeService(repo, Backend()).execute(snapshot, request)
        assert turn.result.get("report_accepted") is True
        assert turn.result["accepted"] is False and turn.result["disposition"] == "unknown"
        assert turn.result["output_text"] == REPORT
        assert turn.result["unresolved_effects"] is (fault != "resource-unavailable")
        assert calls == ["workspace.execute"]
        replies = [json.loads(m["result"]["contentItems"][0]["text"])
                   for m in peer.sent if m.get("id") in (7, 8)]
        assert replies[0]["reporting"]["mode"] == "final-report"
        assert replies[1]["reason"] == "problem-report-only"
        assert "PRIVATE" not in json.dumps(turn.result) + json.dumps(replies)
        assert repo.read_answer_seal(request.session, request.operation_id)["report_accepted"]
        assert repo.intents(request.session)[0]["kind"] == "parked"
        destination = Destination()
        assert ChannelDelivery(ChannelOutbox(repo), route, destination).deliver(
            request.operation_id, "claim")["state"] == "sent"
        reopened = RuntimeRepository(repo.root)
        assert reopened.read_turn(request.session, request.operation_id) == turn
        assert ChannelOutbox(reopened).read(route)[0]["payload"]["output_text"] == REPORT
        with pytest.raises(ContractError):
            ChannelDelivery(ChannelOutbox(reopened), route, destination).deliver(
                request.operation_id, "claim-again")
        assert destination.sent == [request.operation_id]

        # A distinct task keeps the report and honest outcome in its own history.
        next_event = ChannelInboundEvent(route, "next-report", "first reconcile, then continue")
        ChannelIngress(RuntimeInbox(repo), route).submit(next_event, snapshot.goal.digest)
        history = conversation_context(repo, request.session, next_event.operation_id,
                                       include_incomplete=True)
        assert history[0]["assistant"] == REPORT
        assert history[0]["status"] == "unknown" and history[0]["report_accepted"] is True
        tools.invoke = invoke
        next_peer = Peer()
        next_client = CodexProtocolClient(next_peer, cwd="workspace", model="fixture",
                                         clock=lambda: 0, workspace_tools=tools)
        next_request = replace(request, operation_id=next_event.operation_id,
                               input_text=next_event.input_text,
                               configuration_digest=next_client.configuration_digest)
        following = RuntimeService(repo, type("NextBackend", (), {
            "invoke": lambda self, bound: next_client.run_admitted(bound)})()).execute(
                repo.read_current("session"), next_request)
        assert following.result["accepted"] is True
        assert reopened.read_turn(request.session, request.operation_id) == turn
    finally:
        workspace.close()


def test_unobserved_or_unbound_text_cannot_be_promoted_to_a_problem_report(tmp_path):
    from ahc.domain.agent_backend import AgentTurnObservation
    from ahc.domain.sessions import ContractError
    from test_runtime_core import setup as runtime_setup

    _, _, request = runtime_setup(tmp_path)
    for change in ({"disposition": "unknown"}, {"backend_turn": None},
                   {"accepted_backend_turns": 0}, {"output_text": ""}):
        value = AgentTurnObservation(request, "completed", "fixture", 1, 1,
            "thread", "turn", REPORT, unresolved_effects=True, final_report=True)
        with pytest.raises(ContractError):
            accept_observation(request, replace(value, **change))
