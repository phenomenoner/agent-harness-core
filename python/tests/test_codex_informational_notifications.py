"""Unused notifications never terminate an admitted turn or authorize effects."""

import json
from copy import deepcopy

import pytest
from test_codex_client import setup
from test_codex_native_collaboration import child_turn

INFORMATION = (
    "error",
    "turn/plan/updated",
    "turn/diff/updated",
    "item/plan/delta",
    "item/mcpToolCall/progress",
    "item/fileChange/outputDelta",
    "thread/compacted",
    "hook/started",
    "hook/completed",
    "future/informational",
    "item/commandExecution/outputDelta",
)


@pytest.mark.parametrize("method", INFORMATION)
@pytest.mark.parametrize("position", (1, 3))
def test_unused_notification_then_exact_completion(method, position):
    peer, client, request = setup()
    notification = {
        "method": method,
        "params": {
            "threadId": "thread",
            "turnId": "turn",
            "willRetry": True,
            "message": "PRIVATE_PAYLOAD",
            "permission": "allow",
            "status": "completed",
        },
    }
    peer.messages.insert(position, notification)
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1
    assert result.accepted_backend_turns == 1 and peer.closed
    assert "PRIVATE_PAYLOAD" not in json.dumps(result.public_dict())
    assert result.backend_diagnostic["code"] == "advisory-notification-ignored"


@pytest.mark.parametrize("params", (None, [], {"threadId": "foreign"}, {"future": True}))
def test_discarded_data_cannot_enroll_a_foreign_child(params):
    peer, client, request = setup()
    peer.messages.insert(3, {"method": "future/informational", "params": params})
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    assert client._native_collaboration.children == {}


@pytest.mark.parametrize(
    "kind", ("request", "missing-id", "foreign-final", "own-policy", "no-final")
)
def test_ignored_information_cannot_bypass_authoritative_frames(kind):
    peer, client, request = setup()
    info = {"method": "future/informational", "params": {"threadId": "thread"}}
    peer.messages.insert(3, info)
    if kind == "request":
        peer.messages.insert(4, {**info, "id": 77})
    elif kind == "missing-id":
        peer.messages.insert(4, {"method": "item/tool/call", "params": {"threadId": "thread"}})
    elif kind == "foreign-final":
        peer.messages[-1]["params"]["threadId"] = "foreign"
    elif kind == "own-policy":
        peer.messages.insert(
            4,
            {
                "method": "thread/settings/updated",
                "params": {"threadId": "thread", "threadSettings": {}},
            },
        )
    else:
        peer.messages[-1] = deepcopy(info)
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.unresolved_effects
    assert result.output_text == ""
    assert result.backend_diagnostic.get("code") != "advisory-notification-ignored"
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1


def test_many_prebinding_notifications_use_task_deadline_not_a_count_quota():
    peer, client, request = setup()
    peer.messages[2:2] = [
        {
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "thread",
                "turnId": "turn",
                "itemId": "message",
                "delta": "progress",
            },
        }
        for _ in range(32)
    ]
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"


def test_unused_information_does_not_settle_an_unfinished_child():
    from test_codex_native_collaboration import spawn_completed, spawn_started

    peer, client, request = setup()
    peer.messages[3:3] = [
        spawn_started(),
        spawn_completed(),
        child_turn(),
        {"method": "future/informational", "params": {"threadId": "child", "status": "completed"}},
    ]
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.unresolved_effects


def test_workspace_result_notification_acceptance_and_distinct_next_task(tmp_path):
    from test_codex_client import Peer
    from test_workspace_tools import setup as workspace_setup

    from ahc.adapters.codex_client import CodexProtocolClient
    from ahc.adapters.codex_protocol import CODEX_EXECUTABLE_SHA256
    from ahc.application.runtime_service import RuntimeService
    from ahc.domain.agent_backend import AgentTurnRequest

    repo, workspace, tools = workspace_setup(tmp_path)
    try:
        for operation, steps in (("first", 22), ("next", 1)):
            peer = Peer()
            client = CodexProtocolClient(
                peer, cwd="workspace", model="fixture", clock=lambda: 0, workspace_tools=tools
            )
            snapshot = repo.read_current("session")
            request = AgentTurnRequest(
                snapshot.session,
                operation,
                snapshot.goal.digest,
                "ordinary task",
                (),
                CODEX_EXECUTABLE_SHA256,
                client.configuration_digest,
                "work",
                100,
                capabilities=tools.capabilities(),
            )
            events = []
            first_revision = workspace.identity.revision
            for number in range(steps):
                expected = workspace.identity.to_dict()
                expected["revision"] = first_revision + number
                events.append(
                    {
                        "id": 100 + number,
                        "method": "item/tool/call",
                        "params": {
                            "threadId": "thread",
                            "turnId": "turn",
                            "namespace": "workspace",
                            "tool": "execute",
                            "callId": f"call-{number}",
                            "arguments": {"expected": expected, "code": "x=42"},
                        },
                    }
                )
            events.extend(
                {
                    "method": method,
                    "params": {"threadId": "thread", "turnId": "turn", "willRetry": True},
                }
                for method in INFORMATION
            )
            peer.messages[3:3] = events
            from types import SimpleNamespace

            result = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
                snapshot, request
            )
            assert result.result["accepted"] and result.result["output_text"] == "answer"
            assert workspace.identity.revision == first_revision + steps
            assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1
        assert workspace.port.calls == 23
    finally:
        workspace.close()


def test_unused_notification_stream_still_observes_task_deadline():
    peer, client, request = setup()
    now = [0]
    client.clock = lambda: now[0]
    peer.messages[1:1] = [{"method": "future/informational", "params": {}} for _ in range(200)]
    receive = peer.receive

    def timed_receive(*, deadline):
        now[0] += 1
        return receive(deadline=deadline)

    peer.receive = timed_receive
    result = client.run_admitted(request)
    assert result.disposition == "not-submitted" and result.accepted_backend_turns == 0
    assert now[0] == request.deadline and peer.closed
    assert not any(m.get("method") == "turn/start" for m in peer.sent)


@pytest.mark.parametrize("params", [None, [], {}, {"threadId": "foreign", "turnId": "stale", "delta": 7}])
def test_command_output_is_discarded_before_owner_or_shape_validation(params):
    peer, client, request = setup()
    peer.messages.insert(3, {"method": "item/commandExecution/outputDelta", "params": params})
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert not client._native_collaboration.children
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1


def test_late_command_output_does_not_reopen_or_settle_a_child():
    from test_codex_native_collaboration import spawn_started, spawn_completed
    peer, client, request = setup()
    peer.messages[3:3] = [spawn_started(), spawn_completed(), child_turn(), child_turn("turn/completed", status="completed"),
        {"method": "item/commandExecution/outputDelta", "params": {
            "threadId": "child", "turnId": "child-turn", "itemId": "output", "delta": "late"}}]
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert client._native_collaboration.children["child"].terminal


@pytest.mark.parametrize("kind", ["request", "response-shaped", "forged-final"])
def test_command_output_cannot_hide_an_actionable_or_final_frame(kind):
    peer, client, request = setup()
    message = {"method": "item/commandExecution/outputDelta", "params": None}
    if kind == "request":
        message["id"] = 77
    elif kind == "response-shaped":
        message["result"] = {"status": "completed"}
    else:
        peer.messages[-1]["params"]["threadId"] = "foreign"
    peer.messages.insert(3, message)
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.output_text == ""
    assert result.unresolved_effects
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1


def test_unproven_foreign_settings_cannot_change_parent_policy_or_veto_completion():
    # This is the existing D-040 boundary, verified against the R18 classifier.
    peer, client, request = setup()
    peer.messages.insert(3, {"method": "thread/settings/updated", "params": {
        "threadId": "foreign", "threadSettings": {"model": "foreign-model"}}})
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert client.model == "fixture"
