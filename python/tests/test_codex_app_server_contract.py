"""Version-specific app-server seams; no process, account, or provider access."""

import json
from pathlib import Path

import pytest
from test_codex_client import setup
from test_codex_native_collaboration import (
    CHILD_THREAD,
    CHILD_TURN,
    child_status,
    child_turn,
    completed_child_events,
    natural_events,
    run_events,
)


def activity(kind="started", *, child=CHILD_THREAD, path="/root/worker", item="activity"):
    return {
        "method": "item/completed",
        "params": {
            "threadId": "thread",
            "turnId": "turn",
            "item": {
                "type": "subAgentActivity", "id": item, "kind": kind,
                "agentThreadId": child, "agentPath": path,
            },
        },
    }


def test_v2_spawn_then_child_terminal_preserves_parent_final():
    _, client, _, result = run_events([activity(), *completed_child_events()])
    assert result.disposition == "completed" and result.output_text == "answer"
    assert client._native_collaboration.children[CHILD_THREAD].terminal


def test_activity_completion_cannot_substitute_for_child_turn_terminal():
    _, _, _, result = run_events([
        activity(), child_turn(), activity("completed", item="done"),
    ])
    assert result.disposition == "unknown"


def test_late_liveness_does_not_reopen_completed_child():
    _, _, _, result = run_events([*natural_events(), child_status()])
    assert result.disposition == "completed"


def test_reasoning_before_turn_start_ack_is_processed_after_binding():
    peer, client, request = setup()
    peer.messages.insert(2, {
        "method": "item/reasoning/textDelta",
        "params": {"threadId": "thread", "turnId": "turn", "itemId": "reason", "delta": "x"},
    })
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1


def test_matching_rpc_rejection_retains_safe_cause():
    peer, client, request = setup()
    peer.messages[1] = {"id": 2, "error": {"code": -32001, "message": "PRIVATE"}}
    result = client.run_admitted(request)
    assert result.disposition == "not-submitted"
    assert result.backend_diagnostic["backend_failure"]["kind"] == "overloaded"
    assert "PRIVATE" not in json.dumps(result.public_dict())


def test_nonretryable_unauthorized_is_classified_without_payload():
    peer, client, request = setup()
    peer.messages.insert(3, {
        "method": "error",
        "params": {
            "threadId": "thread", "turnId": "turn", "willRetry": False,
            "error": {"message": "PRIVATE", "codexErrorInfo": "unauthorized"},
        },
    })
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.unresolved_effects
    assert result.backend_diagnostic["backend_failure"]["kind"] == "unauthorized"
    assert "PRIVATE" not in json.dumps(result.public_dict())


def test_owned_native_child_tool_denial_stays_local():
    call = {"id": 44, "method": "item/tool/call", "params": {
        "threadId": CHILD_THREAD, "turnId": CHILD_TURN,
        "callId": "child-call", "tool": "execute", "namespace": "workspace", "arguments": {},
    }}
    peer, _, _, result = run_events([
        activity(), child_turn(), call,
        child_turn("turn/completed", status="failed", error={"message": "not available"}),
    ])
    assert result.disposition == "completed"
    reply = next(m for m in peer.sent if m.get("id") == 44)
    assert reply["result"]["success"] is False


@pytest.mark.parametrize("before_ack", [False, True])
def test_current_time_is_a_scoped_read_with_no_new_turn(before_ack):
    peer, client, request = setup()
    client._wall_clock = lambda: 1792030000.9
    peer.messages.insert(2 if before_ack else 3, {
        "id": 90, "method": "currentTime/read", "params": {"threadId": "thread"},
    })
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    assert {"id": 90, "result": {"currentTimeAt": 1792030000}} in peer.sent
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1


@pytest.mark.parametrize("method", [
    "item/permissions/requestApproval", "item/tool/requestUserInput",
    "mcpServer/elicitation/request",
])
def test_unsupported_interaction_refuses_effect_but_allows_model_to_report(method):
    peer, client, request = setup()
    peer.messages.insert(3, {"id": 91, "method": method, "params": {
        "threadId": "thread", "turnId": "turn", "permissions": {"network": True},
    }})
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    reply = next(m for m in peer.sent if m.get("id") == 91)
    assert reply["error"]["code"] == -32601 and "result" not in reply


@pytest.mark.parametrize("method,kind", [
    ("account/chatgptAuthTokens/refresh", "auth-refresh-required"),
    ("attestation/generate", "attestation-required"),
])
def test_unsupported_required_backend_request_never_fabricates_authority(method, kind):
    peer, client, request = setup()
    peer.messages.insert(3, {"id": 92, "method": method, "params": {"PRIVATE": "TOKEN"}})
    result = client.run_admitted(request)
    assert result.disposition == "unknown"
    assert result.backend_diagnostic["backend_failure"]["kind"] == kind
    assert "TOKEN" not in json.dumps(result.public_dict())
    assert next(m for m in peer.sent if m.get("id") == 92)["error"]["code"] == -32601


def test_v2_activity_extra_metadata_cannot_change_binding_or_terminal():
    event = activity()
    event["params"]["item"]["futureMetadata"] = {"status": "completed"}
    _, _, _, result = run_events([event, *completed_child_events()])
    assert result.disposition == "completed"


@pytest.mark.parametrize("change", ["parent-turn", "parent-path", "duplicate", "foreign-tool"])
def test_v2_foreign_or_reused_evidence_cannot_gain_parent_authority(change):
    events = [activity(), *completed_child_events()]
    if change == "parent-turn":
        events[0]["params"]["turnId"] = "foreign"
    elif change == "parent-path":
        events[0]["params"]["item"]["agentPath"] = "/root/other/nested"
    elif change == "duplicate":
        events.insert(1, activity())
    else:
        events.insert(2, {"id": 44, "method": "item/tool/call", "params": {
            "threadId": "foreign", "turnId": CHILD_TURN, "tool": "execute", "arguments": {},
        }})
    _, _, _, result = run_events(events)
    assert result.disposition == "unknown"


def test_routing_401_template_is_a_specific_cause_not_an_auth_refresh():
    peer, client, request = setup()
    peer.messages.insert(3, {"method": "error", "params": {
        "threadId": "thread", "turnId": "turn", "willRetry": False,
        "error": {"message": "workspace routing discovery unauthorized (401)",
                  "codexErrorInfo": "other"},
    }})
    result = client.run_admitted(request)
    failure = result.backend_diagnostic["backend_failure"]
    assert failure["kind"] == "routing-unauthorized" and failure["http_status"] == 401
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1


def test_terminal_error_without_error_notification_keeps_typed_cause():
    peer, client, request = setup()
    peer.messages[-1]["params"]["turn"].update(
        status="failed", error={"message": "PRIVATE", "codexErrorInfo": "contextWindowExceeded"})
    result = client.run_admitted(request)
    assert result.disposition == "unknown"
    assert result.backend_diagnostic["backend_failure"]["kind"] == "context-window"


def test_malformed_optional_error_metadata_is_dropped_without_raw_payload():
    from ahc.domain.backend_diagnostics import diagnostic_projection

    _, client, request = setup()
    result = client.run_admitted(request)
    diagnostic = dict(result.backend_diagnostic or {
        "category": "backend-failed", "phase": "decode", "last_method": "error", "code": "none",
        "remaining_ms": 1, "frames": 1, "cleanup": "not-observed",
    }, backend_failure={"source": [], "kind": ["PRIVATE"]})
    assert "PRIVATE" not in json.dumps(diagnostic_projection(diagnostic))


def test_supported_request_catalog_and_activity_match_exact_version_schema():
    import jsonschema

    from ahc.adapters.codex_server_requests import SERVER_REQUEST_METHODS

    fixture = json.loads((Path(__file__).parent / "fixtures/codex_0_160_1_surface.json")
                         .read_text(encoding="utf-8"))
    assert SERVER_REQUEST_METHODS == set(fixture["server_request_methods"])
    jsonschema.validate(activity()["params"]["item"], fixture["subagent_activity_schema"])
    jsonschema.validate({"currentTimeAt": 1792030000}, fixture["current_time_response_schema"])


def test_current_linux_pin_matches_generated_upstream_request_surface():
    import jsonschema

    from ahc.adapters.codex_linux_bundle import LINUX_CODEX_PIN
    from ahc.adapters.codex_server_requests import SERVER_REQUEST_METHODS

    fixture = json.loads((Path(__file__).parent / "fixtures/codex_0_163_0_surface.json")
                         .read_text(encoding="utf-8"))
    assert fixture["provenance"]["version"] == LINUX_CODEX_PIN["version"]
    executable = next(m for m in LINUX_CODEX_PIN["members"]
                      if m["path"] == LINUX_CODEX_PIN["entrypoint"])
    assert fixture["provenance"]["executable_sha256"] == executable["sha256"]
    assert SERVER_REQUEST_METHODS == set(fixture["server_request_methods"])
    peer, client, request = setup()
    assert client.run_admitted(request).disposition == "completed"
    for message in peer.sent:
        if message.get("method") in ("thread/start", "turn/start"):
            key = message["method"].replace("/", "_") + "_properties"
            assert set(message["params"]) <= set(fixture[key])
    jsonschema.validate({"currentTimeAt": 1792030000}, fixture["current_time_response_schema"])


@pytest.mark.parametrize("method", [
    "currentTime/read", "attestation/generate", "item/permissions/requestApproval",
    "account/chatgptAuthTokens/refresh", "applyPatchApproval", "execCommandApproval",
])
def test_missing_request_id_cannot_be_discarded_as_information(method):
    peer, client, request = setup()
    peer.messages.insert(3, {"method": method, "params": {"threadId": "thread", "turnId": "turn"}})
    result = client.run_admitted(request)
    assert result.disposition == "unknown"
    assert client._native_collaboration.children == {}


def test_foreign_clock_request_cannot_use_parent_identity():
    peer, client, request = setup()
    peer.messages.insert(3, {"id": 93, "method": "currentTime/read",
                             "params": {"threadId": "foreign"}})
    result = client.run_admitted(request)
    assert result.disposition == "unknown"
    assert not any(m.get("id") == 93 and "result" in m for m in peer.sent)


def test_tool_activity_observation_never_clears_unknown_effects():
    peer, client, request = setup()
    peer.messages[3:3] = [
        {"method": "item/started", "params": {"threadId": "thread", "turnId": "turn", "item": {
            "type": "commandExecution", "id": "command",
        }}},
        {"method": "error", "params": {"threadId": "thread", "turnId": "turn", "willRetry": False,
                                         "error": {"codexErrorInfo": "unauthorized",
                                                   "message": "PRIVATE"}}},
    ]
    result = client.run_admitted(request)
    assert result.unresolved_effects and result.disposition == "unknown"
    assert result.backend_diagnostic["backend_failure"]["tool_activity"] == "observed"


def test_owned_child_current_time_uses_its_thread_without_parent_turn_authority():
    peer, _, _, result = run_events([
        activity(), child_turn(),
        {"id": 94, "method": "currentTime/read", "params": {"threadId": CHILD_THREAD}},
        child_turn("turn/completed", status="completed"),
    ])
    assert result.disposition == "completed"
    assert type(next(m for m in peer.sent if m.get("id") == 94)["result"]["currentTimeAt"]) is int


@pytest.mark.parametrize("name", ["child-thread", "Worker.2", "研究員"])
def test_v2_paths_do_not_restrict_provider_agent_names(name):
    _, _, _, result = run_events([activity(path=f"/root/{name}"), *completed_child_events()])
    assert result.disposition == "completed"


def _from_child(event, sender="a", turn="ta"):
    event["params"].update(threadId=sender, turnId=turn)
    return event


@pytest.mark.parametrize("target,path", [("b", "/root/b"), ("thread", "/root")])
def test_v2_owned_sibling_or_parent_interaction_keeps_existing_ownership(target, path):
    _, client, _, result = run_events([
        activity(child="a", path="/root/a", item="spawn-a"), child_turn(thread="a", turn="ta"),
        activity(child="b", path="/root/b", item="spawn-b"), child_turn(thread="b", turn="tb"),
        _from_child(activity("interacted", child=target, path=path, item="message")),
        child_turn("turn/completed", thread="a", turn="ta", status="completed"),
        child_turn("turn/completed", thread="b", turn="tb", status="completed"),
    ])
    assert result.disposition == "completed"
    assert {c.parent for c in client._native_collaboration.children.values()} == {"thread"}


def test_legacy_parent_path_is_inferred_from_its_v2_descendant():
    from test_codex_native_collaboration import spawn_completed, spawn_started

    _, client, _, result = run_events([
        spawn_started(), spawn_completed(child="a"), child_turn(thread="a", turn="ta"),
        _from_child(activity(child="b", path="/root/legacy-parent/child-thread", item="nested")),
        *completed_child_events(child="b", turn="tb"),
        child_turn("turn/completed", thread="a", turn="ta", status="completed"),
    ])
    assert result.disposition == "completed"
    children = client._native_collaboration.children
    assert children["a"].agent_path == "/root/legacy-parent" and children["b"].parent == "a"


@pytest.mark.parametrize("kind,target,path", [
    ("interacted", "foreign", "/root/foreign"),
    ("interacted", "b", "/root/a"),
    ("started", "b", "/root/a/b"),
    ("started", "other", "/root/b"),
])
def test_v2_interactions_cannot_enroll_reparent_or_alias_threads(kind, target, path):
    _, _, _, result = run_events([
        activity(child="a", path="/root/a", item="spawn-a"), child_turn(thread="a", turn="ta"),
        activity(child="b", path="/root/b", item="spawn-b"), child_turn(thread="b", turn="tb"),
        _from_child(activity(kind, child=target, path=path, item="message")),
    ])
    assert result.disposition == "unknown"


def test_distinct_owned_threads_cannot_share_one_agent_path():
    _, _, _, result = run_events([
        activity(child="a", path="/root/same", item="spawn-a"),
        activity(child="b", path="/root/same", item="spawn-b"),
    ])
    assert result.disposition == "unknown"


def test_inferred_legacy_path_cannot_be_replaced_by_conflicting_provenance():
    from test_codex_native_collaboration import spawn_completed, spawn_started

    _, _, _, result = run_events([
        spawn_started(), spawn_completed(child="a"), child_turn(thread="a", turn="ta"),
        _from_child(activity(child="b", path="/root/a/b", item="nested")),
        {"method": "thread/started", "params": {"thread": {
            "id": "a", "source": {"subAgent": {"thread_spawn": {
                "parent_thread_id": "thread", "agent_path": "/root/different"}}}}}},
    ])
    assert result.disposition == "unknown"
