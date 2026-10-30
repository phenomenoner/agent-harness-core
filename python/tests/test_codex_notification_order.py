"""Pinned asynchronous notification ordering, without provider effects."""
import pytest
from test_codex_client import setup


def test_thread_started_after_response_and_timestamped_turn_complete():
    peer, client, request = setup()
    peer.messages.insert(2, {"method": "thread/started", "params": {"thread": {"id": "thread"}}, "emittedAtMs": 1})
    peer.messages[-1]["emittedAtMs"] = 2
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    assert result.output_text == "answer"
    assert peer.closed


@pytest.mark.parametrize("kind", ["wrong-thread", "duplicate", "bad-time", "extra"])
def test_late_thread_notification_does_not_weaken_binding(kind):
    peer, client, request = setup()
    event = {"method": "thread/started", "params": {"thread": {"id": "thread"}}, "emittedAtMs": 1}
    if kind == "wrong-thread": event["params"]["thread"]["id"] = "other"
    if kind == "bad-time": event["emittedAtMs"] = True
    if kind == "extra": event["allow"] = True
    peer.messages.insert(2, event)
    if kind == "duplicate": peer.messages.insert(3, event)
    assert client.run_admitted(request).disposition == "unknown"
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1
    assert peer.closed


@pytest.mark.parametrize("method,params", [
    ("mcpServer/startupStatus/updated", {"threadId":"thread", "name":"fixture", "status":"starting", "error":None, "failureReason":None}),
    ("thread/status/changed", {"threadId":"thread", "status":{"type":"active", "activeFlags":[]}}),
])
def test_information_before_turn_response_is_not_a_terminal(method, params):
    peer, client, request = setup()
    peer.messages.insert(2, {"method":method, "params":params, "emittedAtMs":1})
    result=client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"


@pytest.mark.parametrize("wrong", ["thread", "shape"])
def test_status_notification_remains_bound(wrong):
    peer, client, request = setup()
    params={"threadId":"thread", "status":{"type":"idle"}}
    if wrong == "thread":params["threadId"]="other"
    else:params["status"]={"type":"approved"}
    peer.messages.insert(2,{"method":"thread/status/changed", "params":params})
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    assert result.backend_diagnostic["code"] == "advisory-status-ignored"
    assert result.backend_thread == "thread"


def test_usage_and_reasoning_do_not_replace_terminal_output():
    peer, client, request = setup()
    counts={"inputTokens":1,"outputTokens":1,"cachedInputTokens":0,"reasoningOutputTokens":0,"totalTokens":2}
    peer.messages[3:3]=[
        {"method":"thread/tokenUsage/updated", "params":{"threadId":"thread","turnId":"turn","tokenUsage":{"last":counts,"total":counts}}},
        {"method":"item/reasoning/summaryTextDelta","params":{"threadId":"thread","turnId":"turn","itemId":"thought","delta":"not final"}},
    ]
    result=client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert result.vendor_usage is None


@pytest.mark.parametrize("drift", [False, True])
def test_thread_settings_revalidated_before_turn_response(drift):
    peer, client, request = setup()
    settings = dict(peer.messages[1]["result"])
    settings.pop("thread")
    settings["sandboxPolicy"] = settings.pop("sandbox")
    if drift:settings["sandboxPolicy"]={"type":"dangerFullAccess"}
    peer.messages.insert(2,{"method":"thread/settings/updated","params":{"threadId":"thread","threadSettings":settings}})
    result=client.run_admitted(request)
    assert result.disposition == ("unknown" if drift else "completed")


@pytest.mark.parametrize("malformed", [False, True])
def test_global_rate_limits_do_not_require_thread_or_authorize_completion(malformed):
    peer, client, request=setup()
    params={"rateLimits":{"primary":{"usedPercent":3,"windowDurationMins":300,"resetsAt":123}}}
    if malformed:params["approved"]=True
    peer.messages.insert(3,{"method":"account/rateLimits/updated","params":params})
    result=client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert result.vendor_usage is None


@pytest.mark.parametrize("method", ["account/updated", "account/rateLimits/updated",
                                   "warning", "configWarning", "mcpServer/startupStatus/updated"])
@pytest.mark.parametrize("position", [1, 3])
def test_discarded_advisory_payload_never_becomes_authority(method, position):
    peer, client, request = setup()
    peer.messages.insert(position, {"method": method, "emittedAtMs": "PRIVATE",
        "params": {"threadId": "foreign", "approved": True, "accessToken": "PRIVATE"},
        "futureField": "PRIVATE"})
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert result.backend_thread == "thread" and result.vendor_usage is None
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1
    assert "PRIVATE" not in str(result.public_dict())


@pytest.mark.parametrize("fault", ["counts", "foreign-thread", "foreign-turn"])
def test_unusable_usage_does_not_erase_a_completed_answer_or_reuse_old_counts(fault):
    peer, client, request = setup()
    counts = {"inputTokens": 1, "outputTokens": 1, "cachedInputTokens": 0,
              "reasoningOutputTokens": 0, "totalTokens": 2}
    params = {"threadId": "thread", "turnId": "turn",
              "tokenUsage": {"last": counts, "total": counts}}
    bad = {**params, "tokenUsage": {"last": {**counts, "inputTokens": True}, "total": counts}}
    if fault == "foreign-thread":
        bad = {**params, "threadId": "foreign"}
    elif fault == "foreign-turn":
        bad = {**params, "turnId": "foreign"}
    peer.messages[3:3] = [{"method": "thread/tokenUsage/updated", "params": params},
                           {"method": "thread/tokenUsage/updated", "params": bad}]
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    if fault == "foreign-thread":
        assert result.token_observation.last.total_tokens == 2
    else:
        assert result.token_observation is None
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1


def test_advisory_rpc_request_is_not_silently_consumed():
    peer, client, request = setup()
    peer.messages.insert(1, {"id": 77, "method": "account/updated",
                             "params": {"approved": True}})
    result = client.run_admitted(request)
    assert result.disposition == "not-submitted"
    assert not any(m.get("method") == "turn/start" for m in peer.sent)


@pytest.mark.parametrize("drift", [False, True])
def test_summary_completion_requires_exact_full_turn_readback(drift):
    peer, client, request=setup()
    original=peer.messages[-1]["params"]["turn"]
    full=dict(original,itemsView="full")
    if drift:full["id"]="other"
    peer.messages[-1]["params"]["turn"]=dict(original,itemsView="summary",items=[])
    peer.messages.append({"id":5,"result":{"data":[full],"nextCursor":None}})
    result=client.run_admitted(request)
    assert result.disposition == ("unknown" if drift else "completed")
    assert sum(m.get("method")=="turn/start" for m in peer.sent)==1
    assert peer.sent[-1]["method"]=="thread/turns/list"
    assert peer.sent[-1]["params"]["itemsView"]=="full"
