"""Continuation ordering and incomplete-report sealing regressions."""
from dataclasses import replace

import pytest
from test_codex_capacity_recovery import Clock, acknowledgement, completed, configured, failed
from test_codex_client import setup
from test_codex_context_policy import usage

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.domain.runtime_turn import accept_observation, answer_seal_projection
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize("attempt", [1, 2])
@pytest.mark.parametrize("kind", ["status", "error", "tool", "approval"])
def test_each_recovery_buffers_legal_preack_frames_until_exact_binding(attempt, kind):
    peer, client, request, clock = configured()
    tools = port = None
    if kind == "tool":
        from test_codex_workspace_tools import bridge, message
        _, tools = bridge()
        peer, _, template = setup()
        client = CodexProtocolClient(peer, cwd="workspace", model="fixture", clock=clock,
            workspace_tools=tools, recovery_wait=clock.wait,
            overload_recovery={"delays_seconds": [120, 300]})
        request = replace(template, configuration_digest=client.configuration_digest,
            capabilities=tools.capabilities(), workspace_scope="work", deadline=1000)
        frame = message()
    elif kind == "approval":
        from test_codex_approval import DecisionPort, approval_frame
        from test_codex_effective_policy import Peer, policy
        from test_codex_effective_policy import request as policy_request
        effective, port = policy(), DecisionPort("approve-once")
        peer = Peer(effective)
        client = CodexProtocolClient(peer, cwd="workspace", model="fixture", clock=clock,
            effective_policy=effective, approval_port=port, recovery_wait=clock.wait,
            overload_recovery={"delays_seconds": [120, 300]})
        request = replace(policy_request(client, effective), deadline=1000)
        frame = approval_frame()
    elif kind == "error":
        frame = {"method": "error", "params": {"threadId": "thread", "turnId": "final",
            "willRetry": True, "error": {"codexErrorInfo": "serverOverloaded"}}}
    else:
        frame = {"method": "thread/status/changed", "params": {"threadId": "thread",
            "status": {"type": "active", "activeFlags": []}}}
    if kind in {"tool", "approval"}:
        frame["params"]["turnId"] = "final"
    prefix = [failed()]
    if attempt == 2:
        prefix += [acknowledgement(1, "first"), failed("first")]
    peer.messages[3:] = prefix + [frame, acknowledgement(attempt, "final"), completed("final")]
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.backend_recovery["attempts"] == attempt
    if tools is not None:
        assert tools.calls == 1
    if port is not None:
        assert len(port.requests) == 1


@pytest.mark.parametrize("attempt", [1, 2])
@pytest.mark.parametrize("variant", ["foreign", "old-turn", "malformed"])
def test_preack_tool_never_borrows_authority_from_bad_identity(attempt, variant):
    from test_codex_workspace_tools import bridge, message
    _, tools = bridge()
    peer, _, template = setup()
    clock = Clock()
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", clock=clock,
        workspace_tools=tools, recovery_wait=clock.wait,
        overload_recovery={"delays_seconds": [120, 300]})
    request = replace(template, configuration_digest=client.configuration_digest,
        capabilities=tools.capabilities(), workspace_scope="work", deadline=1000)
    frame = message()
    frame["params"]["turnId"] = "final"
    if variant == "foreign":
        frame["params"]["threadId"] = "foreign"
    elif variant == "old-turn":
        frame["params"]["turnId"] = "turn"
    else:
        frame["params"].pop("namespace")
    prefix = [failed()]
    if attempt == 2:
        prefix += [acknowledgement(1, "first"), failed("first")]
    peer.messages[3:] = prefix + [frame, acknowledgement(attempt, "final"), completed("final")]
    result = client.run_admitted(request)
    assert tools.calls == 0
    assert result.disposition == "unknown"
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == attempt + 1


@pytest.mark.parametrize("attempt", [1, 2])
def test_recovered_problem_report_seals_with_bounded_native_turn_counts(attempt):
    peer, client, request, _ = configured()
    prefix = [failed()]
    if attempt == 2:
        prefix += [acknowledgement(1, "first"), failed("first")]
    peer.messages[3:] = prefix + [acknowledgement(attempt, "final"), completed("final")]
    observed = client.run_admitted(request)
    report = replace(observed, final_report=True, unresolved_effects=True)
    durable = accept_observation(request, report)
    sealed = answer_seal_projection(durable)
    assert sealed["report_accepted"] and not sealed["accepted"]
    assert sealed["admissions"] == sealed["accepted_backend_turns"] == attempt + 1
    for changes in ({"admissions": 1}, {"accepted_backend_turns": 4},
                    {"backend_recovery": None}, {"admissions": True}):
        with pytest.raises(ContractError):
            answer_seal_projection({**durable, **changes})


@pytest.mark.parametrize("finished", [False, True])
def test_compaction_state_cannot_pair_old_turn_tokens_with_new_turn_usage(finished):
    peer, client, request, _ = configured()
    def boundary(method, turn):
        return {"method": method, "params": {"threadId": "thread", "turnId": turn,
            "item": {"id": "same-item", "type": "contextCompaction"}}}
    prefix = [usage(350000), boundary("item/started", "turn")]
    if finished:
        prefix += [boundary("item/completed", "turn")]
    after = usage(60000)
    after["params"]["turnId"] = "final"
    peer.messages[3:] = prefix + [failed(), acknowledgement(1, "final"),
        boundary("item/completed", "final"), after, completed("final")]
    observed = client.run_admitted(request)
    assert observed.disposition == "completed"
    assert "context_compaction" not in observed.token_observation.to_dict()
