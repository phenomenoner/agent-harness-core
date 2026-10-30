"""No provider/process/auth operations: serialized inert protocol discriminators."""

import json
from dataclasses import replace

import pytest

from ahc.adapters.inert_agent_backend import InertAgentBackend, InertPeer
from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


def request(**changes):
    value = AgentTurnRequest(
        VirtualSessionIdentity.initial("session", "agent", "route"),
        "operation", "a" * 64, "hello", ("b" * 64,), "c" * 64, "d" * 64,
        "workspace", 100,
    )
    return replace(value, **changes)


def frame(value):
    return json.dumps(value).encode()


def prefix():
    return (
        frame({"id": 1, "result": {"protocol": "inert-v1"}}),
        frame({"id": 2, "result": {"threadId": "thread"}}),
        frame({"id": 3, "result": {"threadId": "thread", "turnId": "turn"}}),
    )


def terminal(**changes):
    return frame({"method": "turn/completed", "params": {
        "threadId": "thread", "turnId": "turn", "status": "completed", "output": "answer",
        **changes,
    }})


def invoke(frames, req=None, **kwargs):
    peer = InertPeer(frames)
    backend = InertAgentBackend(peer, clock=lambda: 0, **kwargs)
    outcome = backend.invoke(req or request())
    return outcome, peer, backend


@pytest.mark.parametrize("resume", [None, "thread"])
def test_bidirectional_sequence_and_exact_binding(resume):
    progress = frame({"method": "turn/progress", "params": {
        "threadId": "thread", "turnId": "turn",
    }})
    req = request(backend_thread=resume)
    result, peer, _ = invoke(prefix() + (progress, terminal()), req)
    assert result.disposition == "completed"
    assert result.request is req
    assert result.output_text == "answer"
    assert (result.admissions, result.accepted_backend_turns, result.vendor_usage) == (1, 1, None)
    sent = [json.loads(item) for item in peer.sent]
    assert [item["method"] for item in sent] == [
        "initialize", "initialized", "thread/resume" if resume else "thread/start", "turn/start",
    ]
    params = sent[-1]["params"]
    assert params["session"] == req.session.to_dict()
    assert params["goalDigest"] == req.goal_digest
    assert params["inputDigest"] == req.input_digest
    assert params["artifactDigest"] == req.artifact_digest
    assert params["configurationDigest"] == req.configuration_digest
    assert params["workspaceScope"] == req.workspace_scope
    assert params["revisionDigests"] == list(req.revision_digests)
    assert peer.closed and not result.unresolved_effects


@pytest.mark.parametrize("changes", [{"capabilities": ("network",)}, {"upstream_request_limit": 1}])
def test_unsupported_envelope_has_zero_admission(changes):
    result, peer, _ = invoke((), request(**changes))
    assert result.disposition == "unsupported"
    assert result.admissions == result.accepted_backend_turns == 0
    assert peer.sent == [] and peer.closed


@pytest.mark.parametrize("frames,submitted", [((), False), (prefix()[:2], True), (prefix(), True)])
def test_loss_counters_unknown_and_no_instance_replay(frames, submitted):
    result, peer, backend = invoke(frames)
    assert result.disposition == ("unknown" if submitted else "not-submitted")
    assert result.unresolved_effects is submitted
    count = len(peer.sent)
    with pytest.raises(ContractError, match="replay"):
        backend.invoke(request())
    assert len(peer.sent) == count


def test_unknown_server_request_rejects_without_echoing_arguments():
    server = frame({"id": 9, "method": "item/commandExecution/requestApproval",
                    "params": {"secret": "FAKE-CREDENTIAL-SENTINEL"}})
    result, peer, _ = invoke(prefix() + (server, terminal()))
    assert result.disposition == "unknown" and result.unresolved_effects
    assert json.loads(peer.sent[-1]) == {
        "id": 9, "error": {"code": -32601, "message": "unsupported method"},
    }
    assert "FAKE-CREDENTIAL-SENTINEL" not in json.dumps(result.public_dict())
    assert b"FAKE-CREDENTIAL-SENTINEL" not in b"".join(peer.sent)


@pytest.mark.parametrize("bad", [
    terminal(threadId="foreign"), terminal(turnId="foreign"),
    terminal(status="failed"), terminal(output="\ud800"),
    frame({"method": "unknown", "params": {"threadId": "thread", "turnId": "turn"}}),
    b'{"method":"turn/completed","method":"turn/progress","params":{}}',
    frame({"method": "turn/progress", "params": {
        "threadId": "thread", "turnId": "turn", "diagnostic": "FAKE-SENTINEL",
    }}),
])
def test_malformed_or_foreign_post_submit_never_completes(bad):
    result, peer, _ = invoke(prefix() + (bad,))
    assert result.disposition == "unknown" and peer.closed
    assert result.output_text == ""
    assert "FAKE-SENTINEL" not in json.dumps(result.public_dict())


def test_resume_wrong_thread_is_not_submitted():
    result, peer, _ = invoke(prefix(), request(backend_thread="other"))
    assert result.disposition == "not-submitted"
    assert not any(json.loads(item).get("method") == "turn/start" for item in peer.sent)


def test_cancel_after_acceptance_interrupts_but_does_not_claim_no_effect():
    checks = iter([False, False, True])
    result, peer, _ = invoke(prefix() + (terminal(),), cancelled=lambda: next(checks))
    assert result.disposition == "unknown" and result.reason == "cancel-unreconciled"
    assert result.accepted_backend_turns == 1
    assert json.loads(peer.sent[-1])["method"] == "turn/interrupt"


def test_expired_before_submission_has_no_admission():
    result, peer, _ = invoke((), request(deadline=-1))
    assert result.disposition == "not-submitted" and result.admissions == 0
    assert not peer.sent


@pytest.mark.parametrize("changes", [
    {"deadline": float("nan")}, {"revision_digests": ["b" * 64]},
    {"purpose": "model.invoke"}, {"upstream_request_limit": True},
])
def test_invalid_binding_rejected(changes):
    with pytest.raises(ContractError):
        request(**changes)


def test_peer_rejects_oversize_transcript():
    with pytest.raises(ContractError):
        InertPeer((b"x" * 16385,))
