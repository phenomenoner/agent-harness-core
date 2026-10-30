"""Conversation fixtures only; no process, credentials, or provider requests."""

from dataclasses import replace

import pytest

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.codex_protocol import CODEX_EXECUTABLE_SHA256
from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.sessions import ContractError, VirtualSessionIdentity, VirtualSessionRef


class Peer:
    def __init__(self):
        self.sent = []
        self.closed = False
        self.fail_write = False
        self.fail_close = False
        self.messages = [
            {
                "id": 1,
                "result": dict(
                    codexHome="PRIVATE",
                    platformFamily="windows",
                    platformOs="windows",
                    userAgent="PRIVATE",
                ),
            },
            {
                "id": 2,
                "result": dict(
                    cwd="workspace",
                    model="fixture",
                    modelProvider="openai",
                    approvalPolicy="never",
                    sandbox={"type": "readOnly", "networkAccess": False},
                    thread={"id": "thread"},
                ),
            },
            {"id": 3, "result": {"turn": {"id": "turn", "status": "inProgress"}}},
            {
                "method": "turn/completed",
                "params": {
                    "threadId": "thread",
                    "turn": {
                        "id": "turn",
                        "status": "completed",
                        "error": None,
                        "items": [
                            {
                                "type": "agentMessage",
                                "id": "item",
                                "phase": "final_answer",
                                "text": "answer",
                            }
                        ],
                    },
                },
            },
        ]

    def send(self, message, *, deadline):
        self.sent.append(message)
        if self.fail_write and message.get("method") == "turn/start":
            raise OSError("PRIVATE")

    def receive(self, *, deadline):
        return self.messages.pop(0)

    def close(self):
        if self.fail_close:
            raise OSError("PRIVATE")
        self.closed = True


def setup():
    peer = Peer()
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", clock=lambda: 0)
    session = VirtualSessionIdentity(
        "ahc.virtual-session.v1",
        VirtualSessionRef("ahc.virtual-session-ref.v1", "session", 1),
        "agent",
        "route",
        None,
    )
    request = AgentTurnRequest(
        session,
        "operation",
        "a" * 64,
        "question",
        (),
        CODEX_EXECUTABLE_SHA256,
        client.configuration_digest,
        "workspace",
        100,
    )
    return peer, client, request


def test_completed_conversation_and_no_replay():
    peer, client, request = setup()
    peer.messages.insert(
        2,
        {
            "method": "turn/started",
            "params": {"threadId": "thread", "turn": {"id": "turn", "status": "inProgress"}},
        },
    )
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert result.admissions == result.accepted_backend_turns == 1
    assert result.vendor_usage is None and peer.closed
    assert [m.get("method") for m in peer.sent] == [
        "initialize",
        "initialized",
        "thread/start",
        "turn/start",
    ]
    with pytest.raises(ContractError):
        client.run_admitted(request)


@pytest.mark.parametrize("field", ["artifact", "config", "limit", "capability"])
def test_unsupported_binding_never_sends(field):
    peer, client, request = setup()
    changes = {
        "artifact": {"artifact_digest": "b" * 64},
        "config": {"configuration_digest": "b" * 64},
        "limit": {"upstream_request_limit": 1},
        "capability": {"capabilities": ("shell",)},
    }
    result = client.run_admitted(replace(request, **changes[field]))
    assert result.disposition == "unsupported" and result.admissions == 0
    assert not peer.sent and peer.closed


def test_linux_target_uses_linux_artifact_and_distinct_configuration():
    from ahc.adapters.codex_linux_bundle import LINUX_CODEX_PIN

    peer, windows, request = setup()
    client = CodexProtocolClient(
        peer, cwd="workspace", model="fixture", clock=lambda: 0, target=LINUX_CODEX_PIN["target"]
    )
    assert client.configuration_digest != windows.configuration_digest
    assert client.executable_sha256 != CODEX_EXECUTABLE_SHA256
    peer.messages[0]["result"]["platformOs"] = "linux"
    peer.messages[0]["result"]["platformFamily"] = "unix"
    result = client.run_admitted(
        replace(
            request,
            artifact_digest=client.executable_sha256,
            configuration_digest=client.configuration_digest,
        )
    )
    assert result.disposition == "completed" and peer.closed


def test_linux_target_rejects_windows_digest_before_wire():
    from ahc.adapters.codex_linux_bundle import LINUX_CODEX_PIN

    peer, _, request = setup()
    client = CodexProtocolClient(
        peer, cwd="workspace", model="fixture", clock=lambda: 0, target=LINUX_CODEX_PIN["target"]
    )
    result = client.run_admitted(replace(request, configuration_digest=client.configuration_digest))
    assert result.disposition == "unsupported" and peer.sent == []


def test_platform_mismatch_rejects_before_thread_creation():
    peer, client, request = setup()
    peer.messages[0]["result"]["platformOs"] = "linux"
    result = client.run_admitted(request)
    assert result.disposition == "not-submitted" and peer.closed
    assert [item.get("method") for item in peer.sent] == ["initialize"]


def test_unknown_target_does_not_choose_a_fallback():
    peer = Peer()
    with pytest.raises(ContractError, match="artifact target"):
        CodexProtocolClient(peer, cwd="workspace", model="fixture", target="other")
    assert peer.sent == []


def test_indeterminate_submission_is_unknown_without_retry():
    peer, client, request = setup()
    peer.fail_write = True
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.unresolved_effects
    assert result.accepted_backend_turns == 0
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1
    assert "PRIVATE" not in str(result.public_dict())


@pytest.mark.parametrize("after_submission", [False, True])
def test_server_request_denied_without_exposing_parameters(after_submission):
    peer, client, request = setup()
    peer.messages.insert(
        3 if after_submission else 0,
        {
            "id": "approval",
            "method": "item/commandExecution/requestApproval",
            "params": {"secret": "PRIVATE"},
        },
    )
    result = client.run_admitted(request)
    assert result.disposition == ("unknown" if after_submission else "not-submitted")
    assert peer.sent[-1] == {
        "id": "approval",
        "error": {"code": -32601, "message": "unsupported method"},
    }
    assert "PRIVATE" not in str(result.public_dict())


def test_cleanup_failure_keeps_the_sealed_answer():
    peer, client, request = setup()
    sealed = []
    client.answer_seal = sealed.append
    peer.fail_close = True
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert not result.unresolved_effects and result.cleanup_pending
    assert len(sealed) == 1 and not sealed[0].cleanup_pending


def test_invalid_ack_does_not_count_as_accepted():
    peer, client, request = setup()
    peer.messages[2]["result"]["turn"]["status"] = "invented"
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.accepted_backend_turns == 0


def test_cancel_after_ack_interrupts_without_claiming_reconciliation():
    peer, client, request = setup()
    client.cancelled = lambda: client.accepted == 1
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.reason == "cancel-unreconciled"
    assert peer.sent[-1]["method"] == "turn/interrupt"
    assert result.unresolved_effects and peer.closed


@pytest.mark.parametrize("ambiguous", [False, True], ids=["completed", "ambiguous-write"])
def test_durable_runtime_reopen_does_not_resubmit_wire_conversation(tmp_path, ambiguous):
    from skill_test_support import goal

    from ahc.application.runtime_service import RuntimeService
    from ahc.domain.working_sets import WorkingSetSnapshot
    from ahc.infrastructure.runtime_repository import RuntimeRepository

    peer, client, request = setup()
    peer.fail_write = ambiguous
    repo = RuntimeRepository.initialize(tmp_path / "runtime")
    snapshot = WorkingSetSnapshot.initial(request.session, goal())
    repo.create(snapshot)
    request = replace(request, goal_digest=snapshot.goal.digest)

    class FixtureBackend:
        calls = 0

        def invoke(self, bound):
            self.calls += 1
            return client.run_admitted(bound)

    backend = FixtureBackend()
    terminal = RuntimeService(repo, backend).execute(snapshot, request)
    assert terminal.result["accepted"] is (not ambiguous)
    assert terminal.result["disposition"] == ("unknown" if ambiguous else "completed")
    reopened = RuntimeRepository(repo.root)
    assert reopened.read_turn(request.session, request.operation_id) == terminal
    intents = reopened.intents(request.session)
    assert len(intents) == 1 and intents[0]["kind"] == ("parked" if ambiguous else "mailbox")
    current = reopened.read_current("session")
    assert current.status == "active" and current.active_turn_id is None
    replay = RuntimeService(reopened, backend).execute(current, replace(request, deadline=200))
    assert replay == terminal and backend.calls == 1
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1
    assert peer.closed


@pytest.mark.parametrize("position", [1, 3])
def test_disabled_remote_status_does_not_break_response_correlation(position):
    peer, client, request = setup()
    peer.messages.insert(
        position,
        {
            "method": "remoteControl/status/changed",
            "emittedAtMs": 1,
            "params": {"installationId": "PRIVATE", "serverName": "PRIVATE", "status": "disabled"},
        },
    )
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert "PRIVATE" not in str(result)


@pytest.mark.parametrize("status", ["connected", "connecting", "errored"])
def test_active_remote_control_is_not_admitted(status):
    peer, client, request = setup()
    peer.messages.insert(
        1,
        {
            "method": "remoteControl/status/changed",
            "params": {"installationId": "PRIVATE", "serverName": "PRIVATE", "status": status},
        },
    )
    result = client.run_admitted(request)
    assert result.disposition == "not-submitted"
    assert not any(m.get("method") == "turn/start" for m in peer.sent)


def test_many_remote_status_notices_do_not_abort_valid_work():
    peer, client, request = setup()
    event = {
        "method": "remoteControl/status/changed",
        "params": {"installationId": "PRIVATE", "serverName": "PRIVATE", "status": "disabled"},
    }
    peer.messages[1:1] = [event] * 5000
    result = client.run_admitted(request)
    assert result.disposition == "completed" and client.frames == 5004
    assert peer.closed


@pytest.mark.parametrize("position", [1, 3])
def test_account_update_metadata_preserves_response_correlation(position):
    peer, client, request = setup()
    peer.messages.insert(position, {
        "method": "account/updated", "emittedAtMs": 1,
        "params": {"authMode": "chatgptAuthTokens", "planType": "pro"},
    })
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert result.accepted_backend_turns == 1
    assert [m.get("method") for m in peer.sent] == [
        "initialize", "initialized", "thread/start", "turn/start"]
    assert "chatgptAuthTokens" not in str(result.public_dict())


@pytest.mark.parametrize("params", [
    {"authMode": "unexpected", "planType": "pro"},
    {"authMode": "chatgptAuthTokens", "planType": True},
    {"authMode": "chatgptAuthTokens", "planType": "future-plan"},
    {"authMode": {"private": "PRIVATE"}, "planType": "pro"},
    {"authMode": "chatgptAuthTokens", "planType": "pro", "accessToken": "PRIVATE"},
])
def test_unused_account_metadata_cannot_suppress_a_turn(params):
    peer, client, request = setup()
    peer.messages.insert(1, {"method": "account/updated", "params": params})
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1
    assert result.backend_diagnostic["code"] == "advisory-metadata-ignored"
    assert "PRIVATE" not in str(result.public_dict())


def test_account_update_does_not_substitute_for_terminal_readback():
    peer, client, request = setup()
    peer.messages[-1] = {"method": "account/updated", "params": {"authMode": None, "planType": None}}
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.unresolved_effects
    assert result.accepted_backend_turns == 1
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1


def test_quota_model_metadata_does_not_interrupt_completed_turn():
    peer, client, request = setup()
    peer.messages.insert(3, {"method": "account/rateLimits/updated", "emittedAtMs": 1,
        "params": {"rateLimits": {"normalModelSlug": "fixture", "primary": None}}})
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert result.accepted_backend_turns == 1


@pytest.mark.parametrize("value", [True, [], {"private": "PRIVATE"}])
def test_unused_quota_model_metadata_cannot_suppress_a_turn(value):
    peer, client, request = setup()
    peer.messages.insert(1, {"method": "account/rateLimits/updated",
        "params": {"rateLimits": {"normalModelSlug": value}}})
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1
    assert "PRIVATE" not in str(result.public_dict())
