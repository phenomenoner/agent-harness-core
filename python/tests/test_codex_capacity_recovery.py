"""Capacity continuation with a controlled protocol peer and virtual time."""

from dataclasses import replace

import pytest
from test_codex_client import Peer, setup

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.domain.runtime_turn import accept_observation


class Clock:
    def __init__(self):
        self.now = 0.0
        self.waits = []
        self.cancelled = False

    def __call__(self):
        return self.now

    def wait(self, seconds):
        self.waits.append(seconds)
        self.now += seconds


def failed(turn="turn", kind="serverOverloaded"):
    return {"method": "turn/completed", "params": {"threadId": "thread", "turn": {
        "id": turn, "status": "failed", "error": {"message": "PRIVATE",
        "codexErrorInfo": kind}, "items": []}}}


def acknowledgement(attempt, turn):
    return {"id": 2_000_000 + attempt,
            "result": {"turn": {"id": turn, "status": "inProgress"}}}


def completed(turn):
    peer = Peer()
    final = peer.messages[-1]
    final["params"]["turn"]["id"] = turn
    return final


def configured(*, delays=(120, 300), deadline=1000):
    peer, _, original = setup()
    clock = Clock()
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", clock=clock,
        cancelled=lambda: clock.cancelled, recovery_wait=clock.wait,
        overload_recovery={"delays_seconds": list(delays)})
    request = replace(original, configuration_digest=client.configuration_digest, deadline=deadline)
    return peer, client, request, clock


def test_exact_terminal_capacity_failure_cools_down_then_continues_same_thread():
    peer, client, request, clock = configured()
    peer.messages[3:] = [
        {"method": "error", "params": {"threadId": "thread", "turnId": "turn",
            "willRetry": False, "error": {"codexErrorInfo": "serverOverloaded"}}},
        failed(), acknowledgement(1, "continued"), completed("continued")]
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert clock.now == 120
    assert result.backend_thread == "thread" and result.backend_turn == "continued"
    assert result.admissions == result.accepted_backend_turns == 2
    assert result.backend_recovery == {
        "attempts": 1, "state": "recovered", "last_failure": "overloaded"}
    starts = [m for m in peer.sent if m.get("method") == "turn/start"]
    assert len(starts) == 2 and starts[1]["params"]["threadId"] == "thread"
    assert starts[1]["params"]["input"][0]["text"] != request.input_text
    assert "Do not restart" in starts[1]["params"]["input"][0]["text"]
    assert accept_observation(request, result)["accepted"] is True
    assert "PRIVATE" not in str(result.public_dict())


def test_two_additional_attempts_exhaust_without_a_fourth_turn():
    peer, client, request, clock = configured()
    peer.messages[3:] = [failed(), acknowledgement(1, "again-1"), failed("again-1"),
        acknowledgement(2, "again-2"), failed("again-2")]
    result = client.run_admitted(request)
    assert clock.now == 420 and sum(clock.waits) == 420
    assert result.disposition == "unknown" and result.unresolved_effects
    assert result.backend_recovery["state"] == "exhausted"
    assert result.backend_recovery["attempts"] == 2
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 3
    assert accept_observation(request, result)["accepted_backend_turns"] == 3


@pytest.mark.parametrize("kind", ["unauthorized", "usageLimitExceeded", "rateLimitExceeded",
                                  "contextWindowExceeded", "internalServerError"])
def test_other_failure_classes_do_not_authorize_capacity_continuation(kind):
    peer, client, request, clock = configured()
    peer.messages[3:] = [failed(kind=kind)]
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and not clock.waits
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1


def test_notification_without_exact_terminal_never_starts_another_turn():
    peer, client, request, clock = configured()
    peer.messages[3:] = [{"method": "error", "params": {"threadId": "thread",
        "turnId": "turn", "willRetry": False, "error": {"codexErrorInfo": "serverOverloaded"}}}]
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and not clock.waits
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1


def test_uncompleted_native_tool_blocks_continuation():
    peer, client, request, clock = configured()
    peer.messages[3:] = [{"method": "item/started", "params": {"threadId": "thread",
        "turnId": "turn", "item": {"type": "commandExecution", "id": "unfinished"}}}, failed()]
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and not clock.waits
    assert result.backend_recovery["state"] == "blocked"


def test_cancel_during_cooldown_preserves_original_turn_and_does_not_resubmit():
    peer, client, request, clock = configured()
    peer.messages[3:] = [failed()]

    def wait(seconds):
        clock.wait(seconds)
        clock.cancelled = True

    client.recovery_wait = wait
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.backend_turn == "turn"
    assert result.backend_recovery["state"] == "cancelled"
    assert result.backend_recovery["attempts"] == 0


def test_original_deadline_does_not_get_extended_for_cooldown():
    peer, client, request, clock = configured(deadline=100)
    peer.messages[3:] = [failed()]
    result = client.run_admitted(request)
    assert result.backend_recovery["state"] == "deadline" and not clock.waits


def test_uncertain_continuation_write_is_not_replayed():
    peer, client, request, clock = configured()
    peer.messages[3:] = [failed()]
    send = peer.send

    def uncertain(message, **kwargs):
        send(message, **kwargs)
        if message.get("id") == 2_000_001:
            raise OSError("PRIVATE")

    peer.send = uncertain
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and clock.now == 120
    assert result.backend_recovery["attempts"] == 1
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 2


def test_reused_turn_identity_cannot_be_accepted_as_new_completion():
    peer, client, request, _ = configured()
    peer.messages[3:] = [failed(), acknowledgement(1, "turn"), completed("turn")]
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.output_text == ""


def test_default_configuration_keeps_single_submission_behavior():
    peer, client, request = setup()
    peer.messages[3:] = [failed()]
    result = client.run_admitted(request)
    assert result.backend_recovery is None
    assert result.disposition == "unknown"
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1


def test_recovered_answer_is_sealable_with_the_final_recovery_projection():
    peer, client, request, _ = configured()
    peer.messages[3:] = [failed(), acknowledgement(1, "continued"), completed("continued")]
    sealed = []
    client.answer_seal = lambda observed: sealed.append(accept_observation(request, observed))
    result = client.run_admitted(request)
    assert result.disposition == "completed" and len(sealed) == 1
    assert sealed[0] == accept_observation(request, result)
    assert sealed[0]["backend_recovery"]["state"] == "recovered"


def test_unsettled_native_child_blocks_recovery_instead_of_replaying_parent():
    from test_codex_native_collaboration import child_turn, spawn_completed, spawn_started

    peer, client, request, clock = configured()
    peer.messages[3:] = [spawn_started(), spawn_completed(), child_turn(), failed()]
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.unresolved_effects
    assert result.backend_recovery["state"] == "blocked" and not clock.waits


def test_context_and_recovery_policies_roundtrip_through_native_owner_config():
    from test_foreground_config_native import native_config

    from ahc.cli.foreground_config_native import parse_foreground_config_native

    raw = native_config()
    raw["backend"]["context_policy"] = {"soft_limit_percent": 80, "headroom_tokens": 32768}
    raw["backend"]["overload_recovery"] = {"delays_seconds": [120, 300]}
    assert parse_foreground_config_native(raw).to_dict() == raw


def test_nonretryable_capacity_notification_cannot_be_overridden_by_unrelated_success():
    peer, client, request, clock = configured()
    peer.messages.insert(3, {"method": "error", "params": {"threadId": "thread",
        "turnId": "turn", "willRetry": False, "error": {"codexErrorInfo": "serverOverloaded"}}})
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.output_text == "" and not clock.waits
    assert result.backend_recovery["state"] == "blocked"
