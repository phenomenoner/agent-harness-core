"""Late diagnostic metadata must not invalidate an exact, already sealed answer."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_channel_outbox import state
from test_codex_client import setup
from test_runtime_core import Backend

from ahc.application.runtime_service import RuntimeService
from ahc.domain.runtime_turn import accept_observation
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("late", ["advisory", "cleanup"])
def test_client_seal_final_commit_outbox_and_next_turn_keep_late_diagnostics(tmp_path, late):
    repo, snapshot, request, route = state(tmp_path)
    peer, client, _ = setup()
    request = replace(request, artifact_digest=client.executable_sha256,
                      configuration_digest=client.configuration_digest)
    if late == "advisory":
        peer.messages.insert(3, {"method": "thread/status/changed", "params": {
            "threadId": "thread", "status": {"type": "future-status"}}})
    else:
        peer.fail_close = True
    client.answer_seal = lambda observed: repo.seal_observation(
        request, accept_observation(request, observed))
    turn = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
        snapshot, request)
    assert turn.result["accepted"] is True
    diagnostic = turn.result["backend_diagnostic"]
    if late == "advisory":
        assert diagnostic["code"] == "advisory-status-ignored"
    else:
        assert diagnostic["category"] == "cleanup" and diagnostic["cleanup"] == "unverified"
    reopened = RuntimeRepository(repo.root)
    assert reopened.read_turn(request.session, request.operation_id) == turn
    seal = reopened.read_answer_seal(request.session, request.operation_id)
    assert seal["output_text"] == "answer" and "backend_diagnostic" not in seal
    assert ChannelOutbox(reopened).read(route)[0]["payload"]["output_text"].startswith("answer")
    next_turn = RuntimeService(reopened, Backend()).execute(
        reopened.read_current("session"), replace(request, operation_id="next"))
    assert next_turn.result["accepted"] is True


def test_legacy_seal_diagnostic_is_compared_without_rewriting_saved_bytes(tmp_path):
    repo, snapshot, request, route = state(tmp_path)
    repo.begin(snapshot, request)
    result = accept_observation(request, Backend().invoke(request))
    repo.seal_observation(request, result)
    legacy = {**result, "backend_diagnostic": {
        "category": "protocol", "phase": "decode", "last_method": "thread/status/changed",
        "code": "advisory-status-ignored", "remaining_ms": 10, "frames": 4,
        "cleanup": "not-observed"}}
    encoded = canonical_json(legacy).decode()
    repo._mutate(lambda c: c.execute(
        "UPDATE runtime_answer_seals SET result_json=? WHERE operation_id=?",
        (encoded, request.operation_id)))
    repo.seal_observation(request, result)
    assert repo.finish(request, result).result["accepted"] is True
    assert ChannelOutbox(repo).read(route)[0]["payload"]["output_text"] == "answer"
    with repo._read() as c:
        assert c.execute("SELECT result_json FROM runtime_answer_seals").fetchone()[0] == encoded
    assert repo.read_answer_seal(request.session, request.operation_id) == json.loads(encoded)


@pytest.mark.parametrize("change", [
    {"output_text": "different"},
    {"backend_completion": {"thread": "different", "turn": "turn"}},
    {"accepted_backend_turns": 2},
    {"reply_to_message_id": "123"},
])
def test_ignoring_diagnostics_never_allows_changed_answer_evidence(tmp_path, change):
    repo, snapshot, request, _ = state(tmp_path)
    repo.begin(snapshot, request)
    result = accept_observation(request, Backend().invoke(request))
    repo.seal_observation(request, result)
    with pytest.raises(ContractError):
        repo.seal_observation(request, {**result, **change})
    with pytest.raises(ContractError):
        repo.finish(request, {**result, **change})
    assert repo.read_answer_seal(request.session, request.operation_id) == result
