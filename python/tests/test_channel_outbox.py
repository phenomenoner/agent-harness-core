"""Durable local channel-final delivery with a controlled in-process transport."""

from dataclasses import replace

import pytest
from test_runtime_core import setup as runtime_setup

from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.runtime_turn import CLEANUP_NOTICE
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_channel_store import _output, enqueue_final
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import (
    WorkingRepositoryError,
    WorkingSetConflict,
    WorkingSetCorrupt,
)


def state(tmp_path):
    repo, snapshot, template = runtime_setup(tmp_path)
    route = ChannelRoute("telegram", "account", "channel", "user", snapshot.session)
    event = ChannelInboundEvent(route, "event", "hello")
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    request = replace(
        template,
        operation_id=event.operation_id,
        goal_digest=snapshot.goal.digest,
        input_text=event.input_text,
    )
    return repo, snapshot, request, route


def accepted(repo, snapshot, request):
    class Backend:
        def invoke(self, value):
            return AgentTurnObservation(
                value, "completed", "controlled", 1, 1, output_text="answer"
            )

    from ahc.application.runtime_service import RuntimeService

    return RuntimeService(repo, Backend()).execute(snapshot, request)


def test_accepted_parent_result_delivers_once_and_survives_reopen(tmp_path):
    repo, snapshot, request, route = state(tmp_path)
    accepted(repo, snapshot, request)
    outbox = ChannelOutbox(repo)
    assert outbox.read(route)[0]["state"] == "pending"

    class Transport:
        calls = []

        def send(self, received_route, payload, logical_delivery_id):
            self.calls.append((received_route, payload, logical_delivery_id))
            return "controlled-receipt"

    transport = Transport()
    delivered = ChannelDelivery(outbox, route, transport).deliver(request.operation_id, "token")
    assert delivered["state"] == "sent"
    assert transport.calls == [(route, {"output_text": "💬 **回覆**\n\nanswer"}, request.operation_id)]
    reopened = ChannelOutbox(RuntimeRepository(repo.root))
    assert reopened.read(route)[0]["receipt"] == "controlled-receipt"
    with pytest.raises(ContractError):
        ChannelDelivery(reopened, route, transport).deliver(request.operation_id, "token")
    assert len(transport.calls) == 1


def test_cleanup_pending_answer_requires_matching_seal_and_adds_notice(tmp_path):
    repo, snapshot, request, route = state(tmp_path)

    class Backend:
        def invoke(self, value):
            return AgentTurnObservation(
                value, "completed", "controlled", 1, 1, output_text="answer", cleanup_pending=True
            )

    terminal = RuntimeService(repo, Backend()).execute(snapshot, request)
    assert terminal.result["cleanup_pending"] and repo.read_answer_seal(
        request.session, request.operation_id
    )["output_text"] == "answer"
    outbox = ChannelOutbox(repo)
    assert outbox.read(route)[0]["payload"] == {"output_text": "answer" + CLEANUP_NOTICE}
    repo._mutate(lambda connection: connection.execute(
        "UPDATE runtime_answer_seals SET result_json=? WHERE operation_id=?",
        (canonical_json({"different": True}).decode(), request.operation_id),
    ))
    with pytest.raises(WorkingSetCorrupt):
        outbox.read(route)


def test_cleanup_pending_delivery_keeps_a_4096_character_answer(tmp_path):
    repo, snapshot, request, route = state(tmp_path)

    class Backend:
        def invoke(self, value):
            return AgentTurnObservation(
                value, "completed", "controlled", 1, 1, output_text="x" * 4096,
                cleanup_pending=True,
            )

    assert RuntimeService(repo, Backend()).execute(snapshot, request).result["cleanup_pending"]
    assert ChannelOutbox(repo).read(route)[0]["payload"] == {
        "output_text": "x" * 4096 + CLEANUP_NOTICE}


def test_unknown_result_schema_is_not_decoded_as_legacy_channel_output():
    with pytest.raises(WorkingSetCorrupt):
        _output({"accepted": True, "output_text": "answer", "result_schema_version": "v3"})


def test_seal_survives_crash_before_finish_without_backend_replay(tmp_path):
    repo, snapshot, request, _ = state(tmp_path)
    repo.begin(snapshot, request)
    sealed = {
        "result_schema_version": "ahc.runtime-result.v2",
        "disposition": "completed",
        "output_text": "answer",
        "admissions": 1,
        "accepted_backend_turns": 1,
        "vendor_usage": None,
        "unresolved_effects": False,
        "accepted": True,
        "backend_completion": {"thread": "thread", "turn": "turn"},
    }
    repo.seal_observation(request, sealed)
    reopened = RuntimeRepository(repo.root)
    calls = []

    class Backend:
        def invoke(self, _value):
            calls.append(True)
            pytest.fail("sealed submitted turn must not replay")

    turn = RuntimeService(reopened, Backend()).execute(snapshot, request)
    assert turn.status == "submitted" and calls == []
    assert reopened.read_answer_seal(request.session, request.operation_id) == sealed


def test_delivery_scope_blocks_reconcile_during_controlled_send(tmp_path):
    repo, snapshot, request, route = state(tmp_path)
    accepted(repo, snapshot, request)

    class Transport:
        def send(self, *_):
            from ahc.infrastructure.working_repository import WorkingWriterBusy

            with pytest.raises(WorkingWriterBusy):
                ChannelOutbox(RuntimeRepository(repo.root)).reconcile(route)
            return "receipt"

    assert (
        ChannelDelivery(ChannelOutbox(repo), route, Transport()).deliver(
            request.operation_id, "token"
        )["state"]
        == "sent"
    )


def test_stale_route_generation_cannot_claim_before_transport(tmp_path):
    repo, snapshot, request, route = state(tmp_path)
    accepted(repo, snapshot, request)
    from ahc.application.working_continuity import WorkingContinuityService

    WorkingContinuityService(repo).continue_generation(
        repo.read_current("session"), route.session.successor(), "compaction"
    )
    calls = []

    class Transport:
        def send(self, *_):
            calls.append(True)
            return "receipt"

    with pytest.raises(WorkingSetConflict):
        ChannelDelivery(ChannelOutbox(repo), route, Transport()).deliver(
            request.operation_id, "token"
        )
    assert calls == []


@pytest.mark.parametrize(
    "route",
    (
        lambda value: replace(value, platform="discord"),
        lambda value: replace(value, account_id="other"),
        lambda value: replace(value, channel_id="other"),
        lambda value: replace(value, user_id="other"),
        lambda value: replace(
            value,
            session=replace(
                value.session, ref=replace(value.session.ref, virtual_session_id="other")
            ),
        ),
        lambda value: replace(value, session=replace(value.session, agent_id="other")),
        lambda value: replace(value, session=replace(value.session, route_id="other")),
    ),
)
def test_wrong_route_axis_cannot_claim_or_send(tmp_path, route):
    repo, snapshot, request, trusted = state(tmp_path)
    accepted(repo, snapshot, request)
    calls = []

    class Transport:
        def send(self, *_):
            calls.append(True)
            return "receipt"

    with pytest.raises((ContractError, WorkingSetConflict)):
        ChannelDelivery(ChannelOutbox(repo), route(trusted), Transport()).deliver(
            request.operation_id, "token"
        )
    assert calls == []


def test_parent_precommit_failure_rolls_back_pending_outbox(tmp_path):
    repo, snapshot, request, route = state(tmp_path)
    repo.begin(snapshot, request)
    with pytest.raises(WorkingRepositoryError, match="injected"):
        repo.finish(request, {"accepted": True, "output_text": "answer"}, fail_before_commit=True)
    assert ChannelOutbox(repo).read(route) == ()
    assert repo.read_turn(request.session, request.operation_id).status == "submitted"


def test_transport_exception_and_reconcile_mark_unknown_without_retry(tmp_path):
    repo, snapshot, request, route = state(tmp_path)
    accepted(repo, snapshot, request)
    calls = []

    class Fails:
        def send(self, *_):
            calls.append(True)
            raise RuntimeError("private provider failure")

    outbox = ChannelOutbox(repo)
    unknown = ChannelDelivery(outbox, route, Fails()).deliver(request.operation_id, "token")
    assert unknown["state"] == "unknown" and unknown["receipt"] == "transport-exception"
    with pytest.raises(ContractError):
        ChannelDelivery(outbox, route, Fails()).deliver(request.operation_id, "other")
    assert calls == [True]

    repo_invalid, snapshot_invalid, request_invalid, route_invalid = state(tmp_path / "invalid")
    accepted(repo_invalid, snapshot_invalid, request_invalid)

    class Invalid:
        def send(self, *_):
            return "\x01"

    invalid = ChannelDelivery(ChannelOutbox(repo_invalid), route_invalid, Invalid()).deliver(
        request_invalid.operation_id, "token"
    )
    assert invalid["state"] == "unknown" and invalid["receipt"] == "transport-exception"

    repo2, snapshot2, request2, route2 = state(tmp_path / "reconcile")
    accepted(repo2, snapshot2, request2)
    ChannelOutbox(repo2).claim(route2, request2.operation_id, "claim")
    assert ChannelOutbox(RuntimeRepository(repo2.root)).reconcile(route2)[0]["state"] == "unknown"


def test_nonaccepted_terminal_creates_no_outbox(tmp_path):
    repo, snapshot, request, route = state(tmp_path)
    repo.begin(snapshot, request)
    repo.finish(request, {"accepted": False, "output_text": "", "reason": "unknown"})
    assert ChannelOutbox(repo).read(route) == ()


def test_bound_route_without_outbox_table_aborts_parent_terminal(tmp_path):
    repo, snapshot, request, _ = state(tmp_path)
    repo._mutate(lambda connection: connection.execute("DROP TABLE runtime_channel_outbox"))
    repo.begin(snapshot, request)
    with pytest.raises(WorkingSetCorrupt):
        repo.finish(request, {"accepted": True, "output_text": "answer"})
    assert repo.read_turn(request.session, request.operation_id).status == "submitted"


def test_old_store_without_channel_tables_is_terminal_safe(tmp_path):
    import sqlite3

    connection = sqlite3.connect(":memory:")
    try:
        enqueue_final(connection, {"operation_id": "operation"}, {"accepted": True})
    finally:
        connection.close()
