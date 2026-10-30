from dataclasses import replace

import pytest
from test_runtime_core import setup as runtime_setup

from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_conversation_context import conversation_context
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetCorrupt


class _Transport:
    def send(self, *_):
        return "fixture-receipt"


def _submit_channel(repo, template, route, event_id, text, answer, *, disposition="completed"):
    snapshot = repo.read_current("session")
    event = ChannelInboundEvent(route, event_id, text)
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    request = replace(
        template,
        session=snapshot.session,
        operation_id=event.operation_id,
        goal_digest=snapshot.goal.digest,
        input_text=text,
    )

    class Backend:
        def invoke(self, value):
            return AgentTurnObservation(
                value,
                disposition,
                "fixture",
                1,
                1 if disposition == "completed" else 0,
                output_text=answer,
                unresolved_effects=disposition != "completed",
            )

    RuntimeService(repo, Backend()).execute(snapshot, request)
    return request


def _submit_current(repo, route, event_id="current", text="current"):
    snapshot = repo.read_current("session")
    event = ChannelInboundEvent(route, event_id, text)
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    return event.operation_id


def _deliver(repo, route, operation_id):
    return ChannelDelivery(ChannelOutbox(repo), route, _Transport()).deliver(operation_id, "token")


def test_context_returns_confirmed_same_route_pairs_in_order_and_survives_reopen(tmp_path):
    repo, snapshot, template = runtime_setup(tmp_path)
    route = ChannelRoute("telegram", "account", "channel", "user", snapshot.session)
    first = _submit_channel(repo, template, route, "first", "first user", "first answer")
    _deliver(repo, route, first.operation_id)
    second = _submit_channel(repo, template, route, "second", "新🧭", "答覆")
    _deliver(repo, route, second.operation_id)
    current = _submit_current(repo, route)

    reopened = RuntimeRepository(repo.root)
    assert conversation_context(reopened, route.session, current) == (
        {"user": "first user", "assistant": "first answer"},
        {"user": "新🧭", "assistant": "答覆"},
    )
    assert conversation_context(reopened, route.session, current, max_turns=1) == (
        {"user": "新🧭", "assistant": "答覆"},
    )
    newest_size = len("新🧭") + len("答覆")
    assert conversation_context(reopened, route.session, current, max_chars=newest_size - 1) == ()
    assert conversation_context(reopened, route.session, current, max_chars=newest_size) == (
        {"user": "新🧭", "assistant": "答覆"},
    )


@pytest.mark.parametrize(
    "replacement",
    (
        lambda route: replace(route, platform="discord"),
        lambda route: replace(route, account_id="other-account"),
        lambda route: replace(route, channel_id="other-channel"),
        lambda route: replace(route, user_id="other-user"),
    ),
)
def test_context_excludes_other_channel_route_axes(tmp_path, replacement):
    repo, snapshot, template = runtime_setup(tmp_path)
    route = ChannelRoute("telegram", "account", "channel", "user", snapshot.session)
    other = replacement(route)
    prior = _submit_channel(repo, template, other, "other", "other user", "other answer")
    _deliver(repo, other, prior.operation_id)
    current = _submit_current(repo, route)

    assert conversation_context(repo, route.session, current) == ()


def test_context_excludes_prior_session_generation_and_channel_history_from_local_current(tmp_path):
    repo, snapshot, template = runtime_setup(tmp_path)
    old_route = ChannelRoute("telegram", "account", "channel", "user", snapshot.session)
    old = _submit_channel(repo, template, old_route, "old", "old user", "old answer")
    _deliver(repo, old_route, old.operation_id)
    current = repo.read_current("session")
    WorkingContinuityService(repo).continue_generation(
        current, current.session.successor(), "compaction"
    )
    successor = repo.read_current("session")
    route = replace(old_route, session=successor.session)
    fresh = _submit_channel(repo, template, route, "fresh", "fresh user", "fresh answer")
    _deliver(repo, route, fresh.operation_id)
    channel_current = _submit_current(repo, route)
    assert conversation_context(repo, route.session, channel_current) == (
        {"user": "fresh user", "assistant": "fresh answer"},
    )

    local = "local-previous"
    RuntimeInbox(repo).submit(route.session, local, successor.goal.digest, "local user")
    local_request = replace(
        template,
        session=route.session,
        operation_id=local,
        goal_digest=successor.goal.digest,
        input_text="local user",
    )

    class Backend:
        def invoke(self, value):
            return AgentTurnObservation(
                value, "completed", "fixture", 1, 1, output_text="local answer"
            )

    RuntimeService(repo, Backend()).execute(
        repo.read_current("session"), local_request
    )
    repo._mutate(
        lambda connection: connection.execute(
            "DELETE FROM runtime_channel_routes WHERE operation_id=?",
            (fresh.operation_id,),
        )
    )
    RuntimeInbox(repo).submit(
        route.session, "local-current", successor.goal.digest, "local current"
    )
    assert conversation_context(repo, route.session, "local-current") == (
        {"user": "local user", "assistant": "local answer"},
    )


def test_context_excludes_unknown_and_unconfirmed_channel_results(tmp_path):
    repo, snapshot, template = runtime_setup(tmp_path)
    route = ChannelRoute("telegram", "account", "channel", "user", snapshot.session)
    confirmed = _submit_channel(
        repo, template, route, "confirmed", "confirmed user", "confirmed answer"
    )
    _deliver(repo, route, confirmed.operation_id)
    _submit_channel(repo, template, route, "unknown", "unknown user", "", disposition="unknown")
    _submit_channel(repo, template, route, "pending", "pending user", "pending answer")
    current = _submit_current(repo, route)

    assert conversation_context(repo, route.session, current) == (
        {"user": "confirmed user", "assistant": "confirmed answer"},
    )


def test_context_rejects_invalid_bounds_and_deleted_current_channel_route(tmp_path):
    repo, snapshot, template = runtime_setup(tmp_path)
    route = ChannelRoute("telegram", "account", "channel", "user", snapshot.session)
    prior = _submit_channel(repo, template, route, "prior", "prior user", "prior answer")
    _deliver(repo, route, prior.operation_id)
    current = _submit_current(repo, route)

    with pytest.raises(ContractError):
        conversation_context(repo, route.session, current, max_turns=0)
    with pytest.raises(ContractError):
        conversation_context(repo, route.session, current, max_chars=True)
    repo._mutate(
        lambda connection: connection.execute(
            "DELETE FROM runtime_channel_routes WHERE operation_id=?",
            (current,),
        )
    )
    with pytest.raises(WorkingSetCorrupt, match="conversation|channel route"):
        conversation_context(repo, route.session, current)
