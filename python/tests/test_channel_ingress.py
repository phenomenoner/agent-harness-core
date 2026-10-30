from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from skill_test_support import goal

from ahc.application.channel_ingress import ChannelIngress
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute, channel_operation_id
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.working_sets import WorkingSetSnapshot
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository, WorkingSetConflict


def _state(tmp_path: Path):
    session = VirtualSessionIdentity.initial("session", "agent", "route")
    repository = RuntimeRepository.initialize(tmp_path / "runtime")
    snapshot = repository.create(WorkingSetSnapshot.initial(session, goal()))
    route = ChannelRoute("telegram", "account", "channel", "user", session)
    return repository, snapshot, route, ChannelIngress(RuntimeInbox(repository), route)


def _changed_route(route: ChannelRoute, field: str) -> ChannelRoute:
    if field == "platform":
        return replace(route, platform="discord")
    if field == "account_id":
        return replace(route, account_id="other-account")
    if field == "channel_id":
        return replace(route, channel_id="other-channel")
    if field == "user_id":
        return replace(route, user_id="other-user")
    if field == "virtual_session_id":
        ref = replace(route.session.ref, virtual_session_id="other-session")
        return replace(route, session=replace(route.session, ref=ref))
    if field == "generation":
        return replace(route, session=route.session.successor())
    if field == "agent_id":
        return replace(route, session=replace(route.session, agent_id="other-agent"))
    if field == "route_id":
        return replace(route, session=replace(route.session, route_id="other-route"))
    raise AssertionError(field)


def _submission_count(repository: RuntimeRepository) -> int | None:
    with repository._read() as connection:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_submissions'"
        ).fetchone()
        if exists is None:
            return None
        return connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0]


def _assert_no_turn_or_intent(repository: RuntimeRepository) -> None:
    with repository._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_turns").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM runtime_intents").fetchone()[0] == 0


@pytest.mark.parametrize(
    "field",
    (
        "platform",
        "account_id",
        "channel_id",
        "user_id",
        "virtual_session_id",
        "generation",
        "agent_id",
        "route_id",
    ),
)
def test_mismatched_route_rejects_before_inbox_effect(tmp_path: Path, field: str) -> None:
    repository, snapshot, trusted_route, ingress = _state(tmp_path)
    observed_route = _changed_route(trusted_route, field)
    event = ChannelInboundEvent(observed_route, "event-1", "hello")

    with pytest.raises(ContractError, match="route"):
        ingress.submit(event, snapshot.goal.digest)

    assert _submission_count(repository) is None
    _assert_no_turn_or_intent(repository)


def test_current_generation_admits_and_operation_id_excludes_generation(tmp_path: Path) -> None:
    repository, snapshot, trusted_route, ingress = _state(tmp_path)
    event = ChannelInboundEvent(trusted_route, "event-1", "hello")

    receipt = ingress.submit(event, snapshot.goal.digest)
    assert receipt["operation_id"] == event.operation_id
    successor_route = replace(trusted_route, session=trusted_route.session.successor())
    assert channel_operation_id(successor_route, event.event_id) == event.operation_id
    assert _submission_count(repository) == 1
    _assert_no_turn_or_intent(repository)


def test_stale_trusted_generation_is_rejected_by_current_inbox_owner(tmp_path: Path) -> None:
    repository, snapshot, trusted_route, _ = _state(tmp_path)
    stale_route = replace(trusted_route, session=trusted_route.session.successor())
    ingress = ChannelIngress(RuntimeInbox(repository), stale_route)
    event = ChannelInboundEvent(stale_route, "event-1", "hello")

    with pytest.raises(WorkingSetConflict, match="current and active"):
        ingress.submit(event, snapshot.goal.digest)

    assert _submission_count(repository) in (None, 0)
    _assert_no_turn_or_intent(repository)


def test_duplicate_event_is_idempotent_but_body_and_goal_conflicts_reject(tmp_path: Path) -> None:
    repository, snapshot, trusted_route, ingress = _state(tmp_path)
    event = ChannelInboundEvent(trusted_route, "event-1", "hello")
    first = ingress.submit(event, snapshot.goal.digest)

    assert ingress.submit(event, snapshot.goal.digest) == first
    with pytest.raises(ContractError, match="already bound"):
        ingress.submit(replace(event, input_text="changed"), snapshot.goal.digest)
    with pytest.raises(ContractError, match="already bound"):
        ingress.submit(event, "f" * 64)

    assert _submission_count(repository) == 1
    _assert_no_turn_or_intent(repository)


def test_wrong_goal_rejects_new_event_without_turn_or_provider_effect(tmp_path: Path) -> None:
    repository, snapshot, trusted_route, ingress = _state(tmp_path)
    event = ChannelInboundEvent(trusted_route, "event-2", "hello")

    with pytest.raises(WorkingSetConflict, match="current and active"):
        ingress.submit(event, "f" * 64)

    assert _submission_count(repository) in (None, 0)
    _assert_no_turn_or_intent(repository)


def test_route_values_are_immutable_and_operation_identity_uses_only_event_binding() -> None:
    session = VirtualSessionIdentity.initial("session", "agent", "route")
    route = ChannelRoute("telegram", "account", "channel", "user", session)
    event = ChannelInboundEvent(route, "event-1", "first body")
    changed_body = replace(event, input_text="second body")
    changed_session = replace(route, session=session.successor())

    assert event.operation_id == channel_operation_id(route, event.event_id)
    assert changed_body.operation_id == event.operation_id
    assert channel_operation_id(changed_session, event.event_id) == event.operation_id
    assert ChannelRoute.from_dict(route.to_dict()) == route
    assert ChannelInboundEvent.from_dict(event.to_dict()) == event
