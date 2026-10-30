"""Route authority is durable at admission, never inferred after execution."""

import pytest
from test_channel_ingress import _state

from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.working_repository import WorkingRepositoryError


def test_existing_local_submission_cannot_acquire_a_channel_destination(tmp_path):
    repo, snapshot, route, ingress = _state(tmp_path)
    event = ChannelInboundEvent(route, "event", "hello")
    RuntimeInbox(repo).submit(route.session, event.operation_id, snapshot.goal.digest, "hello")
    with pytest.raises(ContractError):
        ingress.submit(event, snapshot.goal.digest)
    with repo._read() as connection:
        assert connection.execute("SELECT count(*) FROM runtime_channel_routes").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 1


def test_route_insert_failure_rolls_back_the_inbound_submission(tmp_path):
    repo, snapshot, route, ingress = _state(tmp_path)
    repo._mutate(
        lambda connection: connection.execute(
            "CREATE TRIGGER reject_route BEFORE INSERT ON runtime_channel_routes "
            "BEGIN SELECT RAISE(ABORT, 'injected route failure'); END"
        )
    )
    with pytest.raises(WorkingRepositoryError):
        ingress.submit(ChannelInboundEvent(route, "event", "hello"), snapshot.goal.digest)
    with repo._read() as connection:
        exists = RuntimeInbox._exists(connection)
        if exists:
            assert connection.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM runtime_channel_routes").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM runtime_channel_outbox").fetchone()[0] == 0
