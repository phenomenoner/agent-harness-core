"""Child usage snapshots retain unknown truth and exact claim ownership."""

from dataclasses import replace

import pytest
from test_child_dispatch import state

from ahc.adapters.child_agent_backend import ChildAgentBackend
from ahc.application.child_dispatch import ChildDispatchService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.token_observation import TokenCounts, TokenObservation
from ahc.infrastructure.child_token_observations import record
from ahc.infrastructure.child_work_repository import ChildWorkRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetConflict, WorkingSetCorrupt


def observation(parent, envelope, unknown=False):
    request = replace(parent, operation_id=envelope.child_id, input_text=envelope.task_text,
                      deadline=0.05, capabilities=(), backend_thread=None)
    counts = TokenCounts(3, 2, 1, 0, 5)
    return AgentTurnObservation(request, "unknown" if unknown else "completed", "fixture",
        1, 1, "thread", "turn", "done", unresolved_effects=unknown,
        token_observation=TokenObservation("thread", "turn", counts, counts))


@pytest.mark.parametrize("unknown", [False, True])
@pytest.mark.parametrize("write_loss", [False, True])
def test_backend_snapshot_persists_before_result_and_reopen_never_replays(
        tmp_path, unknown, write_loss):
    repo, parent, clock, children, envelope = state(tmp_path)
    observed = observation(parent, envelope, unknown)
    calls = []

    class Backend:
        def invoke(self, request):
            calls.append(request)
            return observed

        def close(self):
            pass

    def save(value):
        record(children, envelope, "claim", value)
        if write_loss:
            raise OSError("local reply lost after snapshot commit")

    service = ChildDispatchService(children, lambda item: ChildAgentBackend(
        envelope, observed.request, lambda *a, **k: Backend(), parent_request=parent,
        monotonic=lambda: 0, wall_ms=lambda: clock[0],
        observe=save))
    result = service.dispatch(envelope, "claim")
    assert result["state"] == ("unknown" if unknown or write_loss else "terminal")
    assert result["token_observation"]["total"]["total_tokens"] == 5
    fresh = ChildWorkRepository(RuntimeRepository(repo.root)).read(children.parent)[0]
    assert fresh == result
    assert service.dispatch(envelope, "again") == result and len(calls) == 1
    service.close()


def test_claim_and_snapshot_conflicts_preserve_first_record(tmp_path):
    repo, parent, _, children, envelope = state(tmp_path)
    children.admit(envelope)
    assert "token_observation" not in children.read(children.parent)[0]
    with repo._read() as connection:
        assert connection.execute("SELECT 1 FROM sqlite_master WHERE "
            "name='runtime_child_token_observations'").fetchone() is None
    children.claim(children.parent, envelope.child_id, "claim")
    observed = observation(parent, envelope)
    with pytest.raises(WorkingSetConflict):
        record(children, envelope, "wrong", observed)
    record(children, envelope, "claim", observed)
    record(children, envelope, "claim", observed)
    changed = replace(observed, token_observation=TokenObservation(
        "thread", "turn", TokenCounts(4, 2, 1, 0, 6), observed.token_observation.total))
    with pytest.raises(WorkingSetConflict):
        record(children, envelope, "claim", changed)
    assert children.read(children.parent)[0]["token_observation"]["last"]["total_tokens"] == 5
    children.cancel(children.parent, envelope.child_id)
    with pytest.raises(WorkingSetConflict):
        record(children, envelope, "claim", observed)
    repo._mutate(lambda c: c.execute("UPDATE runtime_child_token_observations "
                                    "SET envelope_digest=?", ("f" * 64,)))
    with pytest.raises(WorkingSetCorrupt):
        children.read(children.parent)
