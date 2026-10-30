"""Exact-bound advisory counts persist without leaking backend identifiers."""

from dataclasses import replace

import pytest
from test_runtime_core import Backend, setup

from ahc.application.runtime_service import RuntimeService
from ahc.domain.token_observation import TokenCounts, TokenObservation
from ahc.infrastructure.runtime_repository import RuntimeRepository


@pytest.mark.parametrize("unknown", [False, True])
def test_snapshot_survives_fresh_read_without_replay_or_complete_usage(tmp_path, unknown):
    repo, snapshot, request = setup(tmp_path)

    class Observed(Backend):
        def invoke(self, request):
            result = super().invoke(request)
            counts = TokenCounts(3, 2, 1, 0, 5)
            return replace(result,
                token_observation=TokenObservation("thread", "turn", counts, counts),
                disposition="unknown" if unknown else "completed", unresolved_effects=unknown)

    backend = Observed()
    turn = RuntimeService(repo, backend).execute(snapshot, request)
    assert turn.result["accepted"] is not unknown
    assert turn.result["vendor_usage"] is None
    assert turn.result["token_observation"]["last"]["total_tokens"] == 5
    assert turn.result["token_observation"]["complete"] is False
    assert "backend_thread" not in turn.result["token_observation"]
    reopened = RuntimeRepository(repo.root)
    again = RuntimeService(reopened, backend).execute(snapshot, request)
    assert again.result == turn.result and backend.calls == 1


def test_foreign_snapshot_is_not_accepted_or_saved(tmp_path):
    repo, snapshot, request = setup(tmp_path)

    class Foreign(Backend):
        def invoke(self, request):
            counts = TokenCounts(1, 1, 0, 0, 2)
            return replace(super().invoke(request),
                token_observation=TokenObservation("foreign", "turn", counts, counts))

    result = RuntimeService(repo, Foreign()).execute(snapshot, request).result
    assert result["disposition"] == "unknown" and not result["accepted"]
    assert "token_observation" not in result
