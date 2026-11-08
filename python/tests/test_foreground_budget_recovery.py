"""Unknown long jobs must leave a usable, distinct next foreground turn."""

import threading
import time
from dataclasses import replace

import pytest
from test_foreground_runtime import setup
from test_workspace_jobs import call, owned

from ahc.application.adaptive_time_budget import AdaptiveTimeBudget
from ahc.domain.adaptive_time_budget import time_budget_policy
from ahc.domain.agent_backend import AgentTurnObservation

__all__ = ["owned"]


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_unavailable_workspace_is_retired_before_another_admission(tmp_path, cleanup_fails):
    runtime, requests, invoked, request = setup(tmp_path)
    runtime.start()
    original = runtime.workspace
    generation = original.identity.generation
    closes = []
    close = original.port.close

    def close_port():
        closes.append("workspace")
        if cleanup_fails:
            raise OSError("cleanup unverified")
        close()

    original.port.close = close_port

    def factory(bound, tools, *, cancelled):
        invoked.append(bound.operation_id)

        class Backend:
            def invoke(self, admitted):
                if admitted.operation_id == request.operation_id:
                    # Recorded failure shape: the job's step was reserved, its
                    # result became unknown, and the live port remained retained.
                    runtime.store.begin_workspace_step(tools.workspace.identity)
                    tools.workspace.unavailable = True
                    return AgentTurnObservation(admitted, "unknown", "deadline", 1, 1,
                                                "thread", "turn", unresolved_effects=True)
                runtime.store.require_workspace(tools.workspace.identity)
                return AgentTurnObservation(admitted, "completed", "fixture", 1, 1,
                                            "following-thread", "following-turn", "ready")

            def close(self):
                closes.append("backend")

        return Backend()

    runtime.backend_factory = factory
    requests.extend([request, request, replace(request, operation_id="following")])
    try:
        first = runtime.step()
        assert first.result["disposition"] == "unknown"
        assert original.unavailable and not original.closed
        if cleanup_fails:
            assert runtime.step() is None
            assert runtime.workspace is original
            assert runtime.workspace_status()["status"] == "unavailable"
            assert invoked == [request.operation_id]
            assert closes == ["backend", "workspace"]
        else:
            assert runtime.step() == first  # The original operation is never replayed.
            assert original.closed
            assert runtime.workspace.identity.generation > generation
            assert closes[:2] == ["backend", "workspace"]
            assert runtime.step().result["accepted"]
            assert invoked == [request.operation_id, "following"]
        assert runtime.store.read_turn(request.session, request.operation_id) == first
    finally:
        original.port.close = close
        runtime.close()


@pytest.mark.parametrize("judge_error", [False, True])
def test_job_wait_returns_at_wrap_up_transition(owned, judge_error):
    _, workspace, tools = owned
    calls = []

    def judge(evidence):
        calls.append(evidence)
        if judge_error:
            raise OSError("judge unavailable")
        return {"continue": False, "reason": "handover is needed"}

    budget = AdaptiveTimeBudget(time.monotonic() + 0.04,
        time_budget_policy({"enabled": True}), judge, lambda: {"active": True})
    budget.owner_thread = threading.get_ident()
    workspace.port._turn_budget = budget
    admitted = call(tools, "start", code="count-once", expected_seconds=1, wait_seconds=0)
    assert admitted["result"]["status"] == "running"
    assert workspace.port.entered.wait(1)
    started = time.monotonic()
    result = call(tools, "wait", wait_seconds=1)
    elapsed = time.monotonic() - started
    assert budget.status()["state"] == "finalizing"
    assert result["result"]["status"] == "running"
    assert elapsed < 0.5, "job wait consumed the model's graceful wrap-up window"
    assert len(calls) == 1 and workspace.port.calls == 1
    assert budget.peek_deadline() - time.monotonic() > 170
