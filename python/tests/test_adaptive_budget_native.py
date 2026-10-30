"""Real native kernel work spans expiry without restart or duplicate execution."""
import time
from test_workspace_jobs_native import native_work, call, native_owner
from ahc.application.adaptive_time_budget import AdaptiveTimeBudget
from ahc.domain.adaptive_time_budget import time_budget_policy
__all__ = ["native_work", "native_owner"]

def make_budget():
    calls = []
    def judge(evidence):
        calls.append(evidence)
        return {"continue": True, "reason": "active native work"}
    budget = AdaptiveTimeBudget(time.monotonic() + 0.4,
        time_budget_policy({"enabled": True}), judge, lambda: {"active": True})
    return budget, calls

def test_silent_native_cell_finishes_once_across_budget_expiry(native_work):
    _, workspace, _, _ = native_work
    budget, calls = make_budget()
    with workspace.turn_scope(budget):
        result = workspace.execute(workspace.identity,
            "import time; once=globals().get('once',0)+1; time.sleep(1.1); print(once)")
        assert result["ok"] and result["output"].strip() == "1"
        assert len(calls) == budget.status()["renewals"] == 1
        assert workspace.execute(workspace.identity, "print(once)")["output"].strip() == "1"

def test_same_job_ignores_estimate_as_kill_limit_in_adaptive_mode(native_work):
    _, workspace, tools, _ = native_work
    budget, calls = make_budget()
    with workspace.turn_scope(budget):
        code = "import time; once=globals().get('once',0)+1; time.sleep(1.1); print(once)"
        admitted = call(tools, "start", job_id="adaptive-once", code=code,
                        expected_seconds=0.2)
        assert admitted["disposition"] == "observed"
        result = call(tools, "wait", job_id="adaptive-once", wait_seconds=5)
        assert result["disposition"] == "observed", result
        assert result["result"]["status"] == "complete"
        assert result["result"]["execution"]["output"].strip() == "1"
        assert len(calls) == 1
        assert call(tools, "start", job_id="adaptive-once", code=code,
                    expected_seconds=0.2)["result"]["status"] == "complete"


def test_owner_wait_judges_at_expiry_before_short_decision_allowance(native_work):
    import threading
    _, workspace, tools, _ = native_work
    calls = []
    budget = AdaptiveTimeBudget(time.monotonic()+.4,
        time_budget_policy({"enabled": True, "judge_timeout_seconds": .2}),
        lambda evidence: calls.append(evidence) or {"continue": True, "reason": "active test"},
        lambda: {"active": True})
    budget.owner_thread = threading.get_ident()
    with workspace.turn_scope(budget):
        result = call(tools, "start", job_id="owner-bound", expected_seconds=.2,
            code="import time; once=globals().get('once',0)+1; time.sleep(1.1); print(once)")
        assert result["disposition"] == "observed"
        result = call(tools,"wait",job_id="owner-bound",wait_seconds=5)
        assert result["result"]["status"] == "complete", result
        assert result["result"]["execution"]["output"].strip() == "1"
        assert len(calls) == 1
