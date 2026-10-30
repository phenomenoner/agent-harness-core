"""Expiry decisions preserve one owner and never impose a renewal count quota."""
import threading
from concurrent.futures import ThreadPoolExecutor
import pytest
from ahc.application.adaptive_time_budget import AdaptiveTimeBudget
from ahc.domain.adaptive_time_budget import time_budget_policy
from ahc.domain.sessions import ContractError

def make_budget(now, judge, snapshot=lambda: {"active": True}, **overrides):
    policy = time_budget_policy({"enabled": True})
    policy.update(overrides.pop("policy", {}))
    return AdaptiveTimeBudget(10, policy, judge, snapshot, clock=lambda: now[0], **overrides)

def test_no_judgment_before_expiry():
    now, calls = [9], []
    budget = make_budget(now, lambda value: calls.append(value))
    for _ in range(10):
        assert budget.current_deadline() == 10
    assert calls == []

def test_positive_uses_decision_completion_and_next_snapshot():
    now, calls = [10], []
    def judge(value):
        calls.append(value)
        now[0] += 4
        return {"continue": True, "reason": "completed tools and an active test"}
    budget = make_budget(now, judge)
    assert budget.current_deadline() == 1814
    assert calls == [{"current": {"active": True}, "previous": None, "renewals": 0}]
    now[0] = 1814
    assert budget.current_deadline() == 3618
    assert calls[1]["previous"] == {"active": True}

def test_no_fixed_renewal_count():
    now, calls = [9], []
    def judge(value):
        calls.append(value)
        return {"continue": True, "reason": "new observed progress"}
    budget = make_budget(now, judge)
    for _ in range(6):
        now[0] = budget.current_deadline()
        budget.current_deadline()
    assert len(calls) == budget.status()["renewals"] == 6

def test_negative_finalizes_once():
    now, calls = [10], []
    def judge(value):
        calls.append(value)
        return {"continue": False, "reason": "no progress"}
    budget = make_budget(now, judge)
    assert budget.current_deadline() == 190
    now[0] = 190
    assert budget.current_deadline() == 190
    assert len(calls) == 1
    assert budget.status()["state"] == "finalizing"

@pytest.mark.parametrize("reply", [{"continue": "yes", "reason": "invalid"}, {},
                                   {"continue": True, "reason": ""}])
def test_invalid_decision_finalizes(reply):
    budget = make_budget([10], lambda _: reply)
    assert budget.current_deadline() == 190
    assert budget.status()["renewals"] == 0

def test_error_fallback_is_configurable():
    def fail(_):
        raise RuntimeError("unavailable")
    assert make_budget([10], fail).current_deadline() == 190
    budget = make_budget([10], fail, policy={"on_judge_error": "extend"})
    assert budget.current_deadline() == 1810
    assert budget.status()["renewals"] == 1

def test_inactive_does_not_launch_judge():
    calls = []
    budget = make_budget([10], lambda p: calls.append(p), lambda: {"active": False})
    assert budget.current_deadline() == 10
    assert budget.status()["state"] == "expired"
    assert calls == []

def test_cancelled_before_judgment():
    calls = []
    budget = make_budget([10], lambda p: calls.append(p), cancelled=lambda: True)
    assert budget.current_deadline() <= 10
    assert budget.status()["state"] == "cancelled"
    assert calls == []

@pytest.mark.parametrize("cancel,late", [(True, False), (False, True), (False, False)])
def test_one_pending_decision_cannot_extend_after_stop_or_judge_timeout(cancel, late):
    now, stopped, calls = [10], [False], []
    entered, release = threading.Event(), threading.Event()
    def judge(value):
        calls.append(value)
        entered.set()
        assert release.wait(2)
        return {"continue": True, "reason": "active progress"}
    budget = make_budget(now, judge, cancelled=lambda: stopped[0])
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(budget.current_deadline)
        assert entered.wait(2)
        assert budget.current_deadline() == 130
        assert len(calls) == 1
        stopped[0] = cancel
        if late:
            now[0] = 131
        release.set()
        future.result(timeout=2)
    assert budget.status()["renewals"] == (0 if cancel or late else 1)
    assert budget.status()["state"] == ("cancelled" if cancel else "finalizing" if late else "running")

@pytest.mark.parametrize("policy", [{"enabled": 1}, {"extension_seconds": 1},
    {"check_interval": 60}, {"max_renewals": 3}, {"reasoning_effort": None},
    {"judge_timeout_seconds": float("inf")}])
def test_policy_rejects_invalid_or_polling_parameters(policy):
    with pytest.raises(ContractError):
        time_budget_policy(policy)

def test_disabled_policy_never_invokes_judge():
    calls = []
    budget = make_budget([10], lambda p: calls.append(p), policy={"enabled": False})
    assert budget.current_deadline() == 10
    assert calls == []
