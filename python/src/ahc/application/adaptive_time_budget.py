"""One owner decision at budget expiry; no polling job or fixed renewal quota."""
import math
import threading
import time
from ahc.domain.adaptive_time_budget import time_budget_policy
from ahc.domain.sessions import ContractError

class AdaptiveTimeBudget:
    def __init__(self, deadline, policy, judge, snapshot, *, cancelled=lambda: False,
                 clock=time.monotonic):
        if (type(deadline) not in (int, float) or not math.isfinite(deadline)
                or not all(callable(x) for x in (judge, snapshot, cancelled, clock))):
            raise ContractError("adaptive budget binding invalid")
        self.initial_deadline = self._deadline = deadline
        self.policy = time_budget_policy(policy)
        self.judge, self.snapshot = judge, snapshot
        self.cancelled, self.clock = cancelled, clock
        self._lock = threading.RLock()
        self._state = "running"
        self._epoch = self._renewals = 0
        self._previous = None
        self._last_reason = "initial-budget"

    def seed_snapshot(self):
        value = self.snapshot()
        with self._lock:
            if self._epoch == 0 and self._state == "running":
                self._previous = value

    def peek_deadline(self):
        with self._lock:
            return self._deadline

    def _failure(self, now, reason):
        self._last_reason = reason
        if self.policy["on_judge_error"] == "extend":
            self._state = "running"
            self._renewals += 1
            self._deadline = now + self.policy["extension_seconds"]
        else:
            self._state = "finalizing"
            self._deadline = now + self.policy["wrap_up_seconds"]

    def current_deadline(self):
        with self._lock:
            now = self.clock()
            if self.cancelled():
                self._epoch += 1
                self._state, self._last_reason = "cancelled", "owner-stop"
                self._deadline = min(self._deadline, now)
                return self._deadline
            if not self.policy["enabled"] or now < self._deadline:
                return self._deadline
            if self._state == "judging":
                self._epoch += 1  # A pending decision has crossed its own deadline.
                self._failure(now, "judge-timeout")
                return self._deadline
            if self._state != "running":
                return self._deadline
            owner = getattr(self, "owner_thread", None)
            if owner is not None and owner != threading.get_ident():
                return max(now, self._deadline) + self.policy["judge_timeout_seconds"]
            self._state = "judging"
            self._epoch += 1
            epoch = self._epoch
            self._deadline = now + self.policy["judge_timeout_seconds"]
            decision_deadline = self._deadline
            previous, renewals = self._previous, self._renewals
        current, decision, error = None, None, None
        try:
            current = self.snapshot()
            if type(current) is not dict or type(current.get("active")) is not bool:
                raise ContractError("progress snapshot invalid")
            if current["active"]:
                decision = self.judge({"current": current, "previous": previous,
                                       "renewals": renewals})
                if (type(decision) is not dict or set(decision) != {"continue", "reason"}
                        or type(decision["continue"]) is not bool
                        or type(decision["reason"]) is not str
                        or not decision["reason"].strip() or len(decision["reason"]) > 1000):
                    raise ContractError("progress decision invalid")
        except Exception:
            error = "judge-unavailable-or-invalid"
        with self._lock:
            now = self.clock()
            if self.cancelled():
                self._epoch += 1
                self._state, self._last_reason = "cancelled", "owner-stop"
                self._deadline = min(self._deadline, now)
                return self._deadline
            if epoch != self._epoch or self._state != "judging":
                return self._deadline
            self._previous = current
            if now >= decision_deadline:
                self._failure(now, "judge-timeout")
            elif error:
                self._failure(now, error)
            elif not current["active"]:
                self._state, self._last_reason, self._deadline = "expired", "work-inactive", now
            elif decision["continue"]:
                self._state = "running"
                self._renewals += 1
                self._deadline = now + self.policy["extension_seconds"]
                self._last_reason = decision["reason"]
            else:
                self._state = "finalizing"
                self._deadline = now + self.policy["wrap_up_seconds"]
                self._last_reason = decision["reason"]
            return self._deadline

    def status(self):
        with self._lock:
            return {"state": self._state, "renewals": self._renewals,
                    "model": self.policy["model"],
                    "reasoning_effort": self.policy["reasoning_effort"],
                    "last_reason": self._last_reason,
                    "extension_seconds": self.policy["extension_seconds"],
                    "trigger": "budget-expiry", "fixed_renewal_limit": None}
