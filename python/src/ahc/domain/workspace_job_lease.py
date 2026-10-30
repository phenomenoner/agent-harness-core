"""Finite renewable observation leases; never extend the owning task deadline."""

import math
import threading

from ahc.domain.sessions import ContractError


class WorkspaceJobLease:
    def __init__(self, started, hard_deadline, expected_seconds, *, slice_seconds=120):
        if (any(type(v) not in (int, float) or not math.isfinite(v)
                for v in (started, hard_deadline, expected_seconds, slice_seconds))
                or expected_seconds <= 0 or slice_seconds <= 0
                or hard_deadline <= started or expected_seconds > hard_deadline - started):
            raise ContractError("workspace job budget invalid")
        self.started = started
        self.hard_deadline = hard_deadline
        self.quiet_deadline = min(hard_deadline, started + expected_seconds)
        self.slice_seconds = slice_seconds
        self.expires = min(self.quiet_deadline, started + slice_seconds)
        self.renewals = 0
        self.lock = threading.Lock()
        self._cancel = threading.Event()
        self.stopped = threading.Event()

    @property
    def cancel_requested(self):
        return self._cancel.is_set()

    def request_cancel(self):
        self._cancel.set()

    def remaining(self, now):
        budget = getattr(self, "owner_budget", None)
        if budget is not None:
            return 0 if self.cancel_requested else max(0, budget.current_deadline() - now)
        with self.lock:
            if self.cancel_requested:
                return 0
            if now >= self.expires and now < self.quiet_deadline:
                self.expires = min(self.quiet_deadline, now + self.slice_seconds)
                self.renewals += 1
            return max(0, min(self.expires, self.hard_deadline) - now)

    def to_dict(self, now):
        budget = getattr(self, "owner_budget", None)
        if budget is not None:
            return {"source": "admitted-adaptive-owner",
                    "hard_remaining_ms": max(0, int((budget.current_deadline() - now) * 1000)),
                    "renewals": budget.status()["renewals"], "reexecution_count": 0,
                    "cancel_requested": self.cancel_requested}
        with self.lock:
            return {
                "source": "admitted-finite-quiet-work-estimate",
                "hard_remaining_ms": max(0, int((self.hard_deadline - now) * 1000)),
                "lease_remaining_ms": max(0, int((self.expires - now) * 1000)),
                "quiet_remaining_ms": max(0, int((self.quiet_deadline - now) * 1000)),
                "renewals": self.renewals, "reexecution_count": 0,
                "cancel_requested": self.cancel_requested,
            }
