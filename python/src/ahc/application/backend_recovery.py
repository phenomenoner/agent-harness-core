"""Finite host-owned recovery after new read-only transport evidence."""

import time


class BackendRecovery:
    def __init__(self, *, clock=time.monotonic):
        self.clock = clock
        self.owner = None
        self.probes = self.attempts = 0
        self.state = "idle"
        self.due = None

    def observe(self):
        return {"state": self.state, "probes": self.probes, "probe_limit": 2,
                "attempts": self.attempts, "attempt_limit": 1}

    def step(self, owner, cleanup, trace):
        # Identity/protocol refusals and live activity-worker errors are not transport outages.
        probe = getattr(owner, "recovery_ready", None)
        if not callable(probe) or (trace or {}).get("exception_type") != "WslControlTransportUnavailable":
            return
        if self.owner is None:
            self.owner, self.state, self.due = owner, "waiting", self.clock() + 15
        if self.owner is not owner or self.state not in ("waiting", "unavailable"):
            return
        if self.clock() < self.due:
            return
        self.probes += 1
        try:
            ready = probe() is True
        except Exception:
            ready = False
        if not ready:
            self.state = "exhausted" if self.probes >= 2 else "unavailable"
            self.due = self.clock() + 45
            return
        # New transport success permits one exact-owner reconciliation, never a model replay.
        self.attempts += 1
        self.state = "recovering"
        try:
            cleanup()
        except Exception:
            self.state = "failed"
        else:
            self.state = "recovered"
