"""Wait for an existing local decision channel with no transaction held open."""

import time

from ahc.domain.runtime_approval import ApprovalDecision
from ahc.infrastructure.working_repository import WorkingWriterBusy


class PendingApprovalPort:
    def __init__(
        self, store, admitted, *, clock=time.monotonic, wall_clock=time.time, wait=time.sleep,
        notifier=None, interactive=True
    ):
        self.store, self.admitted = store, admitted
        self.clock, self.wall_clock, self.wait = clock, wall_clock, wait
        self.notifier = notifier
        self.interactive = interactive
        self.activity_event = None

    def _activity(self, phase):
        if callable(self.activity_event):
            try:
                self.activity_event(phase)
            except Exception:
                pass

    def decide(self, request, *, cancelled):
        try:
            return self._decide(request, cancelled=cancelled)
        except WorkingWriterBusy:
            # No tool effect has been authorized. Keep durable state and deny
            # execution without replaying publication, notification or consumption.
            return ApprovalDecision(request.digest, "unavailable")

    def _decide(self, request, *, cancelled):
        if cancelled():
            return ApprovalDecision(request.digest, "cancelled")
        if self.interactive is not True:
            # Notification-only jobs cannot request live chat interaction or
            # acquire extra authority. The model receives ordinary denied-tool
            # feedback and can explain the required user action in its report.
            return ApprovalDecision(request.digest, "unavailable")
        # Monotonic elapsed time bounds the wait even if the wall clock moves back.
        remaining = max(
            0.0,
            min(
                request.expires_at - self.wall_clock(),
                self.admitted.effective_policy.approval_wait_seconds,
            ),
        )
        deadline = self.clock() + remaining
        if remaining == 0:
            return ApprovalDecision(request.digest, "timeout")
        self.store.publish(self.admitted, request)
        self._activity("等待核准")
        try:
            if self.notifier is not None:
                self.notifier(request)
            while True:
                stopped = bool(cancelled())
                expired = self.clock() >= deadline
                result = self.store.consume(
                    request.session, request.digest, cancelled=stopped, timed_out=expired
                )
                if result is not None:
                    return result
                # consume committed/closed its transaction before this bounded idle wait.
                self.wait(min(0.1, max(0.0, deadline - self.clock())))
        finally:
            self._activity("模型處理中")
