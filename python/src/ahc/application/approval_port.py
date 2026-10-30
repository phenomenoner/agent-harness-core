"""One synchronous decision boundary; implementations never take the execution lease."""

from collections.abc import Callable
from typing import Protocol

from ahc.domain.runtime_approval import ApprovalDecision, ApprovalRequest


class ApprovalDecisionPort(Protocol):
    def decide(
        self, request: ApprovalRequest, *, cancelled: Callable[[], bool]
    ) -> ApprovalDecision:
        """Return by expiry/cancel; never wait inside a SQLite write transaction.

        A durable implementation publishes one pending request and consumes at most
        one matching response. No response is consent. Display only deliberately
        selected action details, never arbitrary vendor payloads or credentials.
        """
        ...


class UnavailableApproval:
    """Headless manual mode has no implicit approval or indefinite wait."""

    def decide(self, request, *, cancelled):
        return ApprovalDecision(request.digest, "cancelled" if cancelled() else "unavailable")
