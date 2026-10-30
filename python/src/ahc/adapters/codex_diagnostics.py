"""Capture a first fault before cleanup without retaining exception payloads."""
from math import isfinite

from ahc.domain.backend_diagnostics import CODES, METHODS, transport_failure_projection
from ahc.domain.sessions import ContractError
from ahc.infrastructure.working_repository import WorkingWriterBusy

from .codex_transport import CodexTransportError


def method(value):
    return value if type(value) is str and value in METHODS else "other"


def remember(client, category=None, error=None, *, code="none"):
    if getattr(client, "_diagnostic", None) is not None:
        return
    if error is not None:
        if isinstance(error, WorkingWriterBusy):
            category = "repository-busy"
        elif isinstance(error, CodexTransportError):
            category = "transport"
            code = error.transport_error_type
        elif isinstance(error, ContractError):
            category = "protocol"
            code = {"Codex event unsupported": "unsupported-event",
                    "Codex event thread mismatch": "thread-mismatch",
                    "Codex event turn mismatch": "turn-mismatch",
                    "Codex item turn mismatch": "turn-mismatch",
                    "Codex status notification shape invalid": "status-shape-invalid",
                    "Codex status notification binding invalid": "status-binding-invalid",
                    "Codex warning notification shape invalid": "status-shape-invalid",
                    "Codex warning notification binding invalid": "status-binding-invalid",
                    "dynamic tool call limit exceeded": "tool-budget-exhausted",
                    "dynamic tool effects unresolved": "workspace-effects-unresolved",
                    "Codex unrelated native thread": "native-thread-unbound"}.get(
                        str(error), "contract-invalid")
            if code == "workspace-effects-unresolved":
                category = "workspace"
        elif isinstance(error, (OSError, EOFError)):
            category = "transport"
            code = "unknown"
        else:
            category = "internal"
    budget = client.request.time_budget
    deadline = budget.peek_deadline() if budget is not None else client.request.deadline
    remaining = deadline - client.clock()
    remaining_ms = int(min(86400000, max(0, remaining * 1000))) if isfinite(remaining) else 0
    client._diagnostic = {
        "category": category, "phase": getattr(client, "_diagnostic_phase", "admission"),
        "last_method": getattr(client, "_diagnostic_method", "none"),
        "code": code if code in CODES else "unknown", "remaining_ms": remaining_ms,
        "frames": min(100000, max(0, client.frames)), "cleanup": "not-observed",
    }
    if isinstance(error, CodexTransportError):
        failure = transport_failure_projection(error.transport_failure)
        if failure:
            client._diagnostic["transport_failure"] = failure
    if category == "workspace":
        from ahc.domain.workspace import workspace_failure_projection

        failure = workspace_failure_projection(
            getattr(getattr(client, "_bridge", None), "workspace_failure", None))
        if failure:
            client._diagnostic["workspace_failure"] = failure
