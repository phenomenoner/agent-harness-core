"""AHC-owned durable turn observations; backend completion is not goal completion."""

from dataclasses import dataclass

from .agent_backend import AgentTurnObservation, AgentTurnRequest
from .result_artifacts import result_artifacts
from .sessions import ContractError, _identifier
from .skill_values import digest_json

RUNTIME_RESULT_SCHEMA_VERSION = "ahc.runtime-result.v2"
CLEANUP_NOTICE = "\n\n[Answer verified; resource cleanup pending.]"


def request_fingerprint(request: AgentTurnRequest) -> str:
    # A monotonic deadline belongs to one process invocation, not restart identity.
    return digest_json(
        {
            "session": request.session.to_dict(),
            "operation": request.operation_id,
            "goal": request.goal_digest,
            "input": request.input_digest,
            "revisions": list(request.revision_digests),
            "artifact": request.artifact_digest,
            "configuration": request.configuration_digest,
            "workspace": request.workspace_scope,
            "thread": request.backend_thread,
            "purpose": request.purpose,
            "capabilities": list(request.capabilities),
            "limit": request.upstream_request_limit,
            **({"skill_bodies": request.skill_bodies} if request.skill_bodies else {}),
            **(
                {"effective_policy": request.effective_policy.to_dict()}
                if request.effective_policy is not None
                else {}
            ),
        }
    )


@dataclass(frozen=True, slots=True)
class RuntimeTurn:
    operation_id: str
    request_digest: str
    status: str
    result: dict[str, object] | None

    def to_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id,
            "request_digest": self.request_digest,
            "status": self.status,
            "result": self.result,
        }


def _backend_counters(admissions, accepted, recovery):
    from .backend_recovery import recovery_projection

    projection = recovery_projection(recovery)
    maximum = 1 + (recovery or {}).get("attempts", 0)
    if any(type(count) is not int or not 0 <= count <= maximum
           for count in (admissions, accepted)):
        raise ContractError("backend counters invalid")
    if accepted > admissions:
        raise ContractError("backend counters inconsistent")
    return projection


def accept_observation(request: AgentTurnRequest, value: AgentTurnObservation) -> dict[str, object]:
    """Validate at the application acceptance seam before committing any result."""
    if type(value) is not AgentTurnObservation or value.request != request:
        raise ContractError("backend result binding mismatch")
    if type(value.disposition) is not str or value.disposition not in (
        "completed",
        "unknown",
        "unsupported",
        "not-submitted",
    ):
        raise ContractError("backend disposition unsupported")
    if type(value.output_text) is not str:
        raise ContractError("backend output type invalid")
    if any(0xD800 <= ord(c) <= 0xDFFF for c in value.output_text):
        raise ContractError("backend output encoding invalid")
    if type(value.unresolved_effects) is not bool:
        raise ContractError("backend effect disposition invalid")
    if type(value.cleanup_pending) is not bool:
        raise ContractError("backend cleanup disposition invalid")
    if type(value.final_report) is not bool:
        raise ContractError("backend report disposition invalid")
    recovery = _backend_counters(value.admissions, value.accepted_backend_turns,
                                 value.backend_recovery)
    if value.disposition == "completed" and (
        value.accepted_backend_turns < 1 or value.unresolved_effects and not value.final_report
    ):
        raise ContractError("backend success lacks terminal evidence")
    if value.final_report and (
        value.disposition != "completed" or value.accepted_backend_turns < 1
        or value.backend_thread is None or value.backend_turn is None
        or not value.output_text.strip() or value.artifacts or value.reply_to_message_id is not None
    ):
        raise ContractError("problem report lacks exact terminal evidence")
    for label, identifier in (("backend completion thread", value.backend_thread),
                              ("backend completion turn", value.backend_turn)):
        if identifier is not None:
            _identifier(identifier, label)
    from .token_observation import durable_token_snapshot

    tokens = durable_token_snapshot(
        value.token_observation, value.backend_thread, value.backend_turn)
    # Do not persist arbitrary vendor reason/thread fields or diagnostics.
    artifacts = result_artifacts(value.artifacts)
    from .channel_reply import reply_message_id

    reply = value.reply_to_message_id
    if reply is not None:
        reply_message_id(reply)
        if value.disposition != "completed":
            raise ContractError("non-completed result cannot select a reply")
    from .backend_diagnostics import diagnostic_projection
    from .confirmed_work import confirmed_work_projection
    if artifacts and value.disposition != "completed":
        raise ContractError("non-completed result cannot select artifacts")
    return {
        "result_schema_version": RUNTIME_RESULT_SCHEMA_VERSION,
        "disposition": "unknown" if value.final_report else value.disposition,
        "output_text": value.output_text if value.disposition == "completed" else "",
        "admissions": value.admissions,
        "accepted_backend_turns": value.accepted_backend_turns,
        "vendor_usage": None,
        **tokens,
        **recovery,
        "unresolved_effects": value.unresolved_effects,
        "accepted": value.disposition == "completed" and not value.final_report,
        **({"report_accepted": True} if value.final_report else {}),
        "backend_completion": {"thread": value.backend_thread, "turn": value.backend_turn},
        **diagnostic_projection(value.backend_diagnostic),
        **confirmed_work_projection(value.confirmed_work, request),
        **({"cleanup_pending": True} if value.cleanup_pending else {}),
        **({"artifacts": artifacts} if artifacts else {}),
        **({"reply_to_message_id": reply} if reply is not None else {}),
    }


def answer_seal_projection(result: dict[str, object]) -> dict[str, object]:
    """Immutable answer evidence, excluding later diagnostics and cleanup state."""
    if (
        type(result) is not dict
        or result.get("result_schema_version") != RUNTIME_RESULT_SCHEMA_VERSION
    ):
        raise ContractError("answer seal requires the current result schema")
    projection = dict(result)
    projection.pop("cleanup_pending", None)
    projection.pop("cleanup_notice", None)
    projection.pop("cleanup_trace", None)
    # Diagnostics can arrive after the first seal (including transport cleanup).
    # They remain in the durable terminal result, but never change answer identity.
    projection.pop("backend_diagnostic", None)
    _backend_counters(projection.get("admissions"), projection.get("accepted_backend_turns"),
                      projection.get("backend_recovery"))
    report = projection.get("report_accepted") is True
    if report:
        if (projection.get("accepted") is not False or projection.get("disposition") != "unknown"
                or type(projection.get("unresolved_effects")) is not bool
                or projection["accepted_backend_turns"] < 1
                or type(projection.get("output_text")) is not str
                or not projection["output_text"].strip()
                or projection.get("artifacts")
                or projection.get("reply_to_message_id") is not None):
            raise ContractError("answer seal requires an observed problem report")
    elif projection.get("accepted") is not True or projection.get("disposition") != "completed":
        raise ContractError("answer seal requires an accepted completion")
    elif projection.get("unresolved_effects") is not False:
        raise ContractError("answer seal requires resolved effects")
    completion = projection.get("backend_completion")
    if (
        type(completion) is not dict
        or set(completion) != {"thread", "turn"}
        or any(value is not None and type(value) is not str for value in completion.values())
    ):
        raise ContractError("answer seal completion binding invalid")
    if report and any(type(identifier) is not str for identifier in completion.values()):
        raise ContractError("problem report completion binding unavailable")
    for label, identifier in (("backend completion thread", completion["thread"]),
                              ("backend completion turn", completion["turn"])):
        if identifier is not None:
            _identifier(identifier, label)
    return projection
