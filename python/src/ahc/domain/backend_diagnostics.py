"""Payload-free backend failure observations; never acceptance or retry authority."""

CATEGORIES = frozenset({"backend-interrupted", "backend-failed", "cancelled", "deadline",
                        "protocol", "transport", "repository-busy", "internal",
                        "answer-seal", "cleanup", "workspace"})
PHASES = frozenset({"admission", "send", "receive", "decode", "steering",
                    "steering-reconcile", "terminal-readback", "answer-seal", "cleanup", "tool"})
METHODS = frozenset({"initialize", "initialized", "thread/start", "thread/resume",
                     "turn/start", "turn/steer", "turn/interrupt", "thread/turns/list",
                     "error", "currentTime/read", "attestation/generate",
                     "account/chatgptAuthTokens/refresh", "item/permissions/requestApproval",
                     "item/tool/requestUserInput", "mcpServer/elicitation/request",
                     "turn/started", "turn/completed", "thread/started",
                     "thread/status/changed", "thread/tokenUsage/updated",
                     "account/updated", "account/rateLimits/updated", "configWarning",
                     "mcpServer/startupStatus/updated", "item/started", "item/completed",
                     "item/agentMessage/delta", "item/commandExecution/outputDelta",
                     "item/commandExecution/terminalInteraction", "item/tool/call",
                     "item/commandExecution/requestApproval", "item/fileChange/requestApproval",
                     "item/reasoning/summaryTextDelta", "item/reasoning/summaryPartAdded",
                     "item/reasoning/textDelta", "warning", "response", "other", "none"})
CODES = frozenset({"none", "unknown", "closed", "deadline", "write-indeterminate", "eof",
                   "backend-startup-timeout", "control-request-timeout",
                    "terminal-settlement-timeout", "backend-nonretryable-error",
                    "backend-rpc-rejected", "backend-request-unavailable",
                   "invalid-encoding", "malformed-json", "protocol-frame", "reader-io",
                   "cleanup-unverified", "unsupported-event", "thread-mismatch",
                   "turn-mismatch", "contract-invalid", "advisory-status-ignored",
                   "tool-budget-exhausted", "workspace-effects-unresolved",
                   "workspace-resource-unavailable",
                   "advisory-warning-ignored",
                   "advisory-metadata-ignored", "usage-observation-unavailable",
                   "advisory-notification-ignored",
                   "status-shape-invalid", "status-binding-invalid", "native-thread-unbound"})


def transport_failure_projection(value):
    """Retain fixed reader reasons and observed byte counts, never a wire payload."""
    reasons = {"closed", "eof", "deadline", "wait-timeout", "reader-io", "invalid-encoding",
               "invalid-json", "nonobject-frame", "frame-size-limit", "unterminated-frame"}
    if (type(value) is not dict or type(value.get("reason")) is not str
            or value["reason"] not in reasons):
        return {}
    result = {"reason": value["reason"]}
    for key, maximum in (("observed_bytes", 64 * 1048576 + 1),
                         ("limit_bytes", 64 * 1048576)):
        if type(value.get(key)) is int and 0 < value[key] <= maximum:
            result[key] = value[key]
    return result


def diagnostic_projection(value):
    """Drop malformed advisory data without changing a previously valid outcome."""
    required = {"category", "phase", "last_method", "code", "remaining_ms", "frames", "cleanup"}
    if (type(value) is not dict or not required <= set(value)
            or set(value) - required - {
                "workspace_failure", "backend_failure", "transport_failure"}):
        return {}
    for key, allowed in (("category", CATEGORIES), ("phase", PHASES),
                         ("last_method", METHODS), ("code", CODES),
                         ("cleanup", {"not-observed", "client-close-returned", "unverified"})):
        if type(value[key]) is not str or value[key] not in allowed:
            return {}
    for key, maximum in (("remaining_ms", 86400000), ("frames", 100000)):
        if type(value[key]) is not int or not 0 <= value[key] <= maximum:
            return {}
    projected = {key: value[key] for key in required}
    transport_failure = transport_failure_projection(value.get("transport_failure"))
    if value["category"] == "transport" and transport_failure:
        projected["transport_failure"] = transport_failure
    from .workspace import workspace_failure_projection

    failure = workspace_failure_projection(value.get("workspace_failure"))
    if failure:
        projected["workspace_failure"] = failure
    backend_failure = value.get("backend_failure")
    if type(backend_failure) is dict:
        kinds = {"unauthorized", "routing-unauthorized", "context-window", "session-budget",
                 "usage-limit", "rate-limit", "capacity", "overloaded", "server-error",
                 "bad-request", "sandbox", "policy", "other", "connection", "stream",
                 "auth-refresh-required", "attestation-required", "legacy-approval"}
        if (set(backend_failure) <= {"source", "kind", "http_status", "rpc_code", "tool_activity"}
                and type(backend_failure.get("source")) is str
                and backend_failure["source"] in {
                    "rpc", "notification", "terminal", "server-request"}
                and type(backend_failure.get("kind")) is str and backend_failure["kind"] in kinds
                and ("tool_activity" not in backend_failure or (
                    type(backend_failure["tool_activity"]) is str
                    and backend_failure["tool_activity"] in {"observed", "not-observed"}))
                and ("http_status" not in backend_failure or (
                    type(backend_failure["http_status"]) is int
                    and 100 <= backend_failure["http_status"] <= 599))
                and ("rpc_code" not in backend_failure or (
                    type(backend_failure["rpc_code"]) is int
                    and -(2**31) <= backend_failure["rpc_code"] < 2**31))):
            projected["backend_failure"] = dict(backend_failure)
    return {"backend_diagnostic": projected}
