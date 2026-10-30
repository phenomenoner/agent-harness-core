"""Finite backend error categories; raw provider messages never leave this boundary."""

from ahc.domain.sessions import ContractError

_KINDS = {
    "unauthorized": "unauthorized", "contextWindowExceeded": "context-window",
    "sessionBudgetExceeded": "session-budget", "usageLimitExceeded": "usage-limit",
    "rateLimitExceeded": "rate-limit", "flexUnavailable": "capacity",
    "serverOverloaded": "overloaded", "internalServerError": "server-error",
    "badRequest": "bad-request", "sandboxError": "sandbox",
    "cyberPolicy": "policy", "misalignmentPolicyViolation": "policy",
    "tooManyDenials": "policy", "other": "other",
}
_HTTP_VARIANTS = frozenset({"httpConnectionFailed", "responseStreamConnectionFailed",
                            "responseStreamDisconnected", "responseTooManyFailedAttempts"})


def observe_backend_error(client, error, *, source, rpc_code=None):
    """Capture only bounded enums/numbers, never authorize retry or completion."""
    from .codex_diagnostics import remember

    info = error.get("codexErrorInfo") if type(error) is dict else None
    failure = {"source": source, "kind": _KINDS.get(info, "other")
               if type(info) is str else "other"}
    failure["tool_activity"] = ("observed" if getattr(client, "_observed_tool_activity", False)
                                else "not-observed")
    if type(info) is dict and len(info) == 1:
        variant = next(iter(info))
        payload = info[variant]
        if variant in _HTTP_VARIANTS and type(payload) is dict:
            status = payload.get("httpStatusCode")
            failure["kind"] = "connection" if variant != "responseStreamDisconnected" else "stream"
            if type(status) is int and 100 <= status <= 599:
                failure["http_status"] = status
                if status in {401, 403}:
                    failure["kind"] = "unauthorized"
                elif status == 429:
                    failure["kind"] = "rate-limit"
    # This pinned error has no typed HTTP detail. Match the complete known
    # template, not arbitrary messages, identifiers or credential-shaped text.
    if (type(error) is dict
            and error.get("message") == "workspace routing discovery unauthorized (401)"):
        failure.update(kind="routing-unauthorized", http_status=401)
    if type(rpc_code) is int and -(2**31) <= rpc_code < 2**31:
        failure["rpc_code"] = rpc_code
        if rpc_code == -32001:
            failure["kind"] = "overloaded"
    remember(client, "backend-failed", code=(
        "backend-rpc-rejected" if source == "rpc" else "backend-nonretryable-error"))
    # A prior fault retains ownership of the diagnostic; do not decorate it
    # with an unrelated later observation.
    if client._diagnostic.get("category") == "backend-failed":
        client._diagnostic.setdefault("backend_failure", failure)


def rpc_error(client, message, identifier):
    """A correlated RPC rejection is distinct from malformed wire correlation."""
    if (set(message) != {"id", "error"} or type(message.get("id")) is not int
            or message["id"] != identifier or type(message.get("error")) is not dict):
        return False
    error = message["error"]
    if type(error.get("code")) is not int or type(error.get("message")) is not str:
        raise ContractError("Codex RPC error shape invalid")
    observe_backend_error(client, error.get("data"), source="rpc", rpc_code=error["code"])
    raise ContractError("Codex backend RPC rejected")
