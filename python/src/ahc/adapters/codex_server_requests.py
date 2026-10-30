"""Explicit policies for the remaining pinned app-server request surfaces."""

import math

from ahc.domain.sessions import ContractError

SERVER_REQUEST_METHODS = frozenset({
    "item/commandExecution/requestApproval", "item/fileChange/requestApproval",
    "item/tool/requestUserInput", "mcpServer/elicitation/request",
    "item/permissions/requestApproval", "item/tool/call",
    "account/chatgptAuthTokens/refresh", "attestation/generate", "currentTime/read",
    "applyPatchApproval", "execCommandApproval",
})
_OPTIONAL = frozenset({"item/tool/requestUserInput", "mcpServer/elicitation/request",
                       "item/permissions/requestApproval"})


def respond_auxiliary(client, message):
    method, params = message.get("method"), message.get("params")
    handled = _OPTIONAL | {"currentTime/read", "account/chatgptAuthTokens/refresh",
                           "attestation/generate", "applyPatchApproval", "execCommandApproval"}
    if type(method) is not str or method not in handled:
        return False
    from .codex_approval import _request_id

    identity = _request_id(message["id"])
    if method in _OPTIONAL | {"currentTime/read"}:
        owner = (client if client.thread is not None and type(params) is dict
                 and params.get("threadId") == client.thread else
                 client._native_collaboration.children.get(params.get("threadId"))
                 if type(params) is dict else None)
        if owner is None or (method != "currentTime/read" and (
                owner.turn is None or params.get("turnId") != owner.turn)):
            raise ContractError("Codex auxiliary request owner mismatch")
        if method == "currentTime/read":
            if set(params) != {"threadId"}:
                raise ContractError("Codex current-time request invalid")
            now = client._wall_clock()
            if type(now) not in (float, int) or not math.isfinite(now) or not 0 <= now < 2**63:
                raise ContractError("Codex wall clock unavailable")
            client._send({"id": identity, "result": {"currentTimeAt": int(now)}})
        else:
            # A typed refusal gives the same model turn a chance to explain or
            # use its existing tools. It never grants permissions or invents a
            # user's answer/consent, and does not run the unsupported effect.
            client._send({"id": identity, "error": {
                "code": -32601, "message": "AHC does not bridge this interaction; "
                "no permissions, answers or consent were granted"}})
            from .codex_status_notifications import warn_status

            warn_status(client, method=method, code="backend-request-unavailable")
        return True
    # File-owned authentication is not an external-token refresh authority.
    # Attestation and legacy approvals likewise cannot be fabricated from host
    # identity or silently substituted for the admitted approval bridge.
    client._send({"id": identity, "error": {
        "code": -32601, "message": "AHC has no authority for this backend request"}})
    from .codex_diagnostics import remember

    remember(client, "backend-failed", code="backend-request-unavailable")
    if client._diagnostic.get("category") == "backend-failed":
        kind = ("auth-refresh-required" if method == "account/chatgptAuthTokens/refresh" else
                "attestation-required" if method == "attestation/generate" else "legacy-approval")
        client._diagnostic.setdefault("backend_failure", {"source": "server-request", "kind": kind})
    raise ContractError("Codex required backend request unavailable")
