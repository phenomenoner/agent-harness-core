"""Pinned informational notifications; never completion or permission evidence."""

from ahc.domain.sessions import ContractError

from .codex_server_requests import SERVER_REQUEST_METHODS

_DISCARDED_ADVISORIES = frozenset(
    {
        "account/updated",
        "account/rateLimits/updated",
        "warning",
        "configWarning",
        "mcpServer/startupStatus/updated",
    }
)

# These methods have actual handlers: binding, ownership, completion, policy or
# admitted stream observations. All other no-id notifications are unused data.
# An informational frame never becomes a tool, approval, descendant or result.
_HANDLED_NOTIFICATIONS = frozenset(
    {
        "thread/started",
        "thread/status/changed",
        "thread/tokenUsage/updated",
        "thread/settings/updated",
        "remoteControl/status/changed",
        "turn/started",
        "turn/completed",
        "item/started",
        "item/completed",
        "item/agentMessage/delta",
        "item/reasoning/summaryTextDelta",
        "item/reasoning/summaryPartAdded",
        "item/reasoning/textDelta",
        "item/commandExecution/terminalInteraction",
    }
)
# A missing request id must not turn an actionable method into advisory data.
_REQUEST_METHODS = SERVER_REQUEST_METHODS | frozenset(
    {
        "initialize",
        "initialized",
        "thread/start",
        "thread/resume",
        "turn/start",
        "turn/steer",
        "turn/interrupt",
        "thread/turns/list",
        "item/tool/call",
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
        "item/tool/requestUserInput",
        "mcpServer/elicitation/request",
        "account/chatgptAuthTokens/refresh",
    }
)


def warn_status(client, *, method="thread/status/changed", code="advisory-status-ignored"):
    owner = getattr(client, "_warning_owner", client)
    if getattr(owner, "_advisory_diagnostic", None) is None:
        owner._advisory_diagnostic = {
            "category": "protocol",
            "phase": "decode",
            "last_method": method,
            "code": code,
            "remaining_ms": 0,
            "frames": min(100000, max(0, getattr(owner, "frames", 0))),
            "cleanup": "not-observed",
        }


def consume_advisory(client, message):
    """Discard unused notification data without interpreting its payload.

    The wire owns frame/deadline bounds. Never inspect or retain account secrets,
    use an advisory to change permissions, or consume an RPC request/response.
    """
    if "id" in message:
        return False
    name = message.get("method")
    if type(name) is not str or not name:
        return False
    if name == 'error':
        params = message.get('params')
        if (type(params) is dict and client.thread is not None and client.turn is not None
                and params.get('threadId') == client.thread and params.get('turnId') == client.turn
                and params.get('willRetry') is False and type(params.get('error')) is dict):
            from .codex_backend_errors import observe_backend_error

            observe_backend_error(client, params['error'], source='notification')
            if (getattr(client, 'overload_recovery', None) is not None
                    and params['error'].get('codexErrorInfo') in {
                        'serverOverloaded', 'flexUnavailable'}):
                client._deferred_capacity_error = (
                    client.thread, client.turn, params['error']['codexErrorInfo'])
                client._recovery_record.update(state='blocked', last_failure=(
                    'overloaded' if params['error']['codexErrorInfo'] == 'serverOverloaded'
                    else 'capacity'))
                return True  # Wait for the exact failed terminal before recovery eligibility.
            raise ContractError('Codex nonretryable backend error')
    if (name == "item/commandExecution/outputDelta"
            and not set(message) - {"method", "params", "emittedAtMs"}):
        # AHC never consumes command stdout as authority or a final answer. Drop
        # it before parent/child/turn binding, including stale and malformed
        # payloads. Never retry a command or infer that its effects are settled.
        # One payload-free diagnostic is bounded per admitted conversation; wire
        # deadline/cancellation checks still bound any notification flood.
        warn_status(client, method=name, code="advisory-notification-ignored")
        return True
    if name in _DISCARDED_ADVISORIES:
        warn_status(client, method=name, code="advisory-metadata-ignored")
        return True
    if (
        name in _HANDLED_NOTIFICATIONS
        or name in _REQUEST_METHODS
        or set(message) - {"method", "params", "emittedAtMs"}
    ):
        return False
    from .codex_diagnostics import method

    warn_status(client, method=method(name), code="advisory-notification-ignored")
    return True


def consume_liveness(client, message):
    """Ignore unbound liveness metadata; never enroll, authorize or finish a turn."""
    if "id" in message or message.get("method") != "thread/status/changed":
        return False
    params = message.get("params")
    thread = params.get("threadId") if type(params) is dict else None
    native = client._native_collaboration
    if type(thread) is not str or (thread != client.thread and thread not in native.children):
        warn_status(client)
        return True
    try:
        if not native.consume(message):
            consume_status(client, message)
    except ContractError:
        warn_status(client)
    return True


def consume_status(client, message):
    if "id" in message:
        return False
    if consume_advisory(client, message):
        return True
    if message.get("method") == "thread/tokenUsage/updated":
        params = message.get("params")
        thread = params.get("threadId") if type(params) is dict else None
        if type(thread) is str and client.thread is not None and thread != client.thread:
            # Discard a foreign sample without erasing this owner's valid usage.
            warn_status(
                client, method="thread/tokenUsage/updated", code="usage-observation-unavailable"
            )
            return True
        try:
            if _consume_status(client, message):
                return True
        except ContractError:
            pass
        # An unusable sample is unknown usage, never zero or a stale sample.
        client.token_observation = None
        warn_status(
            client, method="thread/tokenUsage/updated", code="usage-observation-unavailable"
        )
        return True
    return _consume_status(client, message)


def _consume_status(client, message):
    method = message.get("method")
    if method not in {
        "thread/status/changed",
        "thread/tokenUsage/updated",
        "thread/settings/updated",
    }:
        return False
    params = message.get("params")
    if set(message) != {"method", "params"} or type(params) is not dict:
        raise ContractError("Codex status notification binding invalid")
    if client.thread is None:
        # thread/start can emit status before its response establishes identity.
        # The response collector alone may buffer it; validate after exact bind,
        # before turn/start. Returning False never accepts or acts on this event.
        return False
    if params.get("threadId") != client.thread:
        raise ContractError("Codex status notification binding invalid")
    valid = False
    if method == "thread/settings/updated":
        from .codex_approval import validate_thread_configuration

        settings = params.get("threadSettings")
        if (
            set(params) != {"threadId", "threadSettings"}
            or type(settings) is not dict
            or "sandboxPolicy" not in settings
        ):
            raise ContractError("Codex settings notification shape invalid")
        readback = dict(settings)
        readback["sandbox"] = readback.pop("sandboxPolicy")
        validate_thread_configuration(client, readback)
        if (
            client.reasoning_effort is not None
            and settings.get("effort") != client.reasoning_effort
        ):
            raise ContractError("Codex settings reasoning effort mismatch")
        return True
    if method == "thread/status/changed":
        status = params.get("status")
        valid = (
            set(params) == {"threadId", "status"}
            and type(status) is dict
            and (
                set(status) == {"type"}
                and status["type"] in ("notLoaded", "idle", "systemError")
                or set(status) == {"type", "activeFlags"}
                and status["type"] == "active"
                and type(status["activeFlags"]) is list
                and len(status["activeFlags"]) <= 2
                and all(
                    flag in ("waitingOnApproval", "waitingOnUserInput")
                    for flag in status["activeFlags"]
                )
            )
        )
    else:
        usage = params.get("tokenUsage")
        valid = (
            set(params) == {"threadId", "turnId", "tokenUsage"}
            and client.turn is not None
            and params["turnId"] == client.turn
            and type(usage) is dict
            and {"last", "total"} <= set(usage)
            and not set(usage) - {"last", "total", "modelContextWindow"}
            and (
                usage.get("modelContextWindow") is None or type(usage["modelContextWindow"]) is int
            )
        )
        if valid:
            required = {
                "cachedInputTokens",
                "inputTokens",
                "outputTokens",
                "reasoningOutputTokens",
                "totalTokens",
            }
            for name in ("last", "total"):
                counts = usage[name]
                valid = valid and (
                    type(counts) is dict
                    and required <= set(counts)
                    and not set(counts) - required - {"cacheWriteInputTokens"}
                    and all(type(value) is int and value >= 0 for value in counts.values())
                )
    if not valid and method == "thread/status/changed":
        # Liveness metadata cannot grant authority or establish completion.
        # Preserve exact thread binding above, but tolerate new advisory fields.
        warn_status(client)
        return True
    if not valid:
        raise ContractError("Codex status notification shape invalid")
    if method == "thread/tokenUsage/updated":
        from ahc.domain.token_observation import TokenCounts, TokenObservation

        def counts(value):
            return TokenCounts(
                value["inputTokens"],
                value["outputTokens"],
                value["cachedInputTokens"],
                value["reasoningOutputTokens"],
                value["totalTokens"],
                value.get("cacheWriteInputTokens"),
            )

        # Replace snapshots; duplicate notifications must never add usage twice.
        # Neither `last` nor the thread cumulative `total` is a per-child bill.
        if (getattr(client, "_compaction_count", 0)
                and getattr(client, "_compaction_after", None) is None):
            client._compaction_after = usage["last"]["totalTokens"]
        client.token_observation = TokenObservation(
            client.thread, client.turn, counts(usage["last"]), counts(usage["total"]),
            model_context_window=usage.get("modelContextWindow"),
            model=getattr(client, "model", None),
            compaction_count=getattr(client, "_compaction_count", 0),
            compaction_before_tokens=getattr(client, "_compaction_before", None),
            compaction_after_tokens=getattr(client, "_compaction_after", None),
            compaction_threshold=getattr(client, "compact_token_limit", None),
            compaction_threshold_source=("catalog" if
                getattr(client, "compact_token_limit", None) is not None else "backend-default"),
        )
    return True
