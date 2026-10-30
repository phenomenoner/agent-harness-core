"""Bounded Codex wire-frame handling for ``CodexProtocolClient``.

The client retains its method entry points so callers and fixture monkeypatches
continue to observe the same conversation seam.
"""

from ahc.domain.sessions import ContractError

_CONTINUE = object()

def receive(client):
    """Skip handled notifications iteratively within the admitted deadline."""
    while True:
        if client._stopped():
            from .codex_protocol import interrupt_message

            if client.turn is not None and client.clock() < client.io_deadline:
                client._send(interrupt_message(client.thread, client.turn))
            raise ContractError("Codex receive deadline or cancellation")
        message = _receive_one(client)
        if message is not _CONTINUE:
            return message


def _receive_one(client):
    """Receive one admitted frame; counts are telemetry, not lifetime quotas."""
    client.frames += 1
    steering = getattr(client, "steering_port", None)
    wrap_up = getattr(client, "_wrap_up", None)
    if wrap_up is not None:
        wrap_up.poll(client)

    def idle():
        if not getattr(client, 'effects_open', True):
            return
        client._diagnostic_phase = "steering"
        if wrap_up is not None:
            wrap_up.poll(client)
        if steering is not None:
            steering.poll(client)
        client._diagnostic_phase = "receive"

    client._diagnostic_phase = "receive"
    deadline = (lambda: client.io_deadline) if client.request.time_budget is not None else client.io_deadline
    message = (
        client.transport.receive(deadline=deadline)
        if steering is None and wrap_up is None
        else client.transport.receive(
            deadline=deadline, on_idle=idle
        )
    )
    return dispatch_frame(client, message)


def dispatch_frame(client, message):
    """Use one dispatcher for live frames and frames retained before turn binding."""
    from .codex_diagnostics import method

    steering = getattr(client, "steering_port", None)
    if type(message) is not dict:
        raise ContractError("Codex frame must be an object")
    client._diagnostic_phase = "decode"
    client._diagnostic_method = method(message.get("method", "response"))
    # The server can emit events or ask for tools before its turn/start RPC
    # acknowledgement. Retain them, without interpreting authority, until the
    # exact returned turn identity and its bridges have been installed.
    if (client.submitted and client.thread is not None and client.turn is None
            and type(message.get("method")) is str):
        return message
    from .codex_context_policy import observe_compaction
    from .codex_overload_recovery import observe_effect_item
    from .codex_status_notifications import consume_advisory, consume_liveness, consume_status

    observe_compaction(client, message)
    observe_effect_item(client, message)

    if consume_advisory(client, message):
        return _CONTINUE
    if "id" not in message and message.get("method") == "thread/tokenUsage/updated":
        params = message.get("params")
        thread = params.get("threadId") if type(params) is dict else None
        observed = (client._native_collaboration.children.get(thread)
                    if type(thread) is str else None)
        if type(message.get("emittedAtMs")) is int:
            message = {key: value for key, value in message.items() if key != "emittedAtMs"}
        # Observe only an already enrolled owner. Usage cannot enroll a child.
        consume_status(observed if observed is not None else client, message)
        return _CONTINUE
    if "id" not in message and "emittedAtMs" in message:
        if type(message["emittedAtMs"]) is not int:
            raise ContractError("Codex notification timestamp invalid")
        message = {key: value for key, value in message.items() if key != "emittedAtMs"}
    if consume_liveness(client, message):
        return _CONTINUE
    params = message.get("params")
    thread = params.get("threadId") if type(params) is dict else None
    owned = thread == client.thread or (type(thread) is str
                                        and thread in client._native_collaboration.children)
    if owned and type(params) is dict:
        item = params.get("item")
        if (message.get("method") == "item/started" and type(item) is dict
                and item.get("type") not in {"agentMessage", "userMessage", "reasoning",
                                             "plan", "contextCompaction"}):
            client._observed_tool_activity = True
        elif (message.get("method") == "item/completed" and type(item) is dict
                and item.get("type") == "subAgentActivity"):
            client._observed_tool_activity = True
        elif message.get("method") == "item/tool/call" and "id" in message:
            client._observed_tool_activity = True
    if client._native_collaboration.consume(message):
        return _CONTINUE
    if "id" not in message and consume_status(client, message):
        return _CONTINUE
    if ("id" not in message and message.get("method") == "thread/started"
            and client.thread is not None):
        params = message.get("params")
        if (
            set(message) != {"method", "params"}
            or type(params) is not dict or set(params) != {"thread"}
            or type(params["thread"]) is not dict
            or params["thread"].get("id") != client.thread
            or getattr(client, "_thread_started_seen", False)
        ):
            raise ContractError("Codex thread notification mismatch or duplicate")
        client._thread_started_seen = True
        return _CONTINUE
    if "id" not in message and message.get("method") == "remoteControl/status/changed":
        params = message.get("params")
        if (
            not {"method", "params"} <= set(message)
            or set(message) - {"method", "params", "emittedAtMs"}
            or ("emittedAtMs" in message and type(message["emittedAtMs"]) is not int)
            or type(params) is not dict
            or not {"installationId", "serverName", "status"} <= set(params)
            or set(params) - {"installationId", "serverName", "status", "environmentId"}
            or params["status"] != "disabled"
            or any(type(params[key]) is not str for key in ("installationId", "serverName"))
            or params.get("environmentId") is not None
        ):
            raise ContractError("Codex remote control state is not admitted")
        # Startup metadata has no thread effect. Do not expose private identities.
        # The shared frame budget still limits notification floods through the
        # original client method.
        return _CONTINUE
    wrap_up = getattr(client, "_wrap_up", None)
    if wrap_up is not None and wrap_up.consume_response(client, message):
        return _CONTINUE
    if steering is not None and steering.consume_response(message):
        return _CONTINUE
    if "id" in message and "method" in message:
        if client._native_collaboration.respond(message):
            return _CONTINUE
        if getattr(client, '_native_terminal_received', False):
            from .codex_approval import _request_id

            try:
                _request_id(message['id'])
            except ContractError:
                pass
            else:
                client._send({'id': message['id'], 'error': {
                    'code': -32600, 'message': 'turn already completed'}})
            raise ContractError('Codex server request after native completion')
        from .codex_server_requests import respond_auxiliary

        if respond_auxiliary(client, message):
            return _CONTINUE
        if client._approval_bridge is not None and message["method"] in {
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
        }:
            try:
                approval_response = client._approval_bridge.respond(
                    message, cancelled=client.cancelled
                )
            except ContractError:
                from .codex_approval import _request_id

                identifier = message.get("id")
                try:
                    _request_id(identifier)
                except ContractError:
                    pass
                else:
                    client._send(
                        {
                            "id": identifier,
                            "error": {"code": -32602, "message": "invalid approval request"},
                        }
                    )
                raise
            client._send(approval_response)
            return _CONTINUE
        if client._bridge is not None and message["method"] == "item/tool/call":
            if client._stopped():
                raise ContractError("dynamic tool deadline or cancellation")
            client._send(client._bridge.respond(message))
            if client._bridge.reporting:
                client._diagnostic_phase = "tool"
                client._diagnostic_method = "item/tool/call"
                from .codex_diagnostics import remember

                remember(client, "workspace", error=(
                    ContractError("dynamic tool effects unresolved")
                    if client._bridge.unresolved else None), code="workspace-resource-unavailable")
                # Fence tools, not the model. Keep this exact turn/deadline and
                # consume its independently verified final report without replay.
            return _CONTINUE
        identifier = message["id"]
        valid = type(identifier) is int and -(2**63) <= identifier < 2**63
        valid = valid or (
            type(identifier) is str
            and 0 < len(identifier) <= 128
            and not any(0xD800 <= ord(c) <= 0xDFFF for c in identifier)
        )
        if valid:
            client._send(
                {"id": identifier, "error": {"code": -32601, "message": "unsupported method"}}
            )
        raise ContractError("Codex server request is not admitted")
    return message


def response(client, identifier, allowed_notifications=()):
    """Correlate one response while bounding admitted pre-response events."""
    pending = []
    while True:
        message = client._receive()
        if "id" in message:
            if ("method" in message and client.submitted
                    and client.thread is not None and client.turn is None):
                pending.append(message)
                continue
            from .codex_backend_errors import rpc_error

            rpc_error(client, message, identifier)
            if (
                type(message["id"]) is not int
                or message["id"] != identifier
                or set(message) != {"id", "result"}
                or type(message["result"]) is not dict
            ):
                raise ContractError("Codex response correlation or shape invalid")
            return message["result"], pending
        if (client.submitted and client.thread is not None and client.turn is None
                and type(message.get("method")) is str):
            pending.append(message)
            continue
        if (
            set(message) != {"method", "params"}
            or message["method"] not in allowed_notifications
        ):
            raise ContractError("Codex pre-response event is unsupported")
        pending.append(message)
