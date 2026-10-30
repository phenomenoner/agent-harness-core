"""Exact full terminal readback without another model submission."""

from ahc.domain.sessions import ContractError


def full_terminal(client, event):
    client.begin_settlement(event)
    if (event.get("method") == "turn/completed"
            and event.get("params", {}).get("threadId") == client.thread):
        terminal = event["params"].get("turn", {})
        if (terminal.get("id") == client.turn and terminal.get("status") == "completed"
                and terminal.get("error") is None
                and terminal.get("itemsView") in ("summary", "notLoaded")):
            # One bounded read of the latest turn; never submit another turn.
            client._send({"id": 5, "method": "thread/turns/list", "params": {
                "threadId": client.thread, "itemsView": "full", "limit": 1,
                "sortDirection": "desc"}})
            full, _ = client._response(5)
            turns = full.get("data")
            if (type(turns) is not list or len(turns) != 1
                    or type(turns[0]) is not dict or turns[0].get("id") != client.turn):
                raise ContractError("Codex terminal readback mismatch")
            event = {"method": "turn/completed", "params": {
                "threadId": client.thread, "turn": turns[0]}}
    return event


def receive_terminal(client, pending):
    from .codex_diagnostics import method, remember
    from .codex_protocol import CodexTurnDecoder, interrupt_message

    decoder = CodexTurnDecoder(client.thread, client.turn)
    steering = getattr(client, "steering_port", None)
    try:
        while True:
            if client._stopped():
                if client.clock() < client.request.effective_deadline:
                    client._send(interrupt_message(client.thread, client.turn))
                return client._result("unknown", "cancel-unreconciled")
            if pending:
                from .codex_client_wire import _CONTINUE, dispatch_frame

                event = dispatch_frame(client, pending.pop(0))
                if event is _CONTINUE:
                    continue
            else:
                # A terminal notification may precede the start acknowledgement.
                # Drain that already-received evidence before claiming new input.
                if steering is not None:
                    client._diagnostic_phase = "steering"
                    steering.poll(client)
                event = client._receive()
            from .codex_terminal_readback import full_terminal

            client._diagnostic_phase = "terminal-readback"
            event = full_terminal(client, event)
            client._diagnostic_phase = "decode"
            client._diagnostic_method = method(event.get("method", "response"))
            deferred = client._deferred_capacity_error
            if deferred is not None and event.get("method") == "turn/completed":
                params = event.get("params", {})
                turn = params.get("turn", {})
                failure = turn.get("error")
                if (params.get("threadId") != deferred[0] or turn.get("id") != deferred[1]
                        or turn.get("status") != "failed" or type(failure) is not dict
                        or failure.get("codexErrorInfo") != deferred[2]):
                    raise ContractError("Codex capacity terminal does not match its notification")
            result = decoder.receive(event)
            from .codex_activity import observe_activity

            observe_activity(client, event)
            if result is not None:
                from .codex_overload_recovery import continue_after_failure

                resumed = continue_after_failure(client, event)
                if resumed is not None:
                    pending = resumed
                    decoder = CodexTurnDecoder(client.thread, client.turn)
                    continue
                client._native_collaboration.require_settled()
                if result["disposition"] != "completed":
                    status = event.get("params", {}).get("turn", {}).get("status")
                    if status == "failed":
                        from .codex_backend_errors import observe_backend_error

                        observe_backend_error(client, event["params"]["turn"].get("error"),
                                              source="terminal")
                    else:
                        remember(client, "backend-interrupted")
                return client._result(
                    result["disposition"], "backend-observed", result["output_text"]
                )
    except Exception as error:
        remember(client, error=error)
        raise
    finally:
        if steering is not None:
            client._diagnostic_phase = "steering-reconcile"
            steering.reconcile_terminal()
