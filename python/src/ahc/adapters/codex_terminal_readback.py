"""Exact terminal settlement using streamed answers or bounded item pagination."""

import json

from ahc.domain.sessions import ContractError, _identifier

from .codex_final_messages import FinalMessages

MAX_READBACK_PAGES = 128
MAX_READBACK_BYTES = 8 * 1048576


def _readback_items(client):
    answers = FinalMessages(client.thread, client.turn)
    cursor, cursors, identities, total = None, set(), set(), 0
    for _ in range(MAX_READBACK_PAGES):
        params = {"threadId": client.thread, "turnId": client.turn,
                  "limit": 1, "sortDirection": "desc"}
        if cursor is not None:
            params["cursor"] = cursor
        client._send({"id": 5, "method": "thread/items/list", "params": params})
        page, _ = client._response(5)
        total += len(json.dumps(page, ensure_ascii=True, allow_nan=False).encode("utf-8"))
        if total > MAX_READBACK_BYTES:
            raise ContractError("Codex terminal readback byte budget exceeded")
        entries = page.get("data")
        if type(entries) is not list or len(entries) > 1:
            raise ContractError("Codex terminal item page invalid")
        for entry in entries:
            if (type(entry) is not dict or entry.get("turnId") != client.turn
                    or type(entry.get("item")) is not dict):
                raise ContractError("Codex terminal readback turn mismatch")
            item = entry["item"]
            identity = _identifier(item.get("id"), "Codex readback item")
            if identity in identities:
                raise ContractError("Codex terminal readback duplicate item")
            identities.add(identity)
            answers.add(item)
        following = page.get("nextCursor")
        if following is None:
            return list(reversed(answers.items))
        if (type(following) is not str or not following or len(following) > 4096
                or following in cursors or not entries):
            raise ContractError("Codex terminal readback cursor invalid")
        cursors.add(following)
        cursor = following
    raise ContractError("Codex terminal readback page budget exceeded")


def full_terminal(client, event, answers):
    client.begin_settlement(event)
    params = event.get("params")
    terminal = params.get("turn") if type(params) is dict else None
    if (set(event) == {"method", "params"} and event.get("method") == "turn/completed"
            and params.get("threadId") == client.thread and type(terminal) is dict
            and terminal.get("id") == client.turn and terminal.get("status") == "completed"
            and terminal.get("error") is None
            and terminal.get("itemsView") in ("summary", "notLoaded")):
        # Only the exact successful terminal receipt authorizes this projection.
        # Never reassemble tool history or issue another model turn.
        items = answers.items if answers.items else _readback_items(client)
        return {"method": "turn/completed", "params": {"threadId": client.thread,
                "turn": {**terminal, "itemsView": "full", "items": items}}}
    return event


def receive_terminal(client, pending):
    from .codex_diagnostics import method, remember
    from .codex_protocol import CodexTurnDecoder, interrupt_message

    decoder = CodexTurnDecoder(client.thread, client.turn)
    answers = FinalMessages(client.thread, client.turn)
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
            answers.observe(event)
            event = full_terminal(client, event, answers)
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
                    answers = FinalMessages(client.thread, client.turn)
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
