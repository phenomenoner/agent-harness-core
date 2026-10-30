"""Finite same-thread continuation after an exact terminal capacity failure."""

from ahc.domain.sessions import ContractError, _identifier

from .codex_protocol import turn_message

CONTINUATION = (
    "Continue the same authorized task after a temporary model capacity failure. "
    "Do not restart the task or replay earlier tool calls. Their receipts and "
    "confirmed results remain in this thread. First inspect any uncertain outcome; "
    "then perform only unfinished work and return an accurate final report. "
    "The original authority, operation, workspace and deadline still apply."
)


def observe_effect_item(client, message):
    params = message.get("params")
    if (type(params) is not dict or params.get("threadId") != client.thread
            or params.get("turnId") != client.turn or client.turn is None):
        return
    item = params.get("item")
    if type(item) is not dict or item.get("type") in {
            "agentMessage", "userMessage", "reasoning", "plan", "contextCompaction"}:
        return
    identity = item.get("id")
    if type(identity) is not str:
        return
    active = client._recovery_effect_items
    if message.get("method") == "item/started":
        active.add(identity)
    elif message.get("method") == "item/completed":
        active.discard(identity)


def continue_after_failure(client, event):
    policy = client.overload_recovery
    if policy is None:
        return None
    turn = event.get("params", {}).get("turn", {})
    error = turn.get("error")
    kind = error.get("codexErrorInfo") if type(error) is dict else None
    if (turn.get("status") != "failed" or turn.get("id") != client.turn
            or event.get("params", {}).get("threadId") != client.thread
            or kind not in {"serverOverloaded", "flexUnavailable"}):
        return None
    record = client._recovery_record
    record["last_failure"] = "overloaded" if kind == "serverOverloaded" else "capacity"
    try:
        client._native_collaboration.require_settled()
    except ContractError:
        record["state"] = "blocked"
        raise
    bridge = client._bridge
    if (client._recovery_effect_items or bridge is not None and
            (bridge.unresolved or bridge.reporting)):
        record["state"] = "blocked"
        return None
    if record["attempts"] >= len(policy["delays_seconds"]):
        record["state"] = "exhausted"
        return None
    delay = policy["delays_seconds"][record["attempts"]]
    finish = client.clock() + delay
    if finish >= client.request.effective_deadline:
        record["state"] = "deadline"
        return None
    record["state"] = "cooldown"
    client._recovery_waiting = True
    callback = getattr(client, "activity_event", None)
    if callable(callback):
        try:
            callback("模型處理中", f"模型暫時過載，等待 {delay} 秒後進行第 "
                     f"{record['attempts'] + 1} 次安全接續；已執行操作不會整批重跑。")
        except Exception:
            pass
    while client.clock() < finish:
        if client.cancelled():
            record["state"] = "cancelled"
            return None
        if client.clock() >= client.request.effective_deadline:
            record["state"] = "deadline"
            return None
        client.recovery_wait(min(1.0, finish - client.clock()))
    if client.cancelled() or client.clock() >= client.request.effective_deadline:
        record["state"] = "cancelled" if client.cancelled() else "deadline"
        return None
    # A new native turn is an explicit continuation, never a re-admission or
    # automatic repeat of the original user input/tool call sequence.
    record["attempts"] += 1
    record["state"] = "continuing"
    client.admissions += 1
    identifier = 2_000_000 + record["attempts"]
    message = turn_message(client.request, client.thread, cwd=client.cwd,
        permissions=client._permissions, effective_policy=client._effective_policy,
        reasoning_effort=client.reasoning_effort, service_tier=client.service_tier)
    message["id"] = identifier
    message["params"]["input"] = [{"type": "text", "text": CONTINUATION}]
    if client.isolated_provider_auth:
        message["params"]["environments"] = []
    client._recovery_seen_turns.add(client.turn)
    client.turn = None
    client.token_observation = None
    client._compaction_pending = None
    client._compaction_count = 0
    client._compaction_before = client._compaction_after = None
    client._recovery_waiting = False
    client._deferred_capacity_error = None
    client._diagnostic = None
    client._send(message)  # An uncertain write never authorizes another attempt.
    response, pending = client._response(identifier, (
        "turn/started", "turn/completed", "item/started", "item/completed",
        "item/agentMessage/delta"))
    native = response.get("turn")
    if type(native) is not dict or native.get("status") not in {
            "inProgress", "completed", "interrupted", "failed"}:
        raise ContractError("Codex recovery turn response invalid")
    client.turn = _identifier(native.get("id"), "backend recovery turn")
    if client.turn in client._recovery_seen_turns:
        raise ContractError("Codex recovery reused an earlier turn identity")
    client.accepted += 1
    if bridge is not None:
        bridge.turn = client.turn
    if client._approval_bridge is not None:
        client._approval_bridge.turn = client.turn
    return pending
