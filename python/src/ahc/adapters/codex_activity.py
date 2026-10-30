"""Public commentary and generic activity only; never reasoning or tool payloads."""


def observe_activity(client, event):
    callback = getattr(client, "activity_event", None)
    if not callable(callback) or type(event) is not dict:
        return
    params = event.get("params")
    if (type(params) is not dict or params.get("threadId") != client.thread
            or params.get("turnId") != client.turn):
        return
    item = params.get("item")
    if type(item) is not dict:
        return
    phase, text = None, None
    if event.get("method") == "item/completed" and item.get("type") == "agentMessage":
        if item.get("phase") == "commentary" and item.get("delivery") != "async":
            phase, text = "模型處理中", item.get("text")
    elif event.get("method") == "item/started":
        if item.get("type") in {"commandExecution", "dynamicToolCall", "mcpToolCall", "fileChange"}:
            phase = "工具執行中"
        elif item.get("type") == "agentMessage" and item.get("phase") == "final_answer":
            phase = "整理回覆中"
    if phase is not None:
        try:
            callback(phase, text)
        except Exception:
            pass  # Presentation is not acceptance evidence.
