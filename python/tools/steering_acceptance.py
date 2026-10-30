"""Pure, read-only evidence reduction for a marked native steering test.

This does not send steering or establish authority to do so. Callers must still
bind the observed thread to their exact active runtime operation before sending.
Model prose and admission alone cannot establish that a shell process is running.
"""

import json
import re


def terminal_progress(events, marker, command):
    """Return bounded evidence from one marked task in one native rollout."""
    if not marker or not command:
        raise ValueError("A nonempty test marker and command are required")
    result = {
        "marked": False,
        "thread": None,
        "session_id": None,
        "terminal": False,
        "wait_called": False,
        "exit_zero": False,
        "exec_calls": 0,
    }
    calls = {}
    cells = set()
    nested_waits = set()
    for event in events:
        payload = event.get("payload", {})
        kind = payload.get("type")
        if event.get("type") == "session_meta":
            result["thread"] = payload.get("id")
        user_message = event.get("type") == "event_msg" and kind == "user_message"
        user_item = (
            event.get("type") == "response_item"
            and kind == "message"
            and payload.get("role") == "user"
        )
        if user_message or user_item:
            text = (
                payload.get("message", "")
                if user_message
                else json.dumps(payload.get("content", []))
            )
            if marker in text:
                result.update(
                    marked=True,
                    session_id=None,
                    terminal=False,
                    wait_called=False,
                    exit_zero=False,
                    exec_calls=0,
                )
                calls.clear()
                cells.clear()
                nested_waits.clear()
        if not result["marked"]:
            continue
        if event.get("type") == "event_msg" and kind in ("task_complete", "turn_aborted"):
            result["terminal"] = True
        if event.get("type") != "response_item":
            continue
        call_id = payload.get("call_id")
        if not call_id:
            continue
        if kind in ("custom_tool_call", "function_call"):
            raw = str(payload.get("input", payload.get("arguments", "")))
            name = str(payload.get("name", ""))
            if name == "wait":
                try:
                    cell = json.loads(raw).get("cell_id")
                except (ValueError, AttributeError):
                    cell = None
                if cell in cells:
                    calls[call_id] = "cell"
                continue
            is_exec = command in raw and ("exec_command" in raw or name == "exec_command")
            is_wait = bool(
                result["session_id"]
                and re.search(
                    r"""session_id["']?\s*:\s*["']?"""
                    + re.escape(result["session_id"])
                    + r"""(?:["'}\s,]|$)""",
                    raw,
                )
                and ("write_stdin" in raw or name == "write_stdin")
            )
            if is_exec or is_wait:
                result["exec_calls"] += int(is_exec)
                calls[call_id] = "exec" if is_exec else "wait"
                result["wait_called"] |= is_wait
                if is_exec and "write_stdin" in raw:
                    nested_waits.add(call_id)
        elif kind in ("custom_tool_call_output", "function_call_output") and call_id in calls:
            output = payload.get("output", "")
            text = output if isinstance(output, str) else json.dumps(output)
            text = text.replace('\\"', '"')
            cell = re.search(r"Script running with cell ID ([a-zA-Z0-9_-]+)", text)
            if cell and calls[call_id] == "exec":
                cells.add(cell[1])
            session = re.search(
                r"""(?:session_id["']?\s*[:=]\s*["']?|Process running with session ID\s+)(\d+)""",
                text,
            )
            if calls[call_id] == "exec" and session:
                result["session_id"] = session[1]
                result["running_observed_at"] = event.get("timestamp")
                if call_id in nested_waits:
                    # The controlled script prints both exec and wait results.
                    # Repeated session outputs distinguish a wait from call prose.
                    same_session = re.findall(
                        r"""session_id["']?\s*[:=]\s*["']?"""
                        + re.escape(session[1])
                        + r"""(?:["'}\s,]|$)""",
                        text,
                    )
                    result["wait_called"] |= len(same_session) >= 2
            if re.search(r"""(?:exit_code["']?\s*[:=]\s*|exit code\s+)0\b""", text):
                result["exit_zero"] = True
    result["ready"] = bool(
        result["marked"]
        and result["thread"]
        and result["session_id"]
        and result["exec_calls"] == 1
        and not result["terminal"]
        and not result["exit_zero"]
    )
    return result
