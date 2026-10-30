"""Bounded operator-visible action details from an already validated callback."""

import json

from ahc.domain.sessions import ContractError


def approval_summary(method, params):
    # Never silently truncate a permission request into an apparently narrower action.
    action = "command execution" if method.startswith("item/command") else "file changes"
    details = {
        key: value
        for key, value in params.items()
        if key not in {"threadId", "turnId", "itemId", "startedAtMs", "availableDecisions"}
        and value is not None
    }
    document = {"action": action, "details": details}
    if action == "file changes":
        document["scope_note"] = "This callback does not include a file diff."
    elif not params.get("command"):
        document["scope_note"] = "This callback does not include the command text."
    # ASCII JSON escapes terminal controls and Unicode direction overrides as data.
    summary = json.dumps(document, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    if len(summary) > 4096:
        raise ContractError("Codex approval details exceed the operator display bound")
    return summary
