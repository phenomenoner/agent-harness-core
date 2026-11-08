"""Public message categories are transport presentation, not task acceptance."""

from .sessions import ContractError

_LABELS = {"reply": ("💬", "回覆"), "scheduled": ("⏰", "定時任務"),
           "background": ("⚙️", "背景任務"), "reminder": ("🔔", "任務提醒"),
           "scheduled-reminder": ("⏰", "定時任務提醒"), "progress": ("⏳", "互動進度")}


def message_heading(kind):
    if kind not in _LABELS:
        raise ContractError("channel message type invalid")
    icon, label = _LABELS[kind]
    return f"{icon} **{label}**"


def typed_payload(payload, kind):
    heading = message_heading(kind)
    text = payload["output_text"]
    if kind in {"reminder", "scheduled-reminder"}:
        if text.startswith("⚠ 任務未完成\n"):
            text = text.replace("⚠ 任務未完成\n", "⚠️ **任務未完成**\n", 1)
        for label in ("中斷原因：", "建議下一步："):
            text = text.replace("\n" + label, "\n**" + label + "**", 1)
    return {**payload, "output_text": f"{heading}\n\n{text}"}


def text_document_caption(text):
    """Keep an existing presentation heading visible when full text becomes a file."""
    heading = text.partition("\n")[0]
    for icon, label in _LABELS.values():
        if heading in {f"{icon} **{label}**", f"[{label}]"}:
            return heading.replace("**", "") + "\n完整回覆"
    return "完整回覆"


def scheduled_silence_receipt(record):
    """One exact accepted-result policy for text and selected attachments."""
    if (record.get("message_type") == "scheduled"
            and record["payload"]["output_text"] == "NO_REPLY"):
        return "suppressed:scheduled:no-reply"
    return None
