"""Chat-facing schedule grammar, composed inside the caller's command transaction."""

import base64
import re
import struct
import time
import uuid

from ahc.domain.daily_schedules import DailySchedule
from ahc.domain.schedules import IntervalSchedule
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

from .channel_schedule_routes import ChannelScheduleRoutes
from .schedule_repository import ScheduleRepository

_EVERY = re.compile(r"([1-9][0-9]{0,4})([mh])$")
_DAILY = re.compile(r"([01][0-9]|2[0-3]):([0-5][0-9])$")


def _taipei_tzif():
    """Pinned fixed UTC+08 bytes, without consulting host timezone data."""
    raw = (b"TZif\0" + bytes(15) + struct.pack(">6i", 0, 0, 0, 0, 1, 4)
           + struct.pack(">iBB", 28_800, 0, 0) + b"CST\0")
    return base64.b64encode(raw).decode()


def _usage():
    return ("排程語法：/cron list；/cron add every 10m <工作>；"
            "/cron add daily HH:MM Asia/Taipei <工作>；"
            "/cron pause|resume|delete <ID>。")


def _parse(text):
    if type(text) is not str:
        raise ContractError("schedule command text invalid")
    source = text.lstrip()
    if not re.match(r"^/cron(?:\s|$)", source, re.IGNORECASE):
        return None, None
    normalized = source.strip()
    if normalized.lower() == "/cron":
        return None, _usage()
    if normalized.lower() == "/cron list":
        return ("list", None), None
    state = re.fullmatch(r"/cron\s+(pause|resume|delete)\s+(\S+)", normalized, re.IGNORECASE)
    if state is not None:
        return (state.group(1).lower(), state.group(2)), None
    every = re.match(r"^/cron\s+add\s+every\s+([1-9][0-9]{0,4}[mh])[ \t]+(.+)$", source,
                     re.IGNORECASE | re.DOTALL)
    if every is not None:
        match = _EVERY.fullmatch(every.group(1).lower())
        return ("every", (match, every.group(2))), None
    daily = re.match(
        r"^/cron\s+add\s+daily\s+([01][0-9]|2[0-3]):([0-5][0-9])\s+"
        r"(Asia/Taipei)[ \t]+(.+)$",
        source,
        re.IGNORECASE | re.DOTALL,
    )
    if daily is not None:
        match = _DAILY.fullmatch(daily.group(1) + ":" + daily.group(2))
        if daily.group(3) == "Asia/Taipei":
            return ("daily", (match, daily.group(4))), None
    return None, _usage()


def _reply(value):
    if value.state == "removed":
        return f"排程 {value.schedule_id} 已刪除（保留記錄，不再執行）。"
    if value.state == "paused":
        return f"排程 {value.schedule_id} 已暫停。"
    return f"排程 {value.schedule_id} 已啟用。"


def _list(connection, route):
    if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                          "AND name='runtime_channel_schedules'").fetchone() is None:
        return "本聊天室沒有排程。"
    rows = connection.execute(
        "SELECT s.* FROM runtime_schedules s JOIN runtime_channel_schedules r "
        "USING(schedule_id) WHERE s.session_json=? AND r.route_json=? "
        "ORDER BY schedule_id LIMIT 51",
        (canonical_json(route.session.to_dict()).decode(),
         canonical_json(route.to_dict()).decode()),
    ).fetchall()
    if not rows:
        return "本聊天室沒有排程。"
    values = tuple(ScheduleRepository._decode(row) for row in rows[:50])
    lines = ["本聊天室排程："]
    for value in values:
        cadence = (f"每 {value.interval_ms // 60_000} 分鐘" if type(value) is IntervalSchedule
                   else f"每日 {value.hour:02d}:{value.minute:02d} {value.timezone}")
        preview = value.input_text[:160] + ("…" if len(value.input_text) > 160 else "")
        lines.append(f"{value.schedule_id} · {value.state} · {cadence} · {preview}")
    if len(rows) > 50:
        lines.append("僅顯示前 50 筆。")
    return "\n".join(lines)


def handle_schedule_command(controls, connection, route, text, *, now_ms=None):
    """Handle `/cron` inside ChannelControlStore's existing SQL transaction.

    A non-schedule command returns ``None`` so the shared dispatcher can continue.
    """
    parsed, error = _parse(text)
    if parsed is None:
        return error
    store = getattr(controls, "store", None)
    if store is None:
        raise ContractError("schedule command store unavailable")
    current = store._load_current(connection, route.session.ref.virtual_session_id)
    if current.session != route.session or current.status != "active":
        raise ContractError("schedule route is not current and active")
    action, detail = parsed
    schedules = ScheduleRepository(store)
    if action == "list":
        return _list(connection, route)
    if action in {"pause", "resume", "delete"}:
        try:
            value = schedules.read(route.session, detail, _transaction=connection)
        except ContractError:
            return "排程 ID 無效或不屬於本聊天室；未變更。"
        if value is None:
            return "找不到此排程；未變更。"
        if ChannelScheduleRoutes.route_for(connection, value) != route:
            return "排程 ID 不屬於本聊天室；未變更。"
        try:
            state = {"pause": "paused", "resume": "active", "delete": "removed"}[action]
            return _reply(schedules.set_state(value, state, _transaction=connection))
        except ContractError:
            return "此排程目前不能變更；未變更。"
    now = time.time_ns() // 1_000_000 if now_ms is None else now_ms
    if type(now) is not int or now < 1:
        raise ContractError("schedule command clock invalid")
    schedule_id = "chat-" + uuid.uuid4().hex
    match, task = detail
    connection.execute("SAVEPOINT chat_schedule_create")
    try:
        if action == "every":
            amount, unit = int(match.group(1)), match.group(2)
            interval = amount * (60_000 if unit == "m" else 3_600_000)
            value = IntervalSchedule(schedule_id, route.session, current.goal.digest,
                                     now + interval, interval, task)
        else:
            value = DailySchedule(schedule_id, route.session, current.goal.digest, now, task,
                                  "Asia/Taipei", int(match.group(1)), int(match.group(2)),
                                  _taipei_tzif())
        schedules.create(value, _transaction=connection)
        ChannelScheduleRoutes.bind(connection, value, route)
    except ContractError:
        connection.execute("ROLLBACK TO chat_schedule_create")
        connection.execute("RELEASE chat_schedule_create")
        return "排程內容無效或目前無法建立；未變更。"
    connection.execute("RELEASE chat_schedule_create")
    cadence = (f"每 {value.interval_ms // 60_000} 分鐘" if type(value) is IntervalSchedule
               else f"每日 {value.hour:02d}:{value.minute:02d} {value.timezone}")
    return f"已建立排程 {value.schedule_id}：{cadence}；將回覆至本聊天室。"


__all__ = ["handle_schedule_command"]
