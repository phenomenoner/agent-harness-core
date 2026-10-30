"""Durable notices for channel work retained while admission is unavailable."""

from ahc.domain.skill_values import canonical_json, digest_json

from .runtime_channel_store import _exists, _route, _route_json
from .working_repository import WorkingSetNotFound

_MAX_NOTICES = 64

_REASON_TEXT = {
    "backend-cleanup-pending": "後端清理尚未完成。",
    "backend-cleanup-exhausted": "後端清理仍未完成。",
    "workspace-unavailable": "工作區目前不可用。",
    "runtime-not-running": "執行服務目前未運行。",
    "receiver-unavailable": "訊息接收器目前不可用。",
    "observation-unavailable": "目前無法取得可用性觀測。",
    "recovery-pending": "復原目前仍在處理中。",
    "recovery-failed": "復原未成功。",
    "recovery-exhausted": "復原已達嘗試上限。",
}

_REASONS = frozenset(_REASON_TEXT)


def _notice_reason(observation):
    if type(observation) is dict and observation.get("ready") is True:
        return None
    if type(observation) is not dict:
        return "observation-unavailable"
    reason = observation.get("reason")
    return reason if type(reason) is str and reason in _REASONS else "observation-unavailable"


def _notice_text(reason):
    return (
        "⚠ 目前無法開始這則工作。\n"
        f"訊息已保留，尚未執行；{_REASON_TEXT[reason]}\n"
        "請用 /status 查看狀態。"
    )


def _notice_id(operation_id, route_json, reason):
    identity = {"operation": operation_id, "route": route_json, "reason": reason}
    route_digest = digest_json({"route": route_json})
    return (
        f"availability-{operation_id}-{route_digest}-{reason}",
        digest_json(identity),
    )


def queue_unavailable_notices(controls, observation):
    """Queue one fixed notice for each eligible pending channel submission.

    The caller owns delivery.  This function only performs one bounded database
    mutation and never changes submission or turn state.
    """

    reason = _notice_reason(observation)
    if reason is None:
        return 0

    configured_session = getattr(getattr(controls, "config", None), "session", None)
    if configured_session is None:
        return 0
    configured = {}
    for route in getattr(controls, "routes", ()):
        if route.session != configured_session:
            continue
        route_json = _route_json(route)
        configured[route_json] = route

    reply = _notice_text(reason)

    def write(connection):
        # A complete runtime schema is required before an unavailable-admission
        # notice can be justified.  In particular, absence of runtime_turns or
        # channel_held would make the no-replay selection incomplete.
        if not configured or not all(
            _exists(connection, table)
            for table in ("channel_controls", "runtime_submissions",
                          "runtime_channel_routes", "runtime_turns", "channel_held")
        ):
            return 0

        inserted = 0
        for route_json, route in sorted(configured.items()):
            if inserted >= _MAX_NOTICES:
                break
            try:
                current = controls.store._load_current(
                    connection, route.session.ref.virtual_session_id
                )
            except WorkingSetNotFound:
                continue
            if current.session != route.session or current.status != "active":
                continue
            session_json = canonical_json(current.session.to_dict()).decode()
            rows = connection.execute(
                "SELECT s.operation_id, s.session_json, s.goal_digest, "
                "r.route_json, r.event_id "
                "FROM runtime_submissions AS s "
                "JOIN runtime_channel_routes AS r ON r.operation_id=s.operation_id "
                "WHERE r.route_json=? AND s.session_json=? AND s.goal_digest=? "
                "AND NOT EXISTS (SELECT 1 FROM runtime_turns AS t "
                "WHERE t.operation_id=s.operation_id) "
                "AND NOT EXISTS (SELECT 1 FROM channel_held AS h "
                "WHERE h.operation_id=s.operation_id) "
                "AND NOT EXISTS (SELECT 1 FROM channel_controls AS n "
                "WHERE n.operation_id=('availability-' || s.operation_id || '-' || ? || '-' || ?)) "
                "ORDER BY s.rowid LIMIT ?",
                (route_json, session_json, current.goal.digest,
                 digest_json({"route": route_json}), reason, _MAX_NOTICES - inserted),
            ).fetchall()
            for row in rows:
                if inserted >= _MAX_NOTICES:
                    break
                # The route helper verifies canonical JSON and the operation/event
                # binding before any user-facing control row is written.
                if _route(row) != route:
                    continue
                notice_id, input_digest = _notice_id(row["operation_id"], route_json, reason)
                cursor = connection.execute(
                    "INSERT OR IGNORE INTO channel_controls "
                    "(operation_id,route_json,input_digest,reply,state,receipt,restart) "
                    "VALUES(?,?,?,?,'pending',NULL,0)",
                    (notice_id, route_json, input_digest, reply),
                )
                inserted += int(cursor.rowcount == 1)
        return inserted

    return controls.store._mutate(write)


__all__ = ["queue_unavailable_notices"]
