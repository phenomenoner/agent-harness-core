"""Bounded, route-scoped conversation history from durable runtime records."""

import json
import re
from types import SimpleNamespace

from ahc.domain.sessions import ContractError, VirtualSessionIdentity, _identifier
from ahc.domain.skill_values import canonical_json

from .channel_outbox import ChannelOutbox
from .runtime_channel_store import _exists, _matching_seal, _route, _route_json
from .runtime_inbox import RuntimeInbox
from .working_repository import WorkingSetCorrupt

_SCAN_LIMIT = 128
_CHANNEL_OPERATION_ID = re.compile(r"channel-[0-9a-f]{64}\Z")


def _limits(max_turns, max_chars):
    if type(max_turns) is not int or not 1 <= max_turns <= 8:
        raise ContractError("conversation turn limit invalid")
    if type(max_chars) is not int or not 1 <= max_chars <= 16_000:
        raise ContractError("conversation character limit invalid")


def _reserved_channel_operation(operation_id):
    return _CHANNEL_OPERATION_ID.fullmatch(operation_id) is not None


def _submission(row):
    try:
        value = RuntimeInbox._decode(row)
        owner = VirtualSessionIdentity.from_dict(value["session"])
        expected = RuntimeInbox._payload(
            owner,
            row["operation_id"],
            value["goal_digest"],
            value["input_text"],
        )
        if value != expected:
            raise ValueError()
        return value
    except WorkingSetCorrupt:
        raise
    except (KeyError, TypeError, ValueError, ContractError):
        raise WorkingSetCorrupt("conversation submission corrupted") from None


def _terminal_result(connection, operation_id, owner):
    turn = connection.execute(
        "SELECT * FROM runtime_turns WHERE operation_id=?", (operation_id,)
    ).fetchone()
    if turn is None or turn["status"] == "submitted":
        return None
    if (
        turn["status"] != "terminal"
        or turn["session_json"] != canonical_json(owner.to_dict()).decode()
        or turn["result_json"] is None
    ):
        raise WorkingSetCorrupt("conversation terminal binding corrupted")
    intent = connection.execute(
        "SELECT * FROM runtime_intents WHERE operation_id=?", (operation_id,)
    ).fetchone()
    if intent is None:
        raise WorkingSetCorrupt("conversation terminal intent missing")
    try:
        result = json.loads(turn["result_json"])
        if (
            type(result) is not dict
            or turn["result_json"] != canonical_json(result).decode()
            or intent["kind"] not in ("mailbox", "parked")
            or intent["session_json"] != turn["session_json"]
            or intent["payload_json"] != turn["result_json"]
            or result.get("disposition")
            not in {"completed", "unknown", "unsupported", "not-submitted"}
            or type(result.get("accepted")) is not bool
            or type(result.get("unresolved_effects")) is not bool
            or type(result.get("output_text")) is not str
            or any(0xD800 <= ord(char) <= 0xDFFF for char in result["output_text"])
        ):
            raise ValueError()
    except (TypeError, ValueError, json.JSONDecodeError):
        raise WorkingSetCorrupt("conversation terminal result corrupted") from None
    if result["accepted"] and intent["kind"] != "mailbox":
        raise WorkingSetCorrupt("conversation accepted intent corrupted")
    if result.get("report_accepted") is True and (
        intent["kind"] != "parked" or not _matching_seal(connection, turn, result)
    ):
        raise WorkingSetCorrupt("conversation problem report seal corrupted")
    return result


def _fit_latest(entry, max_chars):
    """Compact optional facts before trimming text; never return an overflow."""
    entry["truncated"] = True
    recovery = entry.get("recovery")
    if recovery:
        compact = {"artifact_count": recovery["artifact_count"]}
        if recovery["workspace"] is not None:
            compact["workspace_revision"] = recovery["workspace"]["revision"]
        if recovery["checkpoint"] is not None:
            compact["checkpoint_saved"] = True
        entry["recovery"] = compact
    diagnostic = entry.get("backend_diagnostic")
    if diagnostic:
        compact = {"code": diagnostic["code"]}
        failure = diagnostic.get("backend_failure")
        if failure:
            compact["backend_failure"] = {
                key: failure[key] for key in ("kind", "http_status") if key in failure}
        entry["backend_diagnostic"] = compact
    while len(json.dumps(entry, ensure_ascii=False)) > max_chars:
        quote = entry.get("channel_message", {})
        nested = [key for key in ("quote", "forwarded_text") if len(quote.get(key, "")) > 256]
        if nested:
            largest = max(nested, key=lambda key: len(quote[key]))
            quote[largest] = quote[largest][:-256]
            quote["quote_status"] = "truncated"
            continue
        fields = [key for key in ("user", "assistant", "user_steering")
                  if len(entry.get(key, "")) > 1]
        if fields:
            largest = max(fields, key=lambda key: len(entry[key]))
            overflow = len(json.dumps(entry, ensure_ascii=False)) - max_chars
            entry[largest] = entry[largest][:max(1, len(entry[largest]) - overflow)]
            continue
        optional = next((key for key in ("channel_message", "recovery", "backend_diagnostic")
                         if key in entry), None)
        if optional is None:
            return False
        entry.pop(optional)
    return True


def conversation_context(
    store, session, operation_id, *, max_turns=4, max_chars=6000, include_incomplete=False
):
    """Return whole, confirmed earlier pairs for one exact channel route or local lane."""
    if type(session) is not VirtualSessionIdentity:
        raise ContractError("conversation session invalid")
    _identifier(operation_id, "conversation operation")
    _limits(max_turns, max_chars)
    if type(include_incomplete) is not bool:
        raise ContractError("conversation incomplete option invalid")
    owner_json = canonical_json(session.to_dict()).decode()

    with store._read() as connection:
        if not RuntimeInbox._exists(connection):
            raise ContractError("conversation submissions unavailable")
        current = connection.execute(
            "SELECT rowid, * FROM runtime_submissions WHERE operation_id=?", (operation_id,)
        ).fetchone()
        if current is None:
            raise ContractError("conversation operation unavailable")
        current_payload = _submission(current)
        if current_payload["session"] != session.to_dict():
            raise ContractError("conversation operation owner mismatch")

        channel_tables = _exists(connection, "runtime_channel_routes")
        current_route = None
        if channel_tables:
            route_row = connection.execute(
                "SELECT * FROM runtime_channel_routes WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if route_row is not None:
                current_route = _route(route_row)
                if current_route.session != session:
                    raise WorkingSetCorrupt("conversation route owner corrupted")
        if _reserved_channel_operation(operation_id) and current_route is None:
            raise WorkingSetCorrupt("conversation channel route missing")

        if current_route is not None:
            if not _exists(connection, "runtime_channel_outbox"):
                raise WorkingSetCorrupt("conversation channel outbox unavailable")
            after_rowid = 0
            controls = getattr(store, "channel_controls", None)
            if controls is not None:
                after_rowid = controls._preferences(connection, current_route)["history_after"]
            elif _exists(connection, "channel_preferences"):
                preference = connection.execute(
                    "SELECT payload_json FROM channel_preferences WHERE route_json=?",
                    (_route_json(current_route),),
                ).fetchone()
                if preference is not None:
                    after_rowid = json.loads(preference[0])["history_after"]
            rows = connection.execute(
                "SELECT s.rowid, s.*, r.route_json, r.event_id "
                "FROM runtime_submissions s JOIN runtime_channel_routes r "
                "ON r.operation_id=s.operation_id "
                "WHERE s.session_json=? AND s.rowid<? AND r.route_json=? AND s.rowid>? "
                "ORDER BY s.rowid DESC LIMIT ?",
                (
                    owner_json,
                    current["rowid"],
                    _route_json(current_route),
                    after_rowid,
                    _SCAN_LIMIT,
                ),
            )
        elif channel_tables:
            rows = connection.execute(
                "SELECT s.rowid, s.* FROM runtime_submissions s "
                "LEFT JOIN runtime_channel_routes r ON r.operation_id=s.operation_id "
                "WHERE s.session_json=? AND s.rowid<? AND r.operation_id IS NULL "
                "ORDER BY s.rowid DESC LIMIT ?",
                (owner_json, current["rowid"], _SCAN_LIMIT),
            )
        else:
            rows = connection.execute(
                "SELECT rowid, * FROM runtime_submissions "
                "WHERE session_json=? AND rowid<? ORDER BY rowid DESC LIMIT ?",
                (owner_json, current["rowid"], _SCAN_LIMIT),
            )
        outbox = ChannelOutbox(store)
        selected, used_chars = [], 0
        for row in rows:
            payload = _submission(row)
            if payload["session"] != session.to_dict():
                raise WorkingSetCorrupt("conversation submission owner corrupted")
            if current_route is None and _reserved_channel_operation(row["operation_id"]):
                continue

            if current_route is not None and _route(row) != current_route:
                raise WorkingSetCorrupt("conversation route binding corrupted")

            result = _terminal_result(connection, row["operation_id"], session)
            if result is None:
                continue  # Queued work is not an instruction to execute in this turn.
            assistant = (
                result["output_text"]
                if result.get("report_accepted") is True or (result["accepted"] is True
                and result["unresolved_effects"] is False
                and result.get("disposition") == "completed")
                else None
            )
            delivered = current_route is None and assistant is not None
            if current_route is not None and assistant is not None:
                outbox_row = connection.execute(
                    "SELECT * FROM runtime_channel_outbox WHERE operation_id=?",
                    (row["operation_id"],),
                ).fetchone()
                if outbox_row is not None:
                    record = outbox._record(connection, outbox_row, current_route)
                    delivered = record["state"] == "sent"
                    # _record already binds the full payload to the sealed result.
                    # Reply metadata and a cleanup notice do not replace the
                    # original assistant answer used for conversation history.
            if delivered:
                entry = {"user": payload["input_text"], "assistant": assistant}
                if result.get("report_accepted") is True:
                    entry.update(status=result["disposition"], report_accepted=True,
                                 unresolved_effects=result["unresolved_effects"])
            elif include_incomplete:
                stopped = (
                    _exists(connection, "channel_cancel")
                    and connection.execute(
                        "SELECT 1 FROM channel_cancel WHERE operation_id=?", (row["operation_id"],)
                    ).fetchone()
                    is not None
                )
                entry = {
                    "user": payload["input_text"],
                    "operation_id": row["operation_id"],
                    "status": result["disposition"],
                    "stop_requested": stopped,
                    "unresolved_effects": result["unresolved_effects"],
                    "answer_delivery": "unconfirmed",
                }
            else:
                continue
            if result.get("accepted") is not True or result.get("report_accepted") is True:
                from ahc.domain.backend_diagnostics import diagnostic_projection
                from ahc.domain.confirmed_work import confirmed_work_projection

                entry.update(diagnostic_projection(result.get("backend_diagnostic")))
                work = result.get("confirmed_work")
                if type(work) is dict:
                    workspace = work.get("workspace")
                    checkpoint = work.get("checkpoint")
                    origin = (workspace if type(workspace) is dict else
                              checkpoint.get("origin") if type(checkpoint) is dict else None)
                    if origin is None:
                        artifacts = work.get("artifacts")
                        if type(artifacts) is list and artifacts and type(artifacts[0]) is dict:
                            origin = artifacts[0].get("producer")
                    if type(origin) is dict:
                        validated = confirmed_work_projection(work, SimpleNamespace(
                            session=session, workspace_scope=origin.get("workspace_id")))
                        if validated:
                            facts = validated["confirmed_work"]
                            entry["recovery"] = {
                                "workspace": facts["workspace"], "checkpoint": facts["checkpoint"],
                                "artifact_count": len(facts["artifacts"]),
                            }
            from .channel_context_store import read_context

            context = read_context(store, SimpleNamespace(
                operation_id=row["operation_id"], session=session))
            if context:
                entry["channel_message"] = context
            if current_route is not None and _exists(connection, "channel_steering"):
                supplements = connection.execute(
                    "SELECT input_text FROM channel_steering WHERE target_operation_id=? "
                    "AND route_json=? AND state='sent' ORDER BY rowid DESC LIMIT 8",
                    (row["operation_id"], _route_json(current_route)),
                ).fetchall()
                if supplements:
                    entry["user_steering"] = "\n".join(item[0] for item in reversed(supplements))
            pair_chars = (
                len(json.dumps(entry, ensure_ascii=False))
                if (include_incomplete or context or "recovery" in entry
                    or "backend_diagnostic" in entry)
                else (len(payload["input_text"]) + len(assistant)
                      + len(entry.get("user_steering", "")))
            )
            if len(selected) >= max_turns:
                break
            if used_chars + pair_chars > max_chars:
                if not include_incomplete or selected or max_chars < 512:
                    break
                # A large latest pair must not silently turn a continuation into empty history.
                if not _fit_latest(entry, max_chars):
                    break
                pair_chars = len(json.dumps(entry, ensure_ascii=False))
            if used_chars + pair_chars > max_chars:
                break
            selected.append(entry)
            used_chars += pair_chars

    return tuple(reversed(selected))
