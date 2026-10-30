"""Durable, route-bound Telegram album buffering before task admission."""

import json
from dataclasses import replace
from hashlib import sha256

from ahc.adapters.channel_message_context import (
    ATTACHMENT_ONLY_PLACEHOLDER, MEDIA_ONLY_PLACEHOLDER, safe_attachment,
)
from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.working_repository import WorkingSetCorrupt

from .channel_file_inbox import ChannelFileInbox
from .runtime_inbox import RuntimeInbox

_TABLE = """CREATE TABLE IF NOT EXISTS runtime_telegram_albums (
route_json TEXT NOT NULL, group_id TEXT NOT NULL, topic_id TEXT NOT NULL,
goal_digest TEXT NOT NULL, events_json TEXT NOT NULL, seen_at INTEGER NOT NULL,
state TEXT NOT NULL, PRIMARY KEY(route_json,group_id,topic_id))"""
_QUIET_NS = 1_000_000_000
_PLACEHOLDERS = {ATTACHMENT_ONLY_PLACEHOLDER, MEDIA_ONLY_PLACEHOLDER}


class TelegramAlbumInputInvalid(ContractError):
    """A new remote member cannot satisfy the album's bounded input contract."""


def validate_album_input(event):
    if event is None or not (event.context or {}).get("media_group_id"):
        return
    identifier = event.context.get("message_id", "")
    if (not identifier.isascii() or not identifier.isdigit()
            or not 1 <= len(identifier) <= 128 or int(identifier) <= 0):
        raise TelegramAlbumInputInvalid("Telegram album member identifier invalid")


def _route(route):
    return canonical_json(route.to_dict()).decode()


def _events(row):
    try:
        raw = json.loads(row["events_json"])
        if type(raw) is not list or not 1 <= len(raw) <= 10:
            raise ValueError()
        events = [ChannelInboundEvent.from_dict(item) for item in raw]
        if (row["state"] not in {"pending", "admitted", "held"}
                or type(row["seen_at"]) is not int or row["seen_at"] < 0
                or canonical_json(raw).decode() != row["events_json"]
                or len({event.event_id for event in events}) != len(events)):
            raise ValueError()
        for event in events:
            context = event.context or {}
            message_id = context.get("message_id", "")
            if (_route(event.route) != row["route_json"]
                    or context.get("media_group_id") != row["group_id"]
                    or context.get("topic_id", "") != row["topic_id"]
                    or not message_id.isascii() or not message_id.isdigit()
                    or not 0 < int(message_id)):
                raise ValueError()
        if len({event.context["message_id"] for event in events}) != len(events):
            raise ValueError()
        return events
    except (KeyError, ValueError, TypeError, ContractError):
        raise WorkingSetCorrupt("Telegram album binding corrupted") from None


def _operation(event):
    group = event.context["media_group_id"]
    topic = event.context.get("topic_id", "")
    identifier = sha256(canonical_json([group, topic])).hexdigest()
    return replace(event, event_id="album-" + identifier).operation_id


def stage_album(connection, event, goal_digest, now):
    """Retain original member events atomically with the provider cursor."""
    context = event.context or {}
    group = context.get("media_group_id")
    if group is None:
        return None
    if type(now) is not int or now < 0:
        raise ContractError("Telegram album clock invalid")
    connection.execute(_TABLE)
    key = (_route(event.route), group, context.get("topic_id", ""))
    row = connection.execute(
        "SELECT * FROM runtime_telegram_albums WHERE route_json=? AND group_id=? AND topic_id=?",
        key,
    ).fetchone()
    if row is None:
        count = connection.execute(
            "SELECT count(*) FROM runtime_telegram_albums WHERE route_json=? AND state='pending'",
            (key[0],),
        ).fetchone()[0]
        if count >= 64:
            raise ContractError("pending Telegram album limit reached")
        connection.execute(
            "INSERT INTO runtime_telegram_albums VALUES(?,?,?,?,?,?,?)",
            (*key, goal_digest, canonical_json([event.to_dict()]).decode(), now, "pending"),
        )
    else:
        events = _events(row)
        previous = next((item for item in events if item.event_id == event.event_id), None)
        if previous is not None:
            if previous != event or row["goal_digest"] != goal_digest:
                raise ContractError("Telegram album member already bound")
            return {"operation_id": _operation(event), "status": row["state"]}
        if any(item.context["message_id"] == context["message_id"] for item in events):
            raise TelegramAlbumInputInvalid("Telegram album message identifier already bound")
        # An accepted task is immutable. A late member remains its own ordinary event.
        if row["state"] == "admitted":
            return None
        if len(events) == 10:
            if row["state"] == "held":
                connection.execute("INSERT OR IGNORE INTO channel_held VALUES(?)",
                                   (event.operation_id,))
            return None
        if row["goal_digest"] != goal_digest:
            raise ContractError("Telegram album goal changed")
        events.append(event)
        connection.execute(
            "UPDATE runtime_telegram_albums SET events_json=?,seen_at=? "
            "WHERE route_json=? AND group_id=? AND topic_id=?",
            (canonical_json([item.to_dict() for item in events]).decode(), now, *key),
        )
    return {"operation_id": _operation(event),
            "status": "held" if row is not None and row["state"] == "held" else "awaiting-album"}


def _compose(events):
    events = sorted(events, key=lambda event: int(event.context["message_id"]))
    first = events[0]
    files, members, descriptions, captions = [], [], [], []
    notice = None
    for event in events:
        indexes = []
        for item in event.files:
            prior = next((index for index, existing in enumerate(files)
                          if existing.file_id == item.file_id), None)
            if prior is None:
                prior = len(files)
                files.append(item)
            elif files[prior] != item:
                notice = "Album attachment metadata conflicts; please resend the files."
            indexes.append(prior)
        member = {"message_id": event.context["message_id"], "attachment_indexes": indexes}
        reply = event.context.get("reply_to_id", "")
        if reply.isascii() and reply.isdigit() and 0 < int(reply):
            member["reply_to_id"] = reply
        members.append(member)
        descriptions.extend(event.context.get("attachments", []))
        if event.input_text not in _PLACEHOLDERS and event.input_text not in captions:
            captions.append(event.input_text)
        if event.context.get("notice"):
            notice = event.context["notice"]
    text = "\n\n".join(captions) or first.input_text
    if len(files) > 8:
        notice = "This album exceeds the eight-attachment limit; please resend smaller groups."
    if len(text) > 4096:
        notice = "Combined album captions exceed 4096 characters; please send the text separately."
        text = "Please examine this album's attachment notice."
    if notice is not None:
        files = []
        for member in members:
            member["attachment_indexes"] = []
    context = dict(first.context)
    context["album_members"] = members
    if notice is not None:
        context["notice"] = notice
    if files:
        context["attachments"] = [
            safe_attachment(item.filename, item.media_type, "available")
            for item in files
        ]
    elif descriptions:
        context["attachments"] = descriptions[:8]
    # Keep member identity even when combined quote/forward context reaches its bound.
    for field in ("quote", "forwarded_text"):
        overflow = len(canonical_json(context).decode()) - 7800
        if overflow > 0 and field in context:
            marker = " [truncated]"
            available = max(1, len(context[field]) - overflow - len(marker))
            context[field] = context[field][:available] + marker
            if field == "quote":
                context["quote_status"] = "truncated"
    identifier = sha256(canonical_json(
        [first.context["media_group_id"], first.context.get("topic_id", "")]
    )).hexdigest()
    return replace(first, event_id="album-" + identifier, input_text=text,
                   files=tuple(files), context=context)


def flush_albums(store, connection, goals, now):
    """Seal quiet albums once; staged files and album state share one transaction."""
    if type(now) is not int or now < 0:
        raise ContractError("Telegram album clock invalid")
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_telegram_albums'"
    ).fetchone() is None:
        return []
    receipts = []
    for route, goal in goals.items():
        rows = connection.execute(
            "SELECT * FROM runtime_telegram_albums WHERE route_json=? AND state='pending' "
            "ORDER BY seen_at,rowid LIMIT 64", (_route(route),),
        ).fetchall()
        for row in rows:
            events = _events(row)
            if row["goal_digest"] != goal:
                continue  # Retain old-goal custody; never transfer it to the current goal.
            operation = _operation(events[0])
            if (connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                   "AND name='channel_held'").fetchone() is not None
                    and connection.execute("SELECT 1 FROM channel_held WHERE operation_id=?",
                                           (operation,)).fetchone() is not None):
                connection.execute(
                    "UPDATE runtime_telegram_albums SET state='held' "
                    "WHERE route_json=? AND group_id=? AND topic_id=?",
                    (row["route_json"], row["group_id"], row["topic_id"]),
                )
                continue
            # Clock rollback must not strand a durable album indefinitely.
            if 0 <= now - row["seen_at"] < _QUIET_NS:
                continue
            event = _compose(events)
            if event.files:
                receipt = ChannelFileInbox(store).stage(event, goal, _transaction=connection)
            else:
                receipt = RuntimeInbox(store).submit(
                    route.session, event.operation_id, goal, event.input_text,
                    channel_event=event, _transaction=connection,
                )
            connection.execute(
                "UPDATE runtime_telegram_albums SET state='admitted' "
                "WHERE route_json=? AND group_id=? AND topic_id=?",
                (row["route_json"], row["group_id"], row["topic_id"]),
            )
            receipts.append(receipt)
    return receipts


def hold_albums(connection, route):
    """Use the existing /new hold contract, without a parallel cancellation ledger."""
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_telegram_albums'"
    ).fetchone() is None:
        return
    rows = connection.execute(
        "SELECT * FROM runtime_telegram_albums WHERE route_json=? AND state='pending' "
        "ORDER BY rowid LIMIT 64", (_route(route),),
    ).fetchall()
    for row in rows:
        events = _events(row)
        connection.execute("INSERT OR IGNORE INTO channel_held VALUES(?)",
                           (_operation(events[0]),))
        connection.execute(
            "UPDATE runtime_telegram_albums SET state='held' "
            "WHERE route_json=? AND group_id=? AND topic_id=?",
            (row["route_json"], row["group_id"], row["topic_id"]),
        )
