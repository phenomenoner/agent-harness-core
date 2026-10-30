"""Known recent references reconstructed from exact route-bound durable evidence."""

import json

from ahc.domain.channel_reply import reply_message_id
from ahc.domain.sessions import ContractError

from .channel_attachment_outbox import ChannelAttachmentOutbox
from .channel_context_store import context_for_operation
from .channel_outbox import ChannelOutbox
from .runtime_channel_store import _exists, _route, _route_json
from .runtime_inbox import RuntimeInbox
from .working_repository import WorkingSetCorrupt


def request_route(connection, session, operation_id):
    if not _exists(connection, "runtime_channel_routes"):
        raise ContractError("channel reply route unavailable")
    row = connection.execute("SELECT rowid,* FROM runtime_channel_routes WHERE operation_id=?",
                             (operation_id,)).fetchone()
    if row is None:
        raise ContractError("channel reply operation unavailable")
    route = _route(row)
    if route.session != session:
        raise ContractError("channel reply operation owner mismatch")
    return route, row["rowid"]


def known_messages(connection, session, operation_id):
    route, current = request_route(connection, session, operation_id)
    rows = connection.execute(
        "SELECT * FROM runtime_channel_routes WHERE route_json=? AND rowid<=? "
        "ORDER BY rowid DESC LIMIT 32", (_route_json(route), current)).fetchall()
    result, seen = [], set()

    def add(identifier, kind, text):
        try:
            reply_message_id(identifier)
        except ContractError:
            return  # Old events without a provider message ID are not reply targets.
        if identifier not in seen and len(result) < 20:
            seen.add(identifier)
            result.append({"message_id": identifier, "kind": kind, "excerpt": text[:200]})

    for row in rows:
        if _route(row) != route:
            raise WorkingSetCorrupt("reply catalog route corrupted")
        context = context_for_operation(connection, session, row["operation_id"])
        if context:
            submission = connection.execute(
                "SELECT * FROM runtime_submissions WHERE operation_id=?",
                (row["operation_id"],)).fetchone()
            if submission is None:
                raise WorkingSetCorrupt("reply catalog submission missing")
            payload = RuntimeInbox._decode(submission)
            if payload["session"] != session.to_dict():
                raise WorkingSetCorrupt("reply catalog submission owner mismatch")
            add(context.get("message_id"), "user", payload["input_text"])
            for index, member in enumerate(context.get("album_members", []), 1):
                positions = [position + 1 for position in member["attachment_indexes"]]
                add(member["message_id"], "user",
                    f"Album member {index}; attachment positions {positions}")
            if context.get("quote_status") in ("available", "truncated"):
                add(context.get("reply_to_id"), "quoted", context.get("quote", ""))
        if _exists(connection, "runtime_channel_outbox"):
            sent = connection.execute(
                "SELECT * FROM runtime_channel_outbox WHERE operation_id=? AND state='sent'",
                (row["operation_id"],)).fetchone()
            if sent is not None:
                record = ChannelOutbox(None)._record(connection, sent, route)
                prefix = f"{route.platform}:{route.channel_id}:"
                receipt = record["receipt"]
                if receipt.startswith(prefix):
                    add(receipt[len(prefix):], "assistant", record["payload"]["output_text"])
        if _exists(connection, "runtime_channel_attachments"):
            attachments = connection.execute(
                "SELECT * FROM runtime_channel_attachments WHERE operation_id=? AND state='sent' "
                "ORDER BY ordinal LIMIT 8", (row["operation_id"],)).fetchall()
            for attachment in attachments:
                record = ChannelAttachmentOutbox(None)._record(connection, attachment, route)
                try:
                    receipt = json.loads(record["receipt"])
                    add(receipt["message_id"], "assistant_attachment",
                        record["selection"].get("caption") or record["selection"]["filename"])
                except (ValueError, KeyError, TypeError):
                    raise WorkingSetCorrupt("reply catalog attachment receipt corrupted") from None
        if len(result) >= 20:
            break
    return result


def validate_reply(connection, session, operation_id, message_id):
    reply_message_id(message_id)
    if not any(row["message_id"] == message_id
               for row in known_messages(connection, session, operation_id)):
        raise ContractError("reply target is not a known message on this route")
    return message_id


class ChannelReplyTools:
    def __init__(self, store):
        self.store = store

    def invoke(self, workspace, name, arguments):
        from ahc.domain.workspace import WorkspaceIdentity

        from .runtime_execution_scope import workspace_request

        try:
            keys = {"expected"} | ({"message_id"} if name == "workspace.reply" else set())
            if type(arguments) is not dict or set(arguments) != keys:
                raise ContractError("reply tool fields invalid")
            expected = WorkspaceIdentity.from_dict(arguments["expected"])
            if workspace.store is not self.store or expected != workspace.identity:
                raise ContractError("reply tool workspace mismatch")
            request = workspace_request(self.store, expected.session, expected.workspace_id)
            if request is None:
                raise ContractError("reply tool requires an admitted operation")
            with self.store._read() as connection:
                if name == "workspace.channel_messages":
                    value = {"messages": known_messages(
                        connection, expected.session, request.operation_id)}
                elif name == "workspace.reply":
                    request_route(connection, expected.session, request.operation_id)
                    target = arguments["message_id"]
                    value = {"reply_to_message_id": None if target is None else validate_reply(
                        connection, expected.session, request.operation_id, target)}
                else:
                    raise ContractError("reply tool unsupported")
            return {"disposition": "observed", "unresolved_effects": False, "result": value}
        except ContractError:
            return {"disposition": "rejected", "unresolved_effects": False,
                    "reason": "reply-target-unavailable-use-channel-messages"}
