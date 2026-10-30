"""Immutable operation-bound reply/media context, compatible with legacy submissions."""

import json

from ahc.domain.channel_context import message_context
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json

from .runtime_channel_store import _exists, _route_json
from .working_repository import WorkingSetCorrupt

TABLE = """CREATE TABLE IF NOT EXISTS runtime_channel_context (
operation_id TEXT PRIMARY KEY, route_json TEXT NOT NULL,
context_json TEXT NOT NULL, context_digest TEXT NOT NULL)"""


def bind_context(connection, event):
    context = message_context(event.context)
    if context is None and not _exists(connection, "runtime_channel_context"):
        return
    connection.execute(TABLE)
    row = connection.execute("SELECT * FROM runtime_channel_context WHERE operation_id=?",
                             (event.operation_id,)).fetchone()
    encoded, route = canonical_json(context).decode(), _route_json(event.route)
    fingerprint = digest_json({"operation_id": event.operation_id, "route": route,
                               "context": context})
    if row is not None:
        if (row["route_json"] != route or row["context_json"] != encoded
                or row["context_digest"] != fingerprint):
            raise ContractError("channel context operation already bound")
        return
    if context is not None:
        connection.execute("INSERT INTO runtime_channel_context VALUES(?,?,?,?)",
                           (event.operation_id, route, encoded, fingerprint))


def read_context(store, request):
    with store._read() as connection:
        return context_for_operation(connection, request.session, request.operation_id)


def context_for_operation(connection, session, operation_id):
    if not _exists(connection, "runtime_channel_context"):
        return None
    row = connection.execute("SELECT * FROM runtime_channel_context WHERE operation_id=?",
                             (operation_id,)).fetchone()
    if row is None:
        return None
    try:
        route = ChannelRoute.from_dict(json.loads(row["route_json"]))
        context = message_context(json.loads(row["context_json"]))
        if (route.session != session or _route_json(route) != row["route_json"]
                or canonical_json(context).decode() != row["context_json"]
                or digest_json({"operation_id": operation_id,
                                "route": row["route_json"], "context": context})
                != row["context_digest"]):
            raise ValueError()
        return context
    except (ValueError, TypeError, KeyError, ContractError):
        raise WorkingSetCorrupt("channel context binding corrupted") from None
