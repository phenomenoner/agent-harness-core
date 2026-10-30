"""Recognize retained ancestor events without resubmitting their historical work."""

from dataclasses import replace
from types import SimpleNamespace

from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json

from .channel_file_inbox import ChannelFileInbox
from .runtime_channel_store import _exists, _route
from .runtime_inbox import RuntimeInbox
from .runtime_recovery_access import recovery_allows
from .working_repository import WorkingSetCorrupt


def retained_recovery_event(store, connection, event):
    """Return a historical receipt only for exact content and verified recovery lineage."""
    original = None
    if _exists(connection, "runtime_channel_file_inbox"):
        row = connection.execute("SELECT * FROM runtime_channel_file_inbox WHERE operation_id=?",
                                 (event.operation_id,)).fetchone()
        if row is not None:
            original = ChannelFileInbox._decode(row)
    if original is None and _exists(connection, "runtime_submissions"):
        row = connection.execute("SELECT * FROM runtime_submissions WHERE operation_id=?",
                                 (event.operation_id,)).fetchone()
        if row is not None and _exists(connection, "runtime_channel_routes"):
            route_row = connection.execute("SELECT * FROM runtime_channel_routes "
                "WHERE operation_id=?", (event.operation_id,)).fetchone()
            if route_row is not None:
                route = _route(route_row)
                payload = RuntimeInbox._decode(row)
                if payload != RuntimeInbox._payload(route.session, event.operation_id,
                                                    payload["goal_digest"], payload["input_text"]):
                    raise WorkingSetCorrupt("historical channel submission mismatch")
                from .channel_context_store import read_context

                context = read_context(store, SimpleNamespace(
                    operation_id=event.operation_id, session=route.session))
                original = ChannelInboundEvent(route, route_row["event_id"], payload["input_text"],
                                               context=context)
    if original is None or original.route.session == event.route.session:
        return None
    projected = replace(original, route=replace(original.route, session=event.route.session))
    # Legacy rows lack enrichment. Compare their original immutable input rather than
    # retroactively attaching new metadata or replaying the recovered operation.
    comparable = replace(event, context=None) if original.context is None else event
    if (canonical_json(projected.to_dict()) != canonical_json(comparable.to_dict())
            or not recovery_allows(store, connection, event.route.session, original.route.session)):
        raise ContractError("historical channel event conflict")
    return {"operation_id": event.operation_id, "status": "historical-event-retained",
            "submitted": False, "automatic_replay": False}
