"""Opt-in typing bound to the admitted operation and exact stored channel route."""

import os
import time
from uuid import uuid4

from ahc.adapters.channel_activity import BotActivityTransport
from ahc.adapters.channel_activity_worker import ChannelActivityWorker
from ahc.adapters.channel_typing import BotTypingTransport
from ahc.adapters.channel_typing_worker import ChannelTypingWorker
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_presentation import background_operation
from ahc.infrastructure.channel_progress_store import ChannelProgressStore
from ahc.infrastructure.runtime_channel_store import _route


def compose_typing(config, store):
    bindings = {}
    for row in config.to_dict().get("channels", []):
        if row.get("typing", False) or row.get("progress", False):
            route = ChannelRoute.from_dict(row["route"])
            token = os.environ.get(row["token_env"])
            if token is None:
                raise ContractError("typing credential unavailable")
            bindings[route] = (
                BotTypingTransport(route, token) if row.get("typing") else None,
                token,
                row.get("progress", False),
            )
    if not bindings:
        return None

    def for_turn(request, cancelled):
        if background_operation(store, request.operation_id):
            return None
        with store._read() as connection:
            row = connection.execute(
                "SELECT * FROM runtime_channel_routes WHERE operation_id=?", (request.operation_id,)
            ).fetchone()
            route = None if row is None else _route(row)
        if route not in bindings or route.session != request.session:
            return None

        def owns_card():
            with store._read() as connection:
                current = store._load_current(connection, request.session.ref.virtual_session_id)
                return (
                    current.session == request.session
                    and current.status == "active"
                    and current.active_turn_id == request.operation_id
                )

        def stopped():
            controls = getattr(store, "channel_controls", None)
            return cancelled() or (
                controls is not None and controls.cancelled(request.operation_id)
            )

        def can_send():
            return not stopped() and time.monotonic() < request.effective_deadline and owns_card()

        transport, token, activity = bindings[route]
        typing = ChannelTypingWorker(transport, can_send) if transport else None
        if not activity:
            return typing
        ledger, claim = ChannelProgressStore(store), uuid4().hex
        return ChannelActivityWorker(
            typing,
            BotActivityTransport(route, token),
            can_send,
            lambda text: ledger.reserve(
                request, route, "activity-status", text, claim, int(time.time() * 1000)
            ),
            lambda receipt: ledger.finish(request, route, "activity-status", claim, receipt),
            request.operation_id,
            can_finalize=owns_card,
            stop_requested=stopped,
        )

    return for_turn
