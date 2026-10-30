"""Explicit channel progress capability, with persisted per-turn limits."""

import os

from ahc.adapters.channel_text import BotTextTransport
from ahc.application.channel_progress import ChannelProgress
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.runtime_channel_store import _route
from ahc.infrastructure.runtime_execution_scope import workspace_request


class ForegroundProgress:
    def __init__(self, store, bindings, cancelled=lambda: False):
        self.store, self.bindings = store, bindings
        self.cancelled = cancelled

    def invoke(self, identity, arguments):
        try:
            if type(arguments) is not dict or set(arguments) != {"expected", "call_id", "text"}:
                raise ContractError("progress fields invalid")
            if WorkspaceIdentity.from_dict(arguments["expected"]) != identity:
                raise ContractError("progress workspace stale")
            request = workspace_request(self.store, identity.session, identity.workspace_id)
            self.store.require_workspace(identity)
            with self.store._read() as connection:
                row = connection.execute(
                    "SELECT * FROM runtime_channel_routes WHERE operation_id=?",
                    (request.operation_id,),
                ).fetchone()
                route = None if row is None else _route(row)
            if route not in self.bindings:
                return {"disposition": "unsupported", "reason": "progress-route-disabled"}
            result = ChannelProgress(self.store, request, route, self.bindings[route],
                                     cancelled=self.cancelled).send(
                arguments["call_id"], arguments["text"])
            return {"disposition": "unknown" if result["status"] == "unknown" else "observed",
                    "result": result, "unresolved_effects": False}
        except ContractError:
            return {"disposition": "rejected", "reason": "progress-request-invalid",
                    "unresolved_effects": False}
        except Exception:
            return {"disposition": "unknown", "reason": "progress-delivery-unverified",
                    "unresolved_effects": False}


def compose_progress(config, store, *, cancelled=lambda: False):
    bindings = {}
    for row in config.to_dict().get("channels", []):
        if row.get("progress", False):
            token = os.environ.get(row["token_env"])
            if token is None:
                raise ContractError("progress credential unavailable")
            route = ChannelRoute.from_dict(row["route"])
            bindings[route] = BotTextTransport(route, token)
    return ForegroundProgress(store, bindings, cancelled) if bindings else None
