"""Route-bound scheduling through the same transaction as chat schedule commands."""

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity


class WorkspaceScheduleTools:
    def __init__(self, controls):
        self.controls = controls

    def invoke(self, workspace, arguments):
        import json

        try:
            if (type(arguments) is not dict or set(arguments) != {"expected", "command"}
                    or WorkspaceIdentity.from_dict(arguments["expected"]) != workspace.identity
                    or workspace.store is not self.controls.store
                    or type(arguments["command"]) is not str
                    or not 1 <= len(arguments["command"]) <= 8192):
                raise ContractError("schedule tool fields invalid")

            def apply(connection):
                from ahc.infrastructure.channel_schedule_commands import handle_schedule_command

                current = workspace.store._load_current(
                    connection, workspace.identity.session.ref.virtual_session_id)
                if current.session != workspace.identity.session or current.active_turn_id is None:
                    raise ContractError("schedule tool parent is not active")
                row = connection.execute(
                    "SELECT route_json FROM runtime_channel_routes WHERE operation_id=?",
                    (current.active_turn_id,)).fetchone()
                if row is None:
                    raise ContractError("schedule tool route unavailable")
                route = ChannelRoute.from_dict(json.loads(row[0]))
                if route not in self.controls.routes:
                    from ahc.infrastructure.background_schedule_dispatch import (
                        BackgroundScheduleDispatch,
                    )

                    route = BackgroundScheduleDispatch.origin_route(
                        connection, current.session, current.active_turn_id)
                if route not in self.controls.routes:
                    raise ContractError("schedule tool route is not enabled")
                reply = handle_schedule_command(
                    self.controls, connection, route, "/cron " + arguments["command"])
                if reply is None:
                    raise ContractError("schedule tool command invalid")
                return {"disposition": "observed", "unresolved_effects": False,
                        "result": {"reply": reply}}

            return workspace.store._mutate(apply)
        except ContractError:
            return {"disposition": "rejected", "unresolved_effects": False,
                    "reason": "schedule-request-invalid"}
