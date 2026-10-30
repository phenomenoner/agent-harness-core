"""Durable, route-bound admission for an already authenticated Gateway account."""

from ahc.adapters.channel_events import decode_channel_event
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_inbox import RuntimeInbox

from .channel_recovery_dedup import retained_recovery_event


class DiscordInbox:
    def __init__(self, store, routes, *, include_files=False):
        if type(include_files) is not bool:
            raise ContractError("Discord file admission option invalid")
        self.include_files = include_files
        routes = tuple(routes)
        if (
            not routes
            or any(type(r) is not ChannelRoute for r in routes)
            or any(r.platform != "discord" for r in routes)
            or len({r.account_id for r in routes}) != 1
            or len({(r.channel_id, r.user_id) for r in routes}) != len(routes)
        ):
            raise ContractError("Discord consumer routes invalid")
        self.store, self.routes = store, routes
        self.account_id = routes[0].account_id

    def __call__(self, frame, *, _transaction=None):
        controls = getattr(self.store, "channel_controls", None)
        if (
            controls is not None
            and controls.options.get("native_ui", False)
            and frame.get("t") == "INTERACTION_CREATE"
            and type(frame.get("op")) is int
            and frame["op"] == 0
        ):
            from .discord_control_interactions import admit_interaction

            def interact(connection):
                answers = []
                for route in self.routes:
                    current = self.store._load_current(
                        connection, route.session.ref.virtual_session_id
                    )
                    if current.session != route.session or current.status != "active":
                        raise ContractError("Discord interaction owner is not current")
                    answer = admit_interaction(controls, connection, route, frame.get("d", {}))
                    if answer is not None:
                        answers.append({"interaction_response": answer})
                return answers

            return (
                interact(_transaction) if _transaction is not None else self.store._mutate(interact)
            )
        events = [
            event
            for route in self.routes
            if (event := decode_channel_event(route, frame, include_files=self.include_files))
            is not None
        ]
        if not events:
            return []

        def write(connection):
            receipts = []
            for event in events:
                session = event.route.session
                current = self.store._load_current(connection, session.ref.virtual_session_id)
                if current.session != session or current.status != "active":
                    raise ContractError("Discord route is not current and active")
                retained = retained_recovery_event(self.store, connection, event)
                if retained is not None:
                    receipts.append(retained)
                    continue
                if event.files:
                    from .channel_file_inbox import ChannelFileInbox

                    receipts.append(
                        ChannelFileInbox(self.store).stage(
                            event, current.goal.digest, _transaction=connection
                        )
                    )
                    continue
                receipts.append(
                    RuntimeInbox(self.store).submit(
                        session,
                        event.operation_id,
                        current.goal.digest,
                        event.input_text,
                        channel_event=event,
                        _transaction=connection,
                    )
                )
            return receipts

        return write(_transaction) if _transaction is not None else self.store._mutate(write)
