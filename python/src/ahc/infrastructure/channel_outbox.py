"""Local durable channel delivery ledger; it never calls a platform transport."""

import json

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.infrastructure.runtime_channel_store import (
    _exists,
    _failure_payload_matches,
    _matching_seal,
    _output,
    _route,
    _route_json,
)
from ahc.infrastructure.working_repository import WorkingSetConflict, WorkingSetCorrupt


def _receipt(value):
    if (
        type(value) is not str
        or not value
        or len(value) > 4096
        or any(ord(char) < 0x20 or 0x7F <= ord(char) <= 0x9F for char in value)
        or any(0xD800 <= ord(char) <= 0xDFFF for char in value)
    ):
        raise ContractError("channel receipt invalid")
    return value


class ChannelOutbox:
    def __init__(self, store):
        self.store = store

    @staticmethod
    def _payload(row):
        try:
            payload = json.loads(row["payload_json"])
            if (
                type(payload) is not dict
                or not {"output_text"} <= set(payload)
                or set(payload) - {"output_text", "reply_to_message_id"}
                or type(payload["output_text"]) is not str
                or row["payload_json"] != canonical_json(payload).decode()
            ):
                raise ValueError()
            if "reply_to_message_id" in payload:
                from ahc.domain.channel_reply import reply_message_id

                reply_message_id(payload["reply_to_message_id"])
            return payload
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, ContractError):
            raise WorkingSetCorrupt("channel outbox payload corrupted") from None

    def _record(self, connection, row, route):
        try:
            stored = ChannelRoute.from_dict(json.loads(row["route_json"]))
        except (TypeError, ValueError, json.JSONDecodeError, ContractError):
            raise WorkingSetCorrupt("channel outbox route corrupted") from None
        if stored != route or row["route_json"] != _route_json(route):
            raise ContractError("channel outbox route mismatch")
        parent = connection.execute(
            "SELECT * FROM runtime_turns WHERE operation_id=?", (row["operation_id"],)
        ).fetchone()
        intent = connection.execute(
            "SELECT * FROM runtime_intents WHERE operation_id=?", (row["operation_id"],)
        ).fetchone()
        route_row = connection.execute(
            "SELECT * FROM runtime_channel_routes WHERE operation_id=?", (row["operation_id"],)
        ).fetchone()
        if (
            parent is None
            or intent is None
            or route_row is None
            or parent["status"] != "terminal"
            or parent["session_json"] != canonical_json(route.session.to_dict()).decode()
            or intent["session_json"] != parent["session_json"]
            or intent["payload_json"] != parent["result_json"]
            or route_row["route_json"] != row["route_json"]
            or _route(route_row) != route
        ):
            raise WorkingSetCorrupt("channel outbox parent binding corrupted")
        payload = self._payload(row)
        try:
            result = json.loads(parent["result_json"])
            if type(result) is not dict:
                raise ValueError()
            if result.get("accepted") is True or result.get("report_accepted") is True:
                if (intent["kind"] != ("mailbox" if result.get("accepted") is True else "parked")
                        or payload != _output(result)
                        or not _matching_seal(connection, parent, result)):
                    raise ValueError()
            elif intent["kind"] != "parked" or not _failure_payload_matches(
                connection, route, row["operation_id"], result, payload
            ):
                # Existing durable notices retain the wording used at creation.
                # A presentation change cannot corrupt acknowledged/UNKNOWN rows.
                # Both recognized variants still bind to the same parent facts.
                raise ValueError()
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            raise WorkingSetCorrupt("channel outbox result corrupted") from None
        state, token, receipt = row["state"], row["claim_token"], row["receipt_json"]
        if (
            state not in ("pending", "claimed", "sent", "unknown")
            or (state == "claimed" and token is None)
            or (state != "claimed" and token is not None)
        ):
            raise WorkingSetCorrupt("channel outbox state corrupted")
        if token is not None:
            _identifier(token, "channel claim token")
        try:
            value = None if receipt is None else json.loads(receipt)
            if value is not None and receipt != canonical_json(_receipt(value)).decode():
                raise ValueError()
        except (TypeError, ValueError, json.JSONDecodeError, ContractError):
            raise WorkingSetCorrupt("channel outbox receipt corrupted") from None
        if state == "sent" and value is None:
            raise WorkingSetCorrupt("channel sent receipt unavailable")
        return {
            "operation_id": row["operation_id"],
            "route": route.to_dict(),
            "payload": payload,
            "state": state,
            "receipt": value,
        }

    def read(self, route):
        if type(route) is not ChannelRoute:
            raise ContractError("channel route invalid")
        with self.store._read() as connection:
            if not _exists(connection, "runtime_channel_outbox"):
                return ()
            rows = connection.execute(
                "SELECT * FROM runtime_channel_outbox WHERE route_json=? ORDER BY rowid",
                (_route_json(route),),
            ).fetchall()
            return tuple(self._record(connection, row, route) for row in rows)

    def delivery_scope(self):
        return self.store.execution_lock()

    def route_delivery_scope(self, route):
        if type(route) is not ChannelRoute:
            raise ContractError("channel delivery route invalid")
        return self.store.execution_lock(session=route.session,
            workspace_id="channel-delivery-" + digest_json(route.to_dict()))

    def claim(self, route, operation_id, token):
        if type(route) is not ChannelRoute:
            raise ContractError("channel route invalid")
        _identifier(operation_id, "channel operation")
        _identifier(token, "channel claim token")

        def write(connection):
            if not _exists(connection, "runtime_channel_outbox"):
                raise ContractError("channel outbox unavailable")
            row = connection.execute(
                "SELECT * FROM runtime_channel_outbox WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None:
                raise ContractError("channel outbox operation unavailable")
            self._record(connection, row, route)
            current = self.store._load_current(connection, route.session.ref.virtual_session_id)
            if current.session != route.session or current.status != "active":
                raise WorkingSetConflict("channel route is no longer current and active")
            if row["state"] != "pending":
                raise ContractError("channel outbox operation is not pending")
            connection.execute(
                "UPDATE runtime_channel_outbox SET state='claimed',claim_token=? "
                "WHERE operation_id=?",
                (token, operation_id),
            )
            row = connection.execute(
                "SELECT * FROM runtime_channel_outbox WHERE operation_id=?", (operation_id,)
            ).fetchone()
            record = self._record(connection, row, route)
            from .channel_presentation import operation_kind

            kind = operation_kind(connection, operation_id)
            result = json.loads(connection.execute(
                "SELECT result_json FROM runtime_turns WHERE operation_id=?", (operation_id,)
            ).fetchone()[0])
            if result.get("accepted") is not True:
                kind = "scheduled-reminder" if kind == "scheduled" else "reminder"
            # Derived only for this claimed delivery. Durable results, historical
            # payloads, seals and acknowledged/UNKNOWN receipts remain unchanged.
            return {**record, "message_type": kind}

        return self.store._mutate(write)

    def acknowledge(self, route, operation_id, token, receipt):
        if type(route) is not ChannelRoute:
            raise ContractError("channel route invalid")
        _identifier(operation_id, "channel operation")
        _identifier(token, "channel claim token")
        receipt = _receipt(receipt)

        def write(connection):
            row = connection.execute(
                "SELECT * FROM runtime_channel_outbox WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None or row["state"] != "claimed" or row["claim_token"] != token:
                raise ContractError("channel claim is unavailable")
            self._record(connection, row, route)
            connection.execute(
                "UPDATE runtime_channel_outbox SET state='sent',claim_token=NULL,receipt_json=? "
                "WHERE operation_id=?",
                (canonical_json(receipt).decode(), operation_id),
            )
            row = connection.execute(
                "SELECT * FROM runtime_channel_outbox WHERE operation_id=?", (operation_id,)
            ).fetchone()
            return self._record(connection, row, route)

        return self.store._mutate(write)

    def mark_unknown(self, route, operation_id, token, diagnostic="transport-exception"):
        if type(route) is not ChannelRoute:
            raise ContractError("channel route invalid")
        _identifier(operation_id, "channel operation")
        _identifier(token, "channel claim token")
        diagnostic = _receipt(diagnostic)

        def write(connection):
            row = connection.execute(
                "SELECT * FROM runtime_channel_outbox WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None or row["state"] != "claimed" or row["claim_token"] != token:
                raise ContractError("channel claim is unavailable")
            self._record(connection, row, route)
            connection.execute(
                "UPDATE runtime_channel_outbox SET state='unknown',claim_token=NULL,receipt_json=? "
                "WHERE operation_id=?",
                (canonical_json(diagnostic).decode(), operation_id),
            )
            row = connection.execute(
                "SELECT * FROM runtime_channel_outbox WHERE operation_id=?", (operation_id,)
            ).fetchone()
            return self._record(connection, row, route)

        return self.store._mutate(write)

    def reconcile(self, route):
        if type(route) is not ChannelRoute:
            raise ContractError("channel route invalid")

        def write(connection):
            if not _exists(connection, "runtime_channel_outbox"):
                return ()
            rows = connection.execute(
                "SELECT * FROM runtime_channel_outbox WHERE route_json=? ORDER BY rowid",
                (_route_json(route),),
            ).fetchall()
            for row in rows:
                self._record(connection, row, route)
            connection.execute(
                "UPDATE runtime_channel_outbox SET state='unknown',claim_token=NULL,receipt_json=? "
                "WHERE route_json=? AND state='claimed'",
                (canonical_json("restart-unreconciled").decode(), _route_json(route)),
            )
            rows = connection.execute(
                "SELECT * FROM runtime_channel_outbox WHERE route_json=? ORDER BY rowid",
                (_route_json(route),),
            ).fetchall()
            return tuple(self._record(connection, row, route) for row in rows)

        with self.route_delivery_scope(route):
            return self.store._mutate(write)
