"""Durable attachment claims tied to the original accepted channel result."""

import json

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.channel_presentation import scheduled_silence_receipt
from ahc.domain.result_artifacts import ResultArtifact
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json

from .channel_outbox import ChannelOutbox, _receipt
from .channel_presentation import operation_kind
from .runtime_channel_store import _exists, _route_json
from .runtime_result_artifacts import validate_selected
from .working_repository import WorkingSetConflict, WorkingSetCorrupt


class ChannelAttachmentOutbox:
    def __init__(self, store):
        self.store = store

    def delivery_scope(self):
        return self.store.execution_lock()

    def route_delivery_scope(self, route):
        return ChannelOutbox(self.store).route_delivery_scope(route)

    @staticmethod
    def _key(route, operation_id, ordinal):
        if type(route) is not ChannelRoute:
            raise ContractError("attachment route invalid")
        _identifier(operation_id, "attachment operation")
        if type(ordinal) is not int or ordinal < 0:
            raise ContractError("attachment ordinal invalid")

    def _row(self, connection, operation_id, ordinal):
        row = connection.execute(
            "SELECT * FROM runtime_channel_attachments WHERE operation_id=? AND ordinal=?",
            (operation_id, ordinal),
        ).fetchone()
        if row is None:
            raise ContractError("attachment intent unavailable")
        return row

    def _record(self, connection, row, route):
        self._key(route, row["operation_id"], row["ordinal"])
        if row["route_json"] != _route_json(route):
            raise ContractError("attachment route mismatch")
        parent = connection.execute(
            "SELECT * FROM runtime_channel_outbox WHERE operation_id=?", (row["operation_id"],)
        ).fetchone()
        if parent is None:
            raise WorkingSetCorrupt("attachment parent outbox unavailable")
        parent_record = ChannelOutbox(self.store)._record(connection, parent, route)
        terminal = connection.execute(
            "SELECT result_json FROM runtime_turns WHERE operation_id=?", (row["operation_id"],)
        ).fetchone()
        try:
            selected = ResultArtifact.from_dict(json.loads(row["selection_json"]))
            result = json.loads(terminal["result_json"])
            if (row["selection_json"] != canonical_json(selected.to_dict()).decode()
                    or result["artifacts"][row["ordinal"]] != selected.to_dict()):
                raise ValueError()
            state, token, receipt = row["state"], row["claim_token"], row["receipt_json"]
            if (state not in {"pending", "claimed", "sent", "unknown"}
                    or (state == "claimed") != (token is not None)
                    or (state in {"sent", "unknown"}) != (receipt is not None)):
                raise ValueError()
            if token is not None:
                _identifier(token, "attachment claim")
            decoded = None if receipt is None else json.loads(receipt)
            if receipt is not None and receipt != canonical_json(_receipt(decoded)).decode():
                raise ValueError()
        except (KeyError, IndexError, TypeError, ValueError, ContractError):
            raise WorkingSetCorrupt("attachment intent corrupted") from None
        # Text claims derive categories from durable occurrence provenance too.
        # Do not infer a category from _record(), whose historical shape is untyped.
        kind = operation_kind(connection, row["operation_id"]) if result.get("accepted") is True else None
        suppressed = scheduled_silence_receipt({**parent_record, "message_type": kind})
        return {"operation_id": row["operation_id"], "ordinal": row["ordinal"],
                "route": route.to_dict(), "selection": selected.to_dict(),
                "state": state, "receipt": decoded,
                **({"suppression_receipt": suppressed} if suppressed is not None else {}),
                **({"reply_to_message_id": result["reply_to_message_id"]}
                   if "reply_to_message_id" in result else {})}

    def read(self, route):
        if type(route) is not ChannelRoute:
            raise ContractError("attachment route invalid")
        with self.store._read() as connection:
            if not _exists(connection, "runtime_channel_attachments"):
                return ()
            rows = connection.execute(
                "SELECT * FROM runtime_channel_attachments WHERE route_json=? ORDER BY rowid",
                (_route_json(route),),
            ).fetchall()
            return tuple(self._record(connection, row, route) for row in rows)

    def claim(self, route, operation_id, ordinal, token):
        self._key(route, operation_id, ordinal)
        _identifier(token, "attachment claim")

        def write(connection):
            row = self._row(connection, operation_id, ordinal)
            record = self._record(connection, row, route)
            current = self.store._load_current(connection, route.session.ref.virtual_session_id)
            if current.session != route.session or current.status != "active":
                raise WorkingSetConflict("attachment owner is no longer current and active")
            if record["state"] != "pending":
                raise ContractError("attachment intent is not pending")
            validate_selected(connection, route.session,
                              {"accepted": True, "artifacts": [record["selection"]]})
            connection.execute(
                "UPDATE runtime_channel_attachments SET state='claimed',claim_token=? "
                "WHERE operation_id=? AND ordinal=?", (token, operation_id, ordinal),
            )
            return self._record(connection, self._row(connection, operation_id, ordinal), route)

        return self.store._mutate(write)

    def _finish(self, route, operation_id, ordinal, token, state, receipt):
        self._key(route, operation_id, ordinal)
        _identifier(token, "attachment claim")
        receipt = _receipt(receipt)

        def write(connection):
            row = self._row(connection, operation_id, ordinal)
            self._record(connection, row, route)
            if row["state"] != "claimed" or row["claim_token"] != token:
                raise ContractError("attachment claim unavailable")
            connection.execute(
                "UPDATE runtime_channel_attachments SET state=?,claim_token=NULL,receipt_json=? "
                "WHERE operation_id=? AND ordinal=?",
                (state, canonical_json(receipt).decode(), operation_id, ordinal),
            )
            return self._record(connection, self._row(connection, operation_id, ordinal), route)

        return self.store._mutate(write)

    def acknowledge(self, route, operation_id, ordinal, token, receipt):
        return self._finish(route, operation_id, ordinal, token, "sent", receipt)

    def mark_unknown(self, route, operation_id, ordinal, token):
        return self._finish(route, operation_id, ordinal, token, "unknown", "delivery-unverified")

    def reconcile(self, route):
        if type(route) is not ChannelRoute:
            raise ContractError("attachment route invalid")

        def write(connection):
            if not _exists(connection, "runtime_channel_attachments"):
                return ()
            rows = connection.execute(
                "SELECT * FROM runtime_channel_attachments WHERE route_json=? ORDER BY rowid",
                (_route_json(route),),
            ).fetchall()
            for row in rows:
                self._record(connection, row, route)
            connection.execute(
                "UPDATE runtime_channel_attachments SET state='unknown',claim_token=NULL,"
                "receipt_json=? WHERE route_json=? AND state='claimed'",
                (canonical_json("restart-unreconciled").decode(), _route_json(route)),
            )
            return tuple(self._record(connection, self._row(
                connection, row["operation_id"], row["ordinal"]), route) for row in rows)

        with self.route_delivery_scope(route):
            return self.store._mutate(write)
