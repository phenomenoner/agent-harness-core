"""Persist a bounded progress attempt before its external effect."""

import json

from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.turns import _sha256_hex

from .channel_outbox import _receipt
from .channel_presentation import operation_kind
from .runtime_channel_store import _exists, _route, _route_json
from .working_repository import WorkingSetCorrupt


class ChannelProgressStore:
    def __init__(self, store):
        self.store = store

    def reserve(self, request, route, call_id, text, token, now_ms):
        if type(now_ms) is not int or now_ms < 0:
            raise ContractError("progress time invalid")

        def write(connection):
            turn = connection.execute(
                "SELECT * FROM runtime_turns WHERE operation_id=?", (request.operation_id,)
            ).fetchone()
            binding = connection.execute(
                "SELECT * FROM runtime_channel_routes WHERE operation_id=?", (request.operation_id,)
            ).fetchone()
            current = self.store._load_current(connection, request.session.ref.virtual_session_id)
            if (turn is None or turn["status"] != "submitted"
                    or turn["request_digest"] != request_fingerprint(request)
                    or binding is None or _route(binding) != route
                    or current.session != request.session or current.status != "active"
                    or current.active_turn_id != request.operation_id):
                raise ContractError("progress reservation lost turn authority")
            if operation_kind(connection, request.operation_id) == "scheduled":
                return False
            rows = connection.execute(
                "SELECT call_id,attempted_ms FROM runtime_channel_progress WHERE operation_id=?",
                (request.operation_id,),
            ).fetchall()
            if any(row["call_id"] == call_id for row in rows):
                raise ContractError("progress call already attempted")
            if len(rows) >= 4 or (
                rows and now_ms - max(row["attempted_ms"] for row in rows) < 5000
            ):
                return False
            connection.execute(
                "INSERT INTO runtime_channel_progress VALUES(?,?,?,?,?,'claimed',?,NULL)",
                (request.operation_id, call_id, _route_json(route), digest_json({"text": text}),
                 now_ms, token),
            )
            return True

        return self.store._mutate(write)

    def finish(self, request, route, call_id, token, receipt):
        encoded = None if receipt is None else canonical_json(_receipt(receipt)).decode()

        def write(connection):
            row = connection.execute(
                "SELECT * FROM runtime_channel_progress WHERE operation_id=? AND call_id=?",
                (request.operation_id, call_id),
            ).fetchone()
            if (row is None or row["route_json"] != _route_json(route)
                    or row["state"] != "claimed" or row["claim_token"] != token):
                raise ContractError("progress claim unavailable")
            connection.execute(
                "UPDATE runtime_channel_progress SET state=?,claim_token=NULL,receipt_json=? "
                "WHERE operation_id=? AND call_id=?",
                ("unknown" if receipt is None else "sent", encoded, request.operation_id, call_id),
            )

        self.store._mutate(write)

    def _records(self, connection, route):
        if type(route) is not ChannelRoute:
            raise ContractError("progress route invalid")
        if not _exists(connection, "runtime_channel_progress"):
            return ()
        rows = connection.execute(
            "SELECT p.*,t.session_json FROM runtime_channel_progress p "
            "LEFT JOIN runtime_turns t ON t.operation_id=p.operation_id "
            "WHERE p.route_json=? ORDER BY p.rowid", (_route_json(route),),
        ).fetchall()
        records = []
        for row in rows:
            try:
                _identifier(row["operation_id"], "progress operation")
                _identifier(row["call_id"], "progress call")
                _sha256_hex(row["text_digest"])
                if row["session_json"] != canonical_json(route.session.to_dict()).decode():
                    raise ValueError()
                binding = connection.execute(
                    "SELECT * FROM runtime_channel_routes WHERE operation_id=?",
                    (row["operation_id"],),
                ).fetchone()
                if binding is None or _route(binding) != route:
                    raise ValueError()
                state, token = row["state"], row["claim_token"]
                if (state not in {"claimed", "sent", "unknown"}
                        or (state == "claimed") != (token is not None)
                        or type(row["attempted_ms"]) is not int or row["attempted_ms"] < 0):
                    raise ValueError()
                if token is not None:
                    _identifier(token, "progress claim")
                receipt = None if row["receipt_json"] is None else json.loads(row["receipt_json"])
                if (state == "sent") != (receipt is not None):
                    raise ValueError()
                if (receipt is not None
                        and row["receipt_json"] != canonical_json(_receipt(receipt)).decode()):
                    raise ValueError()
            except (KeyError, TypeError, ValueError, ContractError):
                raise WorkingSetCorrupt("progress record corrupted") from None
            records.append({"operation_id": row["operation_id"], "call_id": row["call_id"],
                            "text_digest": row["text_digest"], "attempted_ms": row["attempted_ms"],
                            "state": state, "receipt": receipt})
        return tuple(records)

    def read(self, route):
        with self.store._read() as connection:
            return self._records(connection, route)

    def reconcile(self, route):
        def write(connection):
            rows = self._records(connection, route)
            if rows:
                connection.execute(
                    "UPDATE runtime_channel_progress SET state='unknown',claim_token=NULL "
                    "WHERE route_json=? AND state='claimed'", (_route_json(route),),
                )
            return self._records(connection, route)

        from .channel_outbox import ChannelOutbox

        with ChannelOutbox(self.store).route_delivery_scope(route):
            return self.store._mutate(write)
