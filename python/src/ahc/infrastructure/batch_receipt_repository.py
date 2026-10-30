"""Durable read-batch admission and metadata-only receipts in the runtime store."""

import json

from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.turns import _sha256_hex


def _decode(row):
    _sha256_hex(row["input_digest"])
    _identifier(row["call_id"], "batch call")
    if row["receipt_json"] is None:
        value = {"call_id": row["call_id"], "state": "unknown",
                 "reason": "completion-unverified"}
    else:
        try:
            value = json.loads(row["receipt_json"])
            if (type(value) is not dict or set(value) != {"call_id", "state", "reason"}
                    or value["call_id"] != row["call_id"]
                    or value["state"] not in ("success", "rejected", "failure", "unknown")
                    or value["reason"] not in (None, "not-started", "admission-unavailable",
                        "capability-unknown", "capability-not-granted", "resource-not-owned",
                        "effect-mismatch", "approval-required", "result-too-large",
                        "invalid-tool-request", "completion-unverified")
                    or canonical_json(value).decode() != row["receipt_json"]):
                raise ValueError()
        except (ValueError, TypeError):
            raise ContractError("batch receipt corrupted") from None
    return {"input_digest": row["input_digest"], **value}


class BatchReceiptRepository:
    def __init__(self, store):
        self.store = store

    def begin(self, request, calls):
        def write(connection):
            row = connection.execute("SELECT * FROM runtime_turns WHERE operation_id=?",
                                     (request.operation_id,)).fetchone()
            if (row is None or row["status"] != "submitted"
                    or row["session_json"] != canonical_json(request.session.to_dict()).decode()
                    or row["request_digest"] != request_fingerprint(request)):
                raise ContractError("batch parent binding invalid")
            for call in calls:
                if connection.execute(
                    "SELECT 1 FROM runtime_batch_receipts WHERE operation_id=? AND call_id=?",
                    (request.operation_id, call.call_id),
                ).fetchone() is not None:
                    raise ContractError("durable batch call cannot be replayed")
            for call in calls:
                digest = digest_json({"capability": call.capability_id,
                                      "resource": call.resource_id,
                                      "arguments": json.loads(call.arguments_json)})
                connection.execute("INSERT INTO runtime_batch_receipts VALUES(?,?,?,NULL)",
                                   (request.operation_id, call.call_id, digest))
        self.store._mutate(write)

    def finish(self, request, call_id, state, reason):
        receipt = {"call_id": call_id, "state": state, "reason": reason}
        if state not in ("success", "rejected", "failure", "unknown"):
            raise ContractError("batch receipt state invalid")

        def write(connection):
            parent = connection.execute(
                "SELECT request_digest FROM runtime_turns WHERE operation_id=?",
                (request.operation_id,),
            ).fetchone()
            if parent is None or parent["request_digest"] != request_fingerprint(request):
                raise ContractError("batch receipt parent mismatch")
            changed = connection.execute(
                "UPDATE runtime_batch_receipts SET receipt_json=? "
                "WHERE operation_id=? AND call_id=? AND receipt_json IS NULL",
                (canonical_json(receipt).decode(), request.operation_id, call_id),
            ).rowcount
            if changed != 1:
                raise ContractError("batch receipt is absent or already completed")
        self.store._mutate(write)

    def read(self, session, operation_id):
        _identifier(operation_id, "batch parent")
        with self.store._read() as connection:
            parent = connection.execute(
                "SELECT session_json FROM runtime_turns WHERE operation_id=?", (operation_id,),
            ).fetchone()
            if parent is None:
                return ()
            if parent["session_json"] != canonical_json(session.to_dict()).decode():
                raise ContractError("batch receipt owner mismatch")
            rows = connection.execute(
                "SELECT * FROM runtime_batch_receipts WHERE operation_id=? ORDER BY rowid",
                (operation_id,),
            ).fetchall()
            return tuple(_decode(row) for row in rows)
