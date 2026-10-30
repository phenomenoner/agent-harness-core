"""Durable local child ledger; it does not dispatch or authorize a worker."""

import json
import time

from ahc.domain.child_result import ChildResult, normalize_child_result
from ahc.domain.child_work import ChildParent, ChildWorkEnvelope
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json
from ahc.infrastructure.working_repository import WorkingSetConflict, WorkingSetCorrupt


class ChildWorkRepository:
    def __init__(self, store, *, clock=lambda: time.time_ns() // 1_000_000):
        if not callable(clock):
            raise ContractError("child clock invalid")
        self.store, self.clock = store, clock
    def _now(self):
        value = self.clock()
        if type(value) is not int or value <= 0:
            raise ContractError("child clock invalid")
        return value

    @staticmethod
    def _parent_json(parent):
        return canonical_json(parent.session.to_dict()).decode()
    def _require_parent(self, connection, parent):
        if type(parent) is not ChildParent:
            raise ContractError("child parent invalid")
        current = self.store._load_current(connection, parent.session.ref.virtual_session_id)
        if (
            current.session != parent.session
            or current.status != "active"
            or current.active_turn_id != parent.operation_id
        ):
            raise WorkingSetConflict("child parent is not current and active")
        row = connection.execute(
            "SELECT * FROM runtime_turns WHERE operation_id=?", (parent.operation_id,)
        ).fetchone()
        if (
            row is None
            or row["status"] != "submitted"
            or row["request_digest"] != parent.request_digest
            or row["session_json"] != self._parent_json(parent)
        ):
            raise WorkingSetConflict("child parent turn is not admitted")

    @staticmethod
    def _decode(row):
        try:
            envelope = ChildWorkEnvelope.from_dict(json.loads(row["envelope_json"]))
            if (
                row["child_id"] != envelope.child_id
                or row["session_json"] != ChildWorkRepository._parent_json(envelope.parent)
                or row["parent_operation_id"] != envelope.parent.operation_id
                or row["request_digest"] != envelope.request_digest
                or row["envelope_json"] != canonical_json(envelope.to_dict()).decode()
                or row["state"] not in ("queued", "claimed", "cancelled", "terminal", "unknown")
                or (row["state"] in ("claimed", "terminal") and row["claim_token"] is None)
                or (
                    row["state"] in ("queued", "cancelled", "unknown")
                    and row["claim_token"] is not None
                )
            ):
                raise ValueError()
            if row["claim_token"] is not None:
                from ahc.domain.sessions import _identifier
                _identifier(row["claim_token"], "child claim token")
            result = None if row["result_json"] is None else json.loads(row["result_json"])
            if result is not None and (
                type(result) is not dict
                or ChildResult.from_dict(result).to_dict() != result
                or row["result_json"] != canonical_json(result).decode()
            ):
                raise ValueError()
            if (row["state"] == "terminal") != (result is not None):
                raise ValueError()
            return envelope, result
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, ContractError):
            raise WorkingSetCorrupt("child work record invalid") from None

    @staticmethod
    def retire_parent(connection, parent_operation_id):
        """Retire pending children inside the parent's existing transaction."""
        from ahc.domain.sessions import _identifier
        _identifier(parent_operation_id, "parent operation identifier")
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_children'"
        ).fetchone()
        if exists is None:
            return ()
        rows = connection.execute(
            "SELECT * FROM runtime_children WHERE parent_operation_id=? ORDER BY rowid",
            (parent_operation_id,),
        ).fetchall()
        if not rows:
            return ()
        parent = connection.execute(
            "SELECT session_json,request_digest FROM runtime_turns WHERE operation_id=?",
            (parent_operation_id,),
        ).fetchone()
        if parent is None:
            raise WorkingSetCorrupt("child parent turn is unavailable")
        decoded = []
        for row in rows:
            envelope, _ = ChildWorkRepository._decode(row)
            if (
                envelope.parent.operation_id != parent_operation_id
                or row["session_json"] != parent["session_json"]
                or envelope.parent.request_digest != parent["request_digest"]
            ):
                raise WorkingSetCorrupt("child parent identity is invalid")
            decoded.append(envelope.child_id)
        from .child_observations import record
        for row in rows:
            if row["state"] in ("queued", "claimed"):
                envelope, _ = ChildWorkRepository._decode(row)
                record(connection, envelope, ("parent-retired",))
        connection.execute(
            "UPDATE runtime_children SET state=CASE state "
            "WHEN 'queued' THEN 'cancelled' WHEN 'claimed' THEN 'unknown' END,"
            "claim_token=NULL WHERE parent_operation_id=? AND state IN ('queued','claimed')",
            (parent_operation_id,),
        )
        return tuple(decoded)
    def _row(self, connection, parent, child_id):
        row = connection.execute(
            "SELECT * FROM runtime_children WHERE child_id=?", (child_id,)
        ).fetchone()
        if row is None:
            raise ContractError("child record unavailable")
        envelope, result = self._decode(row)
        if envelope.parent != parent:
            raise ContractError("child record parent mismatch")
        return row, envelope, result
    def _records(self, connection, parent):
        rows = connection.execute(
            "SELECT * FROM runtime_children WHERE session_json=? AND parent_operation_id=? "
            "ORDER BY rowid",
            (self._parent_json(parent), parent.operation_id),
        ).fetchall()
        values = []
        for row in rows:
            envelope, result = self._decode(row)
            if envelope.parent != parent:
                raise ContractError("child record parent mismatch")
            values.append(self._record(row, envelope, result, connection))
        return tuple(values)

    @staticmethod
    def _record(row, envelope, result, connection=None):
        from .child_record_projection import project_record

        return project_record(row, envelope, result, connection)

    def admit(self, envelope):
        if type(envelope) is not ChildWorkEnvelope:
            raise ContractError("child envelope invalid")
        def write(connection):
            self._require_parent(connection, envelope.parent)
            row = connection.execute(
                "SELECT * FROM runtime_children WHERE child_id=?", (envelope.child_id,)
            ).fetchone()
            if row is not None:
                existing, result = self._decode(row)
                if existing != envelope:
                    raise ContractError("child identifier already bound")
                return self._record(row, existing, result, connection)
            if envelope.deadline_unix_ms <= self._now():
                raise ContractError("child deadline elapsed")
            connection.execute(
                "INSERT INTO runtime_children VALUES(?,?,?,?,?,?,NULL,NULL)",
                (
                    envelope.child_id,
                    self._parent_json(envelope.parent),
                    envelope.parent.operation_id,
                    envelope.request_digest,
                    canonical_json(envelope.to_dict()).decode(),
                    "queued",
                ),
            )
            row = connection.execute(
                "SELECT * FROM runtime_children WHERE child_id=?", (envelope.child_id,)
            ).fetchone()
            return self._record(row, envelope, None, connection)

        return self.store._mutate(write)

    def claim(self, parent, child_id, token):
        from ahc.domain.sessions import _identifier
        _identifier(child_id, "child identifier")
        _identifier(token, "child claim token")

        def write(connection):
            self._require_parent(connection, parent)
            row, envelope, result = self._row(connection, parent, child_id)
            if row["state"] == "claimed":
                raise ContractError("child already claimed; read or reconcile without replay")
            if row["state"] != "queued":
                return self._record(row, envelope, result, connection)
            state, claim_token = (
                ("cancelled", None)
                if envelope.deadline_unix_ms <= self._now()
                else ("claimed", token)
            )
            connection.execute(
                "UPDATE runtime_children SET state=?,claim_token=? "
                "WHERE child_id=? AND state='queued'",
                (state, claim_token, child_id),
            )
            if state == "cancelled":
                from .child_observations import record
                record(connection, envelope, ("deadline-observed",))
            row, envelope, result = self._row(connection, parent, child_id)
            return self._record(row, envelope, result, connection)

        return self.store._mutate(write)

    def complete(self, parent, child_id, token, output_text):
        from ahc.domain.sessions import _identifier
        _identifier(child_id, "child identifier")
        _identifier(token, "child claim token")
        proposal = normalize_child_result(output_text)
        payload = proposal.to_dict()

        def write(connection):
            self._require_parent(connection, parent)
            row, envelope, result = self._row(connection, parent, child_id)
            if row["state"] == "terminal":
                if row["claim_token"] != token or result != payload:
                    raise ContractError("child terminal identity or result conflicts")
                return self._record(row, envelope, result, connection)
            if row["state"] == "claimed" and row["claim_token"] != token:
                raise ContractError("child claim belongs to another token")
            if row["state"] != "claimed" or row["claim_token"] != token:
                return self._record(row, envelope, result, connection)
            if proposal.artifacts:
                from .child_artifacts import validate_child_artifacts
                validate_child_artifacts(connection, envelope, proposal.artifacts)
            if envelope.deadline_unix_ms <= self._now():
                from .child_observations import record
                record(connection, envelope, ("deadline-observed",))
                connection.execute(
                    "UPDATE runtime_children SET state='unknown',claim_token=NULL WHERE child_id=?",
                    (child_id,),
                )
            else:
                connection.execute(
                    "UPDATE runtime_children SET state='terminal',result_json=? WHERE child_id=?",
                    (canonical_json(payload).decode(), child_id),
                )
            row, envelope, result = self._row(connection, parent, child_id)
            return self._record(row, envelope, result, connection)

        return self.store._mutate(write)

    def cancel(self, parent, child_id):
        from ahc.domain.sessions import _identifier
        _identifier(child_id, "child identifier")

        def write(connection):
            self._require_parent(connection, parent)
            row, envelope, result = self._row(connection, parent, child_id)
            if row["state"] in ("queued", "claimed"):
                connection.execute(
                    "UPDATE runtime_children SET state='cancelled',claim_token=NULL "
                    "WHERE child_id=?",
                    (child_id,),
                )
                from .child_observations import record
                record(connection, envelope, ("cancellation-observed",))
                row, envelope, result = self._row(connection, parent, child_id)
            return self._record(row, envelope, result, connection)

        return self.store._mutate(write)

    def read(self, parent):
        if type(parent) is not ChildParent:
            raise ContractError("child parent invalid")
        with self.store._read() as connection:
            return self._records(connection, parent)

    def mailbox(self, parent):
        return tuple(
            value
            for value in self.read(parent)
            if value["state"] in ("terminal", "cancelled", "unknown")
        )

    def reconcile(self, parent):
        from .child_observations import reconcile_claims

        return reconcile_claims(self, parent)
