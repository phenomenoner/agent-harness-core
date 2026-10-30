"""Pending decisions in runtime SQLite; never acquire the turn execution lease."""

import json
import time
from math import isfinite

from ahc.domain.runtime_approval import ApprovalDecision, ApprovalRequest
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json

from .working_repository import WorkingSetConflict, WorkingSetCorrupt

_POLICY_TABLE = """CREATE TABLE IF NOT EXISTS runtime_effective_policies (
operation_id TEXT PRIMARY KEY REFERENCES runtime_turns(operation_id),
payload_json TEXT NOT NULL, digest TEXT NOT NULL)"""
_TABLE = """CREATE TABLE IF NOT EXISTS runtime_approvals (
request_digest TEXT PRIMARY KEY, slot_digest TEXT NOT NULL UNIQUE,
operation_id TEXT NOT NULL REFERENCES runtime_turns(operation_id),
payload_json TEXT NOT NULL, state TEXT NOT NULL, outcome TEXT)"""


def persist_policy(connection, request):
    """Called inside the existing admission transaction, before backend execution."""
    if request.effective_policy is None:
        return
    connection.execute(_POLICY_TABLE)
    connection.execute(
        "INSERT INTO runtime_effective_policies VALUES(?,?,?)",
        (
            request.operation_id,
            canonical_json(request.effective_policy.to_dict()).decode(),
            request.effective_policy.digest,
        ),
    )


class RuntimeApprovalStore:
    def __init__(self, store, *, clock=time.time):
        self.store, self.clock = store, clock

    def _now(self):
        value = self.clock()
        if type(value) not in (int, float) or not isfinite(value):
            raise ContractError("approval clock invalid")
        return value

    @staticmethod
    def _exists(connection):
        return (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_approvals'"
            ).fetchone()
            is not None
        )

    @staticmethod
    def _decode(row):
        request = ApprovalRequest.from_dict(json.loads(row["payload_json"]))
        if request.digest != row["request_digest"] or request.operation_id != row["operation_id"]:
            raise WorkingSetCorrupt("approval binding corrupted")
        if row["state"] not in ("pending", "answered", "consumed"):
            raise WorkingSetCorrupt("approval state corrupted")
        if row["state"] != "pending":
            ApprovalDecision(request.digest, row["outcome"])
        return request

    def _current(self, connection, request):
        row = connection.execute(
            "SELECT * FROM runtime_turns WHERE operation_id=?", (request.operation_id,)
        ).fetchone()
        current = self.store._load_current(connection, request.session.ref.virtual_session_id)
        if (
            row is None
            or row["status"] != "submitted"
            or json.loads(row["session_json"]) != request.session.to_dict()
            or current.session != request.session
            or current.status != "active"
            or current.active_turn_id != request.operation_id
        ):
            raise WorkingSetConflict("approval no longer belongs to an active turn")
        return row

    def publish(self, admitted, request):
        if type(request) is not ApprovalRequest or admitted.effective_policy is None:
            raise ContractError("approval requires an admitted effective policy")
        if (
            request.session != admitted.session
            or request.operation_id != admitted.operation_id
            or request.policy_digest != admitted.effective_policy.digest
        ):
            raise ContractError("approval admission binding mismatch")
        slot = digest_json(
            {
                key: request.to_dict()[key]
                for key in (
                    "session",
                    "operation_id",
                    "backend_thread",
                    "backend_turn",
                    "vendor_request_id",
                )
            }
        )

        def write(connection):
            row = self._current(connection, request)
            if row["request_digest"] != request_fingerprint(admitted):
                raise ContractError("approval admitted request changed")
            stored = connection.execute(
                "SELECT * FROM runtime_effective_policies WHERE operation_id=?",
                (request.operation_id,),
            ).fetchone()
            if stored is None:
                raise ContractError("approval policy missing from durable admission")
            policy = EffectivePolicy.from_dict(json.loads(stored["payload_json"]))
            if policy.digest != stored["digest"] or policy.digest != request.policy_digest:
                raise WorkingSetCorrupt("approval policy binding corrupted")
            now = self._now()
            if request.expires_at <= now or request.expires_at - now > policy.approval_wait_seconds:
                raise ContractError("approval deadline outside admitted limit")
            connection.execute(_TABLE)
            existing = connection.execute(
                "SELECT * FROM runtime_approvals WHERE slot_digest=?", (slot,)
            ).fetchone()
            if existing is not None:
                self._decode(existing)
                if existing["request_digest"] != request.digest or existing["state"] != "pending":
                    raise ContractError("approval request already bound or resolved")
                return request.digest
            count = connection.execute(
                "SELECT count(*) FROM runtime_approvals WHERE operation_id=?",
                (request.operation_id,),
            ).fetchone()[0]
            if count >= 128:
                raise ContractError("approval request budget exhausted")
            connection.execute(
                "INSERT INTO runtime_approvals VALUES(?,?,?,?,'pending',NULL)",
                (
                    request.digest,
                    slot,
                    request.operation_id,
                    canonical_json(request.to_dict()).decode(),
                ),
            )
            return request.digest

        return self.store._mutate(write)

    def query(self, session, request_digest):
        with self.store._read() as connection:
            if not self._exists(connection):
                return None
            row = connection.execute(
                "SELECT * FROM runtime_approvals WHERE request_digest=?", (request_digest,)
            ).fetchone()
            if row is None:
                return None
            request = self._decode(row)
            if request.session != session:
                raise ContractError("approval query owner mismatch")
            return {
                "request_digest": request.digest,
                "request": request.to_dict(),
                "state": row["state"],
                "outcome": row["outcome"],
                "expired": self._now() >= request.expires_at,
            }

    def pending(self, session):
        with self.store._read() as connection:
            if not self._exists(connection):
                return ()
            owner = canonical_json(session.to_dict()).decode()
            rows = connection.execute(
                "SELECT a.* FROM runtime_approvals a JOIN runtime_turns t "
                "ON a.operation_id=t.operation_id WHERE t.session_json=? "
                "AND t.status='submitted' AND a.state!='consumed' ORDER BY a.rowid LIMIT 128",
                (owner,),
            ).fetchall()
            requests = [self._decode(row) for row in rows]
            return tuple(
                {
                    "request_digest": request.digest,
                    "request": request.to_dict(),
                    "state": row["state"],
                    "outcome": row["outcome"],
                    "expired": self._now() >= request.expires_at,
                }
                for row, request in zip(rows, requests, strict=True)
            )

    def _change(self, session, request_digest, *, outcome=None, cancelled=False, timed_out=False,
                _transaction=None):
        if type(cancelled) is not bool or type(timed_out) is not bool:
            raise ContractError("approval terminal flags invalid")

        def write(connection):
            if not self._exists(connection):
                raise ContractError("approval request absent")
            row = connection.execute(
                "SELECT * FROM runtime_approvals WHERE request_digest=?", (request_digest,)
            ).fetchone()
            if row is None:
                raise ContractError("approval request absent")
            request = self._decode(row)
            if request.session != session:
                raise ContractError("approval response owner mismatch")
            self._current(connection, request)
            if row["state"] == "consumed" or (outcome is not None and row["state"] != "pending"):
                raise ContractError("approval response already resolved")
            now = self._now()
            selected = "timeout" if timed_out else outcome or row["outcome"]
            if selected is None and not cancelled and now < request.expires_at:
                return None
            decision = ApprovalDecision(request.digest, selected or "timeout")
            resolved = decision.resolve(request, now=now, cancelled=cancelled)
            state = "answered" if outcome is not None else "consumed"
            connection.execute(
                "UPDATE runtime_approvals SET state=?,outcome=? WHERE request_digest=?",
                (state, resolved, request.digest),
            )
            return ApprovalDecision(request.digest, resolved)

        return write(_transaction) if _transaction is not None else self.store._mutate(write)

    def respond(self, session, request_digest, outcome, *, _transaction=None):
        if outcome not in ("approve-once", "deny"):
            raise ContractError("operator response must approve once or deny")
        return self._change(session, request_digest, outcome=outcome, _transaction=_transaction)

    def consume(self, session, request_digest, *, cancelled=False, timed_out=False):
        """Atomically consume at most once; None means still awaiting a response."""
        return self._change(session, request_digest, cancelled=cancelled, timed_out=timed_out)
