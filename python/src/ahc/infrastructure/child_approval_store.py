"""One-shot approvals bound to an active child claim, not a fabricated main turn."""

import json
import time

from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.runtime_approval import ApprovalDecision, ApprovalRequest
from ahc.domain.runtime_turn import request_fingerprint
from ahc.domain.sessions import ContractError, _identifier
from ahc.domain.skill_values import canonical_json, digest_json

from .runtime_approval_store import RuntimeApprovalStore

_TABLE = """CREATE TABLE IF NOT EXISTS runtime_child_approvals (
request_digest TEXT PRIMARY KEY, slot_digest TEXT UNIQUE NOT NULL,
binding_digest TEXT NOT NULL, payload_json TEXT NOT NULL, state TEXT NOT NULL, outcome TEXT)"""


class ChildApprovalStore:
    def __init__(self, children, envelope, token, admitted, *, clock=time.time):
        if (type(envelope) is not ChildWorkEnvelope or type(admitted) is not AgentTurnRequest
                or admitted.session != envelope.parent.session
                or admitted.operation_id != envelope.child_id
                or admitted.input_text != envelope.task_text
                or admitted.workspace_scope != envelope.artifact_scope
                or admitted.effective_policy is None
                or admitted.effective_policy != children.request.effective_policy):
            raise ContractError("child approval admission invalid")
        children._bound(envelope.parent)
        _identifier(token, "child claim token")
        self.children, self.envelope, self.token = children, envelope, token
        self.admitted, self.store = admitted, children.store
        self._clock = RuntimeApprovalStore(self.store, clock=clock)
        self.binding = digest_json({"envelope": envelope.request_digest, "token": token,
            "request": request_fingerprint(admitted), "deadline": admitted.deadline})

    def _current(self, connection):
        self.children._require_parent(connection, self.envelope.parent)
        row, recorded, _ = self.children._row(
            connection, self.envelope.parent, self.envelope.child_id)
        if (recorded != self.envelope or row["state"] != "claimed"
                or row["claim_token"] != self.token
                or self.children._now() >= self.envelope.deadline_unix_ms
                or self.children._monotonic_now() >= min(
                    self.admitted.deadline, self.children.request.effective_deadline)):
            raise ContractError("child approval claim no longer active")

    def _read(self, connection, session, digest):
        row = connection.execute("SELECT * FROM runtime_child_approvals WHERE request_digest=?",
                                 (digest,)).fetchone()
        if row is None or row["binding_digest"] != self.binding:
            raise ContractError("child approval binding unavailable")
        request = ApprovalRequest.from_dict(json.loads(row["payload_json"]))
        if (request.digest != row["request_digest"] or request.session != session
                or request.operation_id != self.admitted.operation_id
                or request.policy_digest != self.admitted.effective_policy.digest
                or canonical_json(request.to_dict()).decode() != row["payload_json"]
                or row["state"] not in ("pending", "answered", "consumed")
                or row["slot_digest"] != digest_json({"binding": self.binding,
                    "thread": request.backend_thread, "turn": request.backend_turn,
                    "vendor": request.vendor_request_id})
                or (row["state"] == "pending" and row["outcome"] is not None)):
            raise ContractError("child approval record invalid")
        if row["state"] != "pending":
            ApprovalDecision(request.digest, row["outcome"])
        return row, request

    def publish(self, admitted, request):
        if (admitted != self.admitted or type(request) is not ApprovalRequest
                or request.session != admitted.session
                or request.operation_id != admitted.operation_id
                or request.policy_digest != admitted.effective_policy.digest):
            raise ContractError("child approval request mismatch")
        slot = digest_json({"binding": self.binding, "thread": request.backend_thread,
                            "turn": request.backend_turn, "vendor": request.vendor_request_id})

        def write(connection):
            self._current(connection)
            now = self._clock._now()
            if not now < request.expires_at <= min(
                    now + admitted.effective_policy.approval_wait_seconds,
                    self.envelope.deadline_unix_ms / 1000):
                raise ContractError("child approval expiration outside limit")
            connection.execute(_TABLE)
            connection.execute("CREATE TABLE IF NOT EXISTS runtime_child_approval_context ("
                "binding_digest TEXT PRIMARY KEY, request_fingerprint TEXT NOT NULL, "
                "deadline TEXT NOT NULL)")
            context = (self.binding, request_fingerprint(admitted),
                       canonical_json(admitted.deadline).decode())
            saved = connection.execute("SELECT * FROM runtime_child_approval_context "
                                       "WHERE binding_digest=?", (self.binding,)).fetchone()
            if saved is not None and tuple(saved) != context:
                raise ContractError("child approval context differs")
            if saved is None:
                connection.execute("INSERT INTO runtime_child_approval_context VALUES(?,?,?)",
                                   context)
            existing = connection.execute("SELECT request_digest FROM runtime_child_approvals "
                                           "WHERE slot_digest=?", (slot,)).fetchone()
            if existing:
                row, _ = self._read(connection, request.session, existing[0])
                if row["request_digest"] != request.digest or row["state"] != "pending":
                    raise ContractError("child approval slot already resolved or bound")
                return request.digest
            if connection.execute("SELECT count(*) FROM runtime_child_approvals "
                                  "WHERE binding_digest=?", (self.binding,)).fetchone()[0] >= 128:
                raise ContractError("child approval budget exhausted")
            connection.execute("INSERT INTO runtime_child_approvals VALUES(?,?,?,?,'pending',NULL)",
                (request.digest, slot, self.binding, canonical_json(request.to_dict()).decode()))
            return request.digest
        return self.store._mutate(write)

    def _change(self, session, digest, *, outcome=None, cancelled=False, timed_out=False,
                _transaction=None):
        if type(cancelled) is not bool or type(timed_out) is not bool:
            raise ContractError("child approval terminal flags invalid")

        def write(connection):
            self._current(connection)
            row, request = self._read(connection, session, digest)
            if row["state"] == "consumed" or (outcome is not None and row["state"] != "pending"):
                raise ContractError("child approval already resolved")
            now = self._clock._now()
            selected = "timeout" if timed_out else outcome or row["outcome"]
            if selected is None and not cancelled and now < request.expires_at:
                return None
            result = ApprovalDecision(digest, selected or "timeout").resolve(
                request, now=now, cancelled=cancelled)
            connection.execute("UPDATE runtime_child_approvals SET state=?,outcome=? "
                "WHERE request_digest=?", ("answered" if outcome is not None else "consumed",
                                          result, digest))
            return ApprovalDecision(digest, result)
        return write(_transaction) if _transaction is not None else self.store._mutate(write)

    def respond(self, session, digest, outcome, *, _transaction=None):
        if outcome not in ("approve-once", "deny"):
            raise ContractError("child operator response invalid")
        return self._change(session, digest, outcome=outcome, _transaction=_transaction)

    def consume(self, session, digest, *, cancelled=False, timed_out=False):
        return self._change(session, digest, cancelled=cancelled, timed_out=timed_out)
