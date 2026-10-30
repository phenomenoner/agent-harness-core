"""Read child approvals from a new operator process without a backend request object."""

import json
import time
from math import isfinite

from ahc.domain.runtime_approval import ApprovalDecision, ApprovalRequest
from ahc.domain.runtime_policy import EffectivePolicy
from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import canonical_json, digest_json
from ahc.domain.turns import _sha256_hex

from .child_work_repository import ChildWorkRepository
from .runtime_approval_store import RuntimeApprovalStore
from .working_repository import WorkingSetConflict


class ChildApprovalQueries:
    def __init__(self, store, *, clock=time.time, monotonic=time.monotonic):
        self.store, self.monotonic = store, monotonic
        self.clock = RuntimeApprovalStore(store, clock=clock)

    @staticmethod
    def _exists(connection, name):
        return connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                                  (name,)).fetchone() is not None

    def _project(self, connection, row, session):
        request = ApprovalRequest.from_dict(json.loads(row["payload_json"]))
        if request.session != session:
            raise ContractError("child approval query owner mismatch")
        if (request.digest != row["request_digest"]
                or canonical_json(request.to_dict()).decode() != row["payload_json"]
                or row["slot_digest"] != digest_json({"binding": row["binding_digest"],
                    "thread": request.backend_thread, "turn": request.backend_turn,
                    "vendor": request.vendor_request_id})
                or row["state"] not in ("pending", "answered", "consumed")
                or (row["state"] == "pending" and row["outcome"] is not None)):
            raise ContractError("child approval query record invalid")
        if row["state"] != "pending":
            ApprovalDecision(request.digest, row["outcome"])
        context = None
        if self._exists(connection, "runtime_child_approval_context"):
            context = connection.execute("SELECT * FROM runtime_child_approval_context "
                "WHERE binding_digest=?", (row["binding_digest"],)).fetchone()
        child = connection.execute("SELECT * FROM runtime_children WHERE child_id=?",
                                   (request.operation_id,)).fetchone()
        active = False
        parent = None
        if child is not None and context is not None:
            envelope, _ = ChildWorkRepository._decode(child)
            parent = envelope.parent.operation_id
            _sha256_hex(context["request_fingerprint"])
            deadline, now = json.loads(context["deadline"]), self.monotonic()
            if (type(deadline) not in (int, float) or not isfinite(deadline)
                    or type(now) not in (int, float) or not isfinite(now)):
                raise ContractError("child approval query deadline invalid")
            binding = digest_json({"envelope": envelope.request_digest,
                "token": child["claim_token"], "request": context["request_fingerprint"],
                "deadline": deadline})
            if (envelope.parent.session == session and child["state"] == "claimed"
                    and binding == row["binding_digest"] and now < deadline
                    and self.clock._now() * 1000 < envelope.deadline_unix_ms):
                try:
                    ChildWorkRepository(self.store)._require_parent(connection, envelope.parent)
                    active = True
                except WorkingSetConflict:
                    pass
        return {"request_digest": request.digest, "request": request.to_dict(),
                "state": row["state"], "outcome": row["outcome"],
                "expired": self.clock._now() >= request.expires_at,
                "parent_operation_id": parent, "current_claim": active}

    def query(self, session, digest):
        _sha256_hex(digest)
        with self.store._read() as connection:
            if not self._exists(connection, "runtime_child_approvals"):
                return None
            row = connection.execute("SELECT * FROM runtime_child_approvals WHERE request_digest=?",
                                     (digest,)).fetchone()
            return None if row is None else self._project(connection, row, session)

    def respond(self, session, digest, outcome):
        _sha256_hex(digest)
        if outcome not in ("approve-once", "deny"):
            raise ContractError("child operator response invalid")

        def write(connection):
            if not self._exists(connection, "runtime_child_approvals"):
                raise ContractError("child approval absent")
            row = connection.execute("SELECT * FROM runtime_child_approvals WHERE request_digest=?",
                                     (digest,)).fetchone()
            if row is None:
                raise ContractError("child approval absent")
            observed = self._project(connection, row, session)
            if not observed["current_claim"] or observed["state"] != "pending":
                raise ContractError("child approval claim inactive or already resolved")
            request = ApprovalRequest.from_dict(observed["request"])
            policy_row = connection.execute("SELECT * FROM runtime_effective_policies "
                "WHERE operation_id=?", (observed["parent_operation_id"],)).fetchone()
            if policy_row is None:
                raise ContractError("child approval parent policy unavailable")
            policy = EffectivePolicy.from_dict(json.loads(policy_row["payload_json"]))
            if policy.digest != policy_row["digest"] or policy.digest != request.policy_digest:
                raise ContractError("child approval parent policy mismatch")
            selected = ApprovalDecision(digest, outcome).resolve(
                request, now=self.clock._now(), cancelled=False)
            connection.execute("UPDATE runtime_child_approvals SET state='answered',outcome=? "
                               "WHERE request_digest=? AND state='pending'", (selected, digest))
            return ApprovalDecision(digest, selected)
        return self.store._mutate(write)

    def pending(self, session):
        with self.store._read() as connection:
            if not self._exists(connection, "runtime_child_approvals"):
                return ()
            rows = connection.execute("SELECT a.* FROM runtime_child_approvals a "
                "JOIN runtime_children c "
                "ON c.child_id=json_extract(a.payload_json,'$.operation_id') "
                "WHERE a.state!='consumed' AND c.session_json=? ORDER BY a.rowid LIMIT 128",
                (canonical_json(session.to_dict()).decode(),)).fetchall()
            result = []
            for row in rows:
                request = ApprovalRequest.from_dict(json.loads(row["payload_json"]))
                if request.session == session:
                    result.append(self._project(connection, row, session))
                    if len(result) == 128:
                        break
            return tuple(result)
