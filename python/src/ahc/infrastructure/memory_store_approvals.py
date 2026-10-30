"""Exact-payload Store proposals in the existing runtime transaction owner."""

import json
import math
import re
import time

from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import canonical_json, digest_json

from .working_repository import WorkingSetConflict

_TABLE = """CREATE TABLE IF NOT EXISTS memory_store_proposals (
digest TEXT PRIMARY KEY, slot TEXT UNIQUE NOT NULL, payload TEXT NOT NULL,
state TEXT NOT NULL, approval_expires REAL)"""


class MemoryStoreApprovals:
    def __init__(self, store, *, clock=time.time):
        self.store, self.clock = store, clock

    def _now(self):
        value = self.clock()
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ContractError("memory approval clock invalid")
        return value

    def _current(self, conn, session):
        if type(session) is not VirtualSessionIdentity:
            raise ContractError("memory approval session invalid")
        current = self.store._load_current(conn, session.ref.virtual_session_id)
        if current.session != session or current.status != "active":
            raise WorkingSetConflict("memory proposal session no longer active")

    def stage(
        self,
        session,
        *,
        operation_id,
        resource_id,
        provider_digest,
        scope,
        observation,
        origin,
        expires_at,
    ):
        for value in (operation_id, resource_id, scope, origin):
            if type(value) is not str or not value or len(value) > 256:
                raise ContractError("memory proposal text invalid")
        if type(provider_digest) is not str or not re.fullmatch(r"[0-9a-f]{64}", provider_digest):
            raise ContractError("memory proposal provider invalid")
        if type(observation) is not dict or set(observation) != {"kind", "summary", "detail"}:
            raise ContractError("memory proposal observation invalid")
        if type(session) is not VirtualSessionIdentity:
            raise ContractError("memory approval session invalid")
        now = self._now()
        if (
            type(expires_at) not in (int, float)
            or not math.isfinite(expires_at)
            or not now < expires_at <= now + 86400
        ):
            raise ContractError("memory proposal expiry invalid")
        payload = {
            "session": session.to_dict(),
            "operation_id": operation_id,
            "resource_id": resource_id,
            "provider_digest": provider_digest,
            "scope": scope,
            "observation": observation,
            "origin": origin,
            "created_at": now,
            "expires_at": expires_at,
            "capability": "memory.store",
        }
        encoded = canonical_json(payload)
        if len(encoded) > 70000:
            raise ContractError("memory proposal too large")
        digest = digest_json(payload)
        slot = digest_json(
            {"session": session.to_dict(), "operation_id": operation_id, "resource_id": resource_id}
        )

        def write(conn):
            self._current(conn, session)
            conn.execute(_TABLE)
            if conn.execute(
                "SELECT 1 FROM memory_store_proposals WHERE slot=?", (slot,)
            ).fetchone():
                raise ContractError("memory operation already proposed")
            conn.execute(
                "INSERT INTO memory_store_proposals VALUES(?,?,?,'pending',NULL)",
                (digest, slot, encoded.decode()),
            )
            return digest

        return self.store._mutate(write)

    @staticmethod
    def _row(conn, session, digest):
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='memory_store_proposals'"
        ).fetchone()
        row = (
            None
            if not exists
            else conn.execute(
                "SELECT * FROM memory_store_proposals WHERE digest=?", (digest,)
            ).fetchone()
        )
        if row is None:
            raise ContractError("memory proposal absent")
        payload = json.loads(row["payload"])
        if digest_json(payload) != digest or payload["session"] != session.to_dict():
            raise ContractError("memory proposal identity mismatch")
        return row, payload

    def query(self, session, digest):
        with self.store._read() as conn:
            row, payload = self._row(conn, session, digest)
            return {
                "digest": digest,
                "proposal": payload,
                "state": row["state"],
                "approval_expires": row["approval_expires"],
            }

    def approve(self, session, digest, *, approval_expires):
        def write(conn):
            self._current(conn, session)
            row, payload = self._row(conn, session, digest)
            now = self._now()
            if (
                row["state"] != "pending"
                or payload["expires_at"] <= now
                or type(approval_expires) not in (int, float)
                or not math.isfinite(approval_expires)
                or not now < approval_expires <= min(payload["expires_at"], now + 3600)
            ):
                raise ContractError("memory approval state or expiry invalid")
            conn.execute(
                "UPDATE memory_store_proposals SET state='approved', approval_expires=? "
                "WHERE digest=?",
                (approval_expires, digest),
            )

        self.store._mutate(write)

    def consume(self, session, digest):
        def write(conn):
            self._current(conn, session)
            row, payload = self._row(conn, session, digest)
            now = self._now()
            if (
                row["state"] != "approved"
                or row["approval_expires"] <= now
                or payload["expires_at"] <= now
            ):
                raise ContractError("memory approval not consumable")
            conn.execute(
                "UPDATE memory_store_proposals SET state='in-flight' WHERE digest=?", (digest,)
            )
            return payload

        return self.store._mutate(write)

    def reject(self, session, digest):
        def write(conn):
            self._current(conn, session)
            row, _ = self._row(conn, session, digest)
            if row["state"] not in ("pending", "approved"):
                raise ContractError("memory proposal cannot be rejected after dispatch")
            conn.execute(
                "UPDATE memory_store_proposals SET state='rejected' WHERE digest=?", (digest,)
            )

        self.store._mutate(write)

    def finish(self, session, digest, outcome):
        if outcome not in ("succeeded", "failed", "unknown"):
            raise ContractError("memory store outcome invalid")

        def write(conn):
            row, _ = self._row(conn, session, digest)
            if row["state"] != "in-flight":
                raise ContractError("memory store not in flight")
            conn.execute(
                "UPDATE memory_store_proposals SET state=? WHERE digest=?", (outcome, digest)
            )

        self.store._mutate(write)

    def resolve(self, session, digest, outcome):
        if outcome not in ("succeeded", "failed", "unknown"):
            raise ContractError("memory reconciliation outcome invalid")

        def write(conn):
            row, _ = self._row(conn, session, digest)
            if row["state"] not in ("in-flight", "unknown"):
                raise ContractError("memory reconciliation state changed")
            conn.execute(
                "UPDATE memory_store_proposals SET state=? WHERE digest=?", (outcome, digest)
            )

        self.store._mutate(write)
