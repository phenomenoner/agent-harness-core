"""Scoped memory correction with durable one-send custody and read-only recovery.

This service requires an explicit memory.revise grant in addition to trusted
policy. It is not an implicit extension of the legacy memory.store grant.
"""

import json
import re

from ahc.domain.capability_values import (
    ApprovalMode,
    CapabilityDefinition,
    DecisionKind,
    EffectClass,
)
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.profile_values import ProfileResourceKind
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import canonical_json, digest_json

from .capability_composition import compose_capability_views
from .memory_store_service import MemoryStoreService

_TABLE = """CREATE TABLE IF NOT EXISTS memory_revision_operations (
slot TEXT PRIMARY KEY, digest TEXT UNIQUE NOT NULL, payload TEXT NOT NULL,
state TEXT NOT NULL, result TEXT)"""


class MemoryRevisionService:
    def __init__(self, *, registry, agent_id, resource_id, scope, provider_digest,
                 grants, policy, repository, revise, reconcile):
        if type(registry) is not AgentProfileRegistry:
            raise ContractError("memory revision registry invalid")
        profile = registry.profile(agent_id)
        if (
            profile.durable_memory_ref != resource_id
            or policy not in ("deny", "trusted")
            or type(scope) is not str
            or not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]{0,255}", scope)
            or type(provider_digest) is not str
            or not re.fullmatch(r"[a-f0-9]{64}", provider_digest)
            or not callable(revise)
            or not callable(reconcile)
        ):
            raise ContractError("memory revision binding invalid")
        definition = CapabilityDefinition(
            "memory.revise", ProfileResourceKind.DURABLE_MEMORY,
            EffectClass.EXTERNAL_OR_IRREVERSIBLE, ApprovalMode.REQUIRED,
        )
        self.view = compose_capability_views(
            profile_registry=registry, definitions=(definition,), grants=grants,
        ).view(agent_id)
        self.agent, self.resource, self.scope = agent_id, resource_id, scope
        self.provider, self.policy, self.repository = provider_digest, policy, repository
        self.revise, self.reconcile = revise, reconcile

    def _admit(self, session):
        if (
            type(session) is not VirtualSessionIdentity
            or session.agent_id != self.agent
            or self.policy != "trusted"
            or self.view.decide(
                "memory.revise", self.resource, EffectClass.EXTERNAL_OR_IRREVERSIBLE,
            ).kind is not DecisionKind.REQUIRES_APPROVAL
        ):
            raise ContractError("memory revision not admitted")

    def _payload(self, session, operation_id, expected_ref, observation):
        self._admit(session)
        for value in (operation_id, expected_ref):
            if type(value) is not str or not 1 <= len(value) <= 256 or not value.strip():
                raise ContractError("memory revision identity invalid")
        if observation is not None:
            observation = MemoryStoreService._observation(observation)
            if observation["detail"].get("scope", self.scope) != self.scope:
                raise ContractError("memory revision observation scope mismatch")
        request = {
            "scope": self.scope, "operation_id": operation_id,
            "expected_ref": expected_ref, "observation": observation,
        }
        payload = {
            "session": session.to_dict(), "resource_id": self.resource,
            "provider_digest": self.provider, "request": request,
        }
        if len(canonical_json(payload)) > 65536:
            raise ContractError("memory revision exceeds request bound")
        return payload

    @staticmethod
    def _outcome(value, payload):
        request = payload["request"]
        if (
            type(value) is not dict
            or set(value) != {"status", "previous_ref", "record_ref", "mutation_digest"}
            or value["status"] != "succeeded"
            or value["mutation_digest"] != digest_json(request)
            or value["previous_ref"] != request["expected_ref"]
        ):
            return {"status": "unknown"}
        successor = value["record_ref"]
        if request["observation"] is None:
            if successor is not None:
                return {"status": "unknown"}
        elif (
            type(successor) is not str or not 1 <= len(successor) <= 256
            or successor == request["expected_ref"]
        ):
            return {"status": "unknown"}
        return json.loads(canonical_json(value))

    @staticmethod
    def _read(conn, digest):
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='memory_revision_operations'"
        ).fetchone()
        row = None if not exists else conn.execute(
            "SELECT * FROM memory_revision_operations WHERE digest=?", (digest,),
        ).fetchone()
        if row is None or digest_json(json.loads(row["payload"])) != digest:
            raise ContractError("memory revision receipt missing or changed")
        return row, json.loads(row["payload"])

    def _finish(self, digest, result):
        def finish(conn):
            row, payload = self._read(conn, digest)
            if row["state"] == "succeeded":
                retained = json.loads(row["result"])
                if self._outcome(retained, payload)["status"] != "succeeded":
                    raise ContractError("memory revision retained outcome changed")
                return retained
            if row["state"] not in ("in-flight", "unknown"):
                raise ContractError("memory revision custody invalid")
            conn.execute(
                "UPDATE memory_revision_operations SET state=?,result=? WHERE digest=?",
                (result["status"], canonical_json(result).decode(), digest),
            )
            return result
        return self.repository._mutate(finish)

    def revise_trusted(self, session, *, operation_id, expected_ref, observation=None):
        payload = self._payload(session, operation_id, expected_ref, observation)
        digest = digest_json(payload)
        slot = digest_json([self.agent, self.resource, operation_id])

        def reserve(conn):
            current = self.repository._load_current(conn, session.ref.virtual_session_id)
            if current.session != session or current.status != "active":
                raise ContractError("memory revision session no longer active")
            conn.execute(_TABLE)
            old = conn.execute(
                "SELECT digest FROM memory_revision_operations WHERE slot=?", (slot,),
            ).fetchone()
            if old:
                if old[0] != digest:
                    raise ContractError("memory revision operation already bound")
                row, stored = self._read(conn, digest)
                if row["state"] == "succeeded":
                    return self._outcome(json.loads(row["result"]), stored)
                return {"status": "unknown"}
            conn.execute(
                "INSERT INTO memory_revision_operations VALUES(?,?,?,'in-flight',NULL)",
                (slot, digest, canonical_json(payload).decode()),
            )
            return None

        existing = self.repository._mutate(reserve)
        if existing is not None:
            return {**existing, "revision_digest": digest}
        try:
            result = self._outcome(self.revise(**payload["request"]), payload)
        except Exception:
            result = {"status": "unknown"}
        try:
            result = self._finish(digest, result)
        except Exception:
            result = {"status": "unknown"}
        return {**result, "revision_digest": digest}

    def reconcile_only(self, session, revision_digest):
        self._admit(session)
        if type(revision_digest) is not str or not re.fullmatch(r"[a-f0-9]{64}", revision_digest):
            raise ContractError("memory revision receipt invalid")
        with self.repository._read() as conn:
            row, payload = self._read(conn, revision_digest)
            if (
                payload["session"] != session.to_dict()
                or payload["resource_id"] != self.resource
                or payload["provider_digest"] != self.provider
                or payload["request"]["scope"] != self.scope
            ):
                raise ContractError("memory revision binding changed")
            if row["state"] == "succeeded":
                result = self._outcome(json.loads(row["result"]), payload)
                return {**result, "revision_digest": revision_digest}
        request = payload["request"]
        try:
            value = self.reconcile(
                scope=self.scope, operation_id=request["operation_id"],
                mutation_digest=digest_json(request),
            )
            result = self._finish(revision_digest, self._outcome(value, payload))
        except Exception:
            result = {"status": "unknown"}
        return {**result, "revision_digest": revision_digest}
