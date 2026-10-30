"""Reviewed native Store; unknown effects are reconciled, never re-sent."""

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


class MemoryStoreService:
    def __init__(
        self,
        *,
        registry,
        agent_id,
        resource_id,
        scope,
        provider_digest,
        grants,
        policy,
        approvals,
        store,
        reconcile,
    ):
        if type(registry) is not AgentProfileRegistry:
            raise ContractError("memory store registry invalid")
        profile = registry.profile(agent_id)
        if (
            profile.durable_memory_ref != resource_id
            or policy not in ("deny", "review", "trusted")
            or type(scope) is not str
            or not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]{0,255}", scope)
            or type(provider_digest) is not str
            or not re.fullmatch(r"[0-9a-f]{64}", provider_digest)
            or not callable(store)
            or not callable(reconcile)
        ):
            raise ContractError("memory store binding invalid")
        definition = CapabilityDefinition(
            "memory.store",
            ProfileResourceKind.DURABLE_MEMORY,
            EffectClass.EXTERNAL_OR_IRREVERSIBLE,
            ApprovalMode.REQUIRED,
        )
        self.view = compose_capability_views(
            profile_registry=registry, definitions=(definition,), grants=grants
        ).view(agent_id)
        self.agent, self.resource, self.scope = agent_id, resource_id, scope
        self.provider, self.policy, self.approvals = provider_digest, policy, approvals
        self.store, self.reconcile = store, reconcile

    def _admit(self, session):
        if (
            type(session) is not VirtualSessionIdentity
            or session.agent_id != self.agent
            or self.policy not in ("review", "trusted")
            or self.view.decide(
                "memory.store", self.resource, EffectClass.EXTERNAL_OR_IRREVERSIBLE
            ).kind
            is not DecisionKind.REQUIRES_APPROVAL
        ):
            raise ContractError("memory store not admitted")

    @staticmethod
    def _observation(value):
        if (
            type(value) is not dict
            or set(value) != {"kind", "summary", "detail"}
            or type(value["detail"]) is not dict
            or any(type(value[k]) is not str or not value[k].strip() for k in ("kind", "summary"))
        ):
            raise ContractError("memory observation invalid")
        return json.loads(canonical_json(value))

    def propose(self, session, *, operation_id, observation, origin, expires_at):
        self._admit(session)
        observation = self._observation(observation)
        if observation["detail"].get("scope", self.scope) != self.scope:
            raise ContractError("memory observation scope mismatch")
        if (
            len(canonical_json({"version": 1, "scope": self.scope, "observation": observation}))
            > 65536
        ):
            raise ContractError("memory observation too large")
        return self.approvals.stage(
            session,
            operation_id=operation_id,
            resource_id=self.resource,
            provider_digest=self.provider,
            scope=self.scope,
            observation=observation,
            origin=origin,
            expires_at=expires_at,
        )

    def _proposal(self, session, digest):
        self._admit(session)
        row = self.approvals.query(session, digest)
        value = row["proposal"]
        if (
            value["resource_id"] != self.resource
            or value["provider_digest"] != self.provider
            or value["scope"] != self.scope
            or value["capability"] != "memory.store"
        ):
            raise ContractError("memory proposal binding changed")
        return row

    def _outcome(self, value, proposal):
        expected = digest_json(
            {"version": 1, "scope": self.scope, "observation": proposal["observation"]}
        )
        if type(value) is not dict:
            return {"status": "unknown"}
        if value.get("status") == "succeeded":
            record, trace = value.get("record"), value.get("trace")
            if (
                set(value)
                != {
                    "status",
                    "record_ref",
                    "request_digest",
                    "record",
                    "provenance",
                    "trust",
                    "trace",
                }
                or value["request_digest"] != expected
                or type(value["record_ref"]) is not str
                or not 1 <= len(value["record_ref"]) <= 256
                or type(record) is not dict
                or set(record) != {"kind", "summary", "detail", "created_at"}
                or value["provenance"] != {"origin": "unknown"}
                or value["trust"] != "untrusted"
                or trace != {"operation_id": proposal["operation_id"], "request_digest": expected}
                or type(record["created_at"]) is not str
                or not 1 <= len(record["created_at"]) <= 128
            ):
                return {"status": "unknown"}
            original = proposal["observation"]
            if (
                record["kind"] != original["kind"]
                or record["summary"] != original["summary"]
                or record["detail"] != {**original["detail"], "scope": self.scope}
            ):
                return {"status": "unknown"}
            return {
                "status": "succeeded",
                "record_ref": value["record_ref"],
                "request_digest": expected,
            }
        if (
            value.get("status") == "failed"
            and set(value) == {"status", "code", "request_digest"}
            and value["code"] == "no-commit"
            and value["request_digest"] == expected
        ):
            return {"status": "failed"}
        return {"status": "unknown"}

    def execute(self, session, digest):
        self._proposal(session, digest)
        proposal = self.approvals.consume(session, digest)
        try:
            value = self.store(
                scope=self.scope,
                operation_id=proposal["operation_id"],
                observation=proposal["observation"],
            )
            result = self._outcome(value, proposal)
        except Exception:
            result = {"status": "unknown"}
        try:
            self.approvals.finish(session, digest, result["status"])
        except Exception:
            return {"status": "unknown"}
        return result

    def store_trusted(self, session, *, operation_id, observation):
        """Explicit owner-configured Store, retaining exact payload and one-send custody.

        Channel tool admission is still enforced by the calling runtime. The
        trusted policy replaces only the additional operator-CLI review step.
        """
        if self.policy != "trusted":
            raise ContractError("trusted memory writes not configured")
        now = self.approvals.clock()
        digest = self.propose(session, operation_id=operation_id, observation=observation,
                              origin="configured-trusted-store", expires_at=now + 300)
        self.approvals.approve(session, digest, approval_expires=now + 300)
        return {**self.execute(session, digest), "proposal_digest": digest}

    def reconcile_only(self, session, digest):
        row = self._proposal(session, digest)
        if row["state"] not in ("in-flight", "unknown", "succeeded", "failed"):
            raise ContractError("memory proposal not sent")
        proposal = row["proposal"]
        expected = digest_json(
            {"version": 1, "scope": self.scope, "observation": proposal["observation"]}
        )
        try:
            value = self.reconcile(
                scope=self.scope, operation_id=proposal["operation_id"], request_digest=expected
            )
            result = self._outcome(value, proposal)
        except Exception:
            result = {"status": "unknown"}
        if row["state"] in ("unknown", "in-flight"):
            try:
                self.approvals.resolve(session, digest, result["status"])
            except Exception:
                return {"status": "unknown"}
        return result
