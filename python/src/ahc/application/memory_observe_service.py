"""Profile-authorized exact observation through supplied public provider ports."""

import json
import re

from ahc.domain.capability_values import (
    ApprovalMode,
    CapabilityDefinition,
    DecisionKind,
    EffectClass,
)
from ahc.domain.memory_pack import _text
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.profile_values import ProfileResourceKind
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.domain.skill_values import canonical_json

from .capability_composition import compose_capability_views


class MemoryObserveUnavailable(RuntimeError):
    pass


class MemoryObserveService:
    def __init__(
        self, *, registry, agent_id, resource_id, scope, grants, status, get, resolve=None
    ):
        if type(registry) is not AgentProfileRegistry:
            raise ContractError("memory observe registry invalid")
        profile = registry.profile(agent_id)
        if (
            profile.durable_memory_ref != resource_id
            or not callable(status)
            or not callable(get)
            or type(scope) is not str
            or not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]{0,255}", scope)
        ):
            raise ContractError("memory observe binding invalid")
        definitions = tuple(
            CapabilityDefinition(
                "memory.observe." + operation,
                ProfileResourceKind.DURABLE_MEMORY,
                EffectClass.READ_ONLY,
                ApprovalMode.NONE,
            )
            for operation in ("status", "get")
        )
        self._view = compose_capability_views(
            profile_registry=registry, definitions=definitions, grants=grants
        ).view(agent_id)
        self._resolve = resolve
        self._agent, self._scope = agent_id, scope
        self._resource, self._status, self._get = resource_id, status, get

    def _admit(self, session, operation):
        if (
            type(session) is not VirtualSessionIdentity
            or session.agent_id != self._agent
            or self._view.decide(
                "memory.observe." + operation, self._resource, EffectClass.READ_ONLY
            ).kind
            is not DecisionKind.ALLOW
        ):
            raise ContractError("memory observe not admitted")

    def resolve(self, session, row_id):
        self._admit(session, "get")
        if type(row_id) is not int or not 0 < row_id < 2**63:
            raise ContractError("memory citation invalid")
        try:
            if not callable(self._resolve):
                raise ValueError()
            value = self._resolve(scope=self._scope, row_id=row_id)
            if value == {"status": "not-found"}:
                return value
            if (type(value) is not dict or set(value) != {"status", "record_ref"}
                    or value["status"] != "succeeded"):
                raise ValueError()
            _text(value["record_ref"], 256)
            return dict(value)
        except Exception:
            raise MemoryObserveUnavailable("memory citation unavailable") from None

    def status(self, session):
        self._admit(session, "status")
        try:
            value = self._status()
            if type(value) is not dict:
                raise ValueError()
            schema = value.get("schema_version")
            fields = {"schema_version", "status", "code", "capabilities"}
            if schema == "ahc.memory.native.status.v3":
                fields |= {"observation_scope", "integrity"}
                if (value.get("observation_scope") != "operational"
                        or value.get("integrity") != "not-checked"):
                    raise ValueError()
            elif schema not in ("openclaw-mem.native.status.v1", "ahc.memory.native.status.v2"):
                raise ValueError()
            if (
                set(value) != fields
                or value["status"] not in ("ready", "unavailable", "initialization-required")
            ):
                raise ValueError()
            codes = {
                "invalid-locator",
                "storage-absent",
                "invalid-storage",
                "unsupported-storage",
                "coordinated-inspection-required",
                "storage-unverified",
                "native-keyword-ready",
                "native-schema-absent",
                "native-identity-invalid",
                "native-schema-supported",
                "native-write-api-not-implemented",
            }
            if value["code"] not in codes:
                raise ValueError()
            caps = value["capabilities"]
            if (
                type(caps) is not dict
                or set(caps)
                != {"observe_status", "observe_get", "store_committed", "reconcile_store"}
                or any(type(v) is not bool for v in caps.values())
                or (
                    value["status"] == "ready"
                    and (not all(caps.values()) or value["code"] not in (
                        "native-schema-supported","native-keyword-ready"))
                )
                or (value['code']=='native-keyword-ready' and schema!='ahc.memory.native.status.v3')
            ):
                raise ValueError()
            return json.loads(canonical_json(value))
        except Exception:
            raise MemoryObserveUnavailable("memory status unavailable") from None

    def get(self, session, record_ref):
        self._admit(session, "get")
        _text(record_ref, 256)
        try:
            value = self._get(scope=self._scope, record_ref=record_ref)
            if type(value) is not dict:
                raise ValueError()
            if value == {"status": "not-found"}:
                return {"status": "not-found"}
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
                or value["status"] != "succeeded"
                or value["record_ref"] != record_ref
                or value["trust"] != "untrusted"
                or value["provenance"] != {"origin": "unknown"}
            ):
                raise ValueError()
            digest = value["request_digest"]
            if type(digest) is not str or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError()
            trace, record = value["trace"], value["record"]
            if (
                type(trace) is not dict
                or set(trace) != {"operation_id", "request_digest"}
                or trace["request_digest"] != digest
                or type(record) is not dict
                or set(record) != {"kind", "summary", "detail", "created_at"}
                or type(record["detail"]) is not dict
                or record["detail"].get("scope") != self._scope
            ):
                raise ValueError()
            _text(trace["operation_id"], 256)
            for name, bound in (("kind", 4096), ("summary", 65536), ("created_at", 128)):
                _text(record[name], bound)
            encoded = canonical_json(value)
            if len(encoded) > 131072:
                raise ValueError()
            result = json.loads(encoded)
            result["content_authority"] = "untrusted-data"
            return result
        except Exception:
            raise MemoryObserveUnavailable("memory record unavailable") from None
