"""Exact native AAR read-context binding; no discovery or mutation authority."""

import re
from dataclasses import dataclass

from ahc.domain.sessions import ContractError
from ahc.domain.skill_values import digest_json

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._~-]{0,127}\Z")
NATIVE_TOOLS = frozenset({
    "aar_capabilities", "aar_rlm_execute", "aar_rlm_status",
    "aar_operation_status", "aar_operation_cancel", "aar_operation_reconcile",
    "aar_artifact_resolve",
})


def _positive(value):
    if type(value) is not int or value <= 0:
        raise ContractError("AAR positive integer required")
    return value


@dataclass(frozen=True, slots=True)
class NativeAarPin:
    """Owner-supplied expected identity, never learned by accepting a response."""

    tool_surface_digest: str
    schema_bundle_digest: str
    operation_skill_digest: str
    capability_digest: str
    package_version: str = "0.4.0a4"
    tool_surface_version: str = "aar.mcp-tools.v7"

    def __post_init__(self):
        for value in (self.tool_surface_digest, self.schema_bundle_digest,
                      self.operation_skill_digest, self.capability_digest):
            if type(value) is not str or not _DIGEST.fullmatch(value):
                raise ContractError("AAR pin digest invalid")
        if (self.package_version != "0.4.0a4"
                or self.tool_surface_version != "aar.mcp-tools.v7"):
            raise ContractError("AAR native version unsupported")

    def bind(self, report, *, principal_id, session_id):
        for value in (principal_id, session_id):
            if type(value) is not str or not _IDENTITY.fullmatch(value):
                raise ContractError("AAR host identity invalid")
        if type(report) is not dict or report.get("server_name") != "aar-mcp":
            raise ContractError("AAR native surface required")
        for field in ("tool_surface_digest", "schema_bundle_digest",
                      "operation_skill_digest", "package_version", "tool_surface_version"):
            if report.get(field) != getattr(self, field):
                raise ContractError("AAR native pin mismatch")
        tools = report.get("tool_names")
        if (type(tools) is not list or any(type(x) is not str for x in tools)
                or len(tools) != len(set(tools)) or not NATIVE_TOOLS.issubset(tools)):
            raise ContractError("AAR native lifecycle tools unavailable")
        ready = report.get("ready")
        if type(ready) is not dict:
            raise ContractError("AAR ready missing")
        capabilities = ready.get("capabilities")
        if (type(capabilities) is not dict
                or set(capabilities) != {"schema_version", "capabilities", "digest"}
                or capabilities["schema_version"] != "aar.runtime.v1"
                or capabilities["digest"] != self.capability_digest):
            raise ContractError("AAR capability pin mismatch")
        payload = {key: capabilities[key] for key in ("schema_version", "capabilities")}
        if "sha256:" + digest_json(payload) != self.capability_digest:
            raise ContractError("AAR capability bytes mismatch")
        return NativeAarBinding(self, _positive(ready.get("runtime_generation")),
                                _positive(report.get("server_now_unix_ms")),
                                principal_id, session_id)


@dataclass(frozen=True, slots=True)
class NativeAarBinding:
    pin: NativeAarPin
    runtime_generation: int
    server_now_unix_ms: int
    principal_id: str
    session_id: str

    def read_context(self, *, deadline_unix_ms):
        """Build the upstream McpReadContext, not a grant-bearing mutation context."""
        _positive(deadline_unix_ms)
        if not self.server_now_unix_ms < deadline_unix_ms <= self.server_now_unix_ms + 60000:
            raise ContractError("AAR read deadline outside bound")
        return {"runtime_generation": self.runtime_generation,
                "capability_digest": self.pin.capability_digest,
                "principal_id": self.principal_id, "session_id": self.session_id,
                "deadline_unix_ms": deadline_unix_ms}

    def require_same_runtime(self, report):
        current = self.pin.bind(report, principal_id=self.principal_id,
                                session_id=self.session_id)
        if current.runtime_generation != self.runtime_generation:
            raise ContractError("AAR runtime generation changed")
        return current
