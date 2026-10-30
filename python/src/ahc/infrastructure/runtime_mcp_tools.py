"""Lazy per-call MCP connection within an already admitted foreground turn."""

import time
from ahc.domain.mcp_http_profiles import McpHttpProfile
from ahc.domain.mcp_profiles import McpStdioProfile
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity

from .mcp_composition import connect_mcp_profile
from .mcp_http_composition import connect_mcp_http_profile
from .runtime_execution_scope import workspace_request


class RuntimeMcpTools:
    def __init__(self, store, profiles, *, environments=None, resolve=None,
                 cancelled=lambda: False, protection=None):
        if (type(profiles) is not tuple or not 1 <= len(profiles) <= 16
                or any(type(p) not in (McpStdioProfile, McpHttpProfile) for p in profiles)
                or len({p.server_id for p in profiles}) != len(profiles)
                or (protection is not None and any(type(p) is not McpStdioProfile
                                                   for p in profiles))):
            raise ContractError("runtime MCP profiles invalid")
        self.store = store
        self.profiles = {p.server_id: p for p in profiles}
        self.environments = {k: dict(v) for k, v in (environments or {}).items()}
        self.resolve, self.cancelled = resolve, cancelled
        self.protection = protection

    def invoke(self, identity, arguments):
        try:
            if (type(arguments) is not dict or set(arguments) !=
                    {"expected", "server_id", "tool", "call_id", "arguments"}
                    or WorkspaceIdentity.from_dict(arguments["expected"]) != identity
                    or type(arguments["server_id"]) is not str):
                raise ContractError("runtime MCP input invalid")
            request = workspace_request(self.store, identity.session, identity.workspace_id)
            profile = self.profiles.get(arguments["server_id"])
            if (profile is None or not profile.enabled or profile.capability_digest is None
                    or profile.agent_id != request.session.agent_id
                    or arguments["tool"] not in profile.allowed_tools):
                raise ContractError("runtime MCP profile unavailable")
        except (ContractError, TypeError, KeyError):
            return {"disposition": "rejected", "reason": "mcp-admission"}
        client = None
        # Each MCP operation retains its explicit transport cap after parent renewal.
        cap = 3600 if type(profile) is McpStdioProfile else 300
        operation_deadline = min(request.effective_deadline, time.monotonic() + cap)
        try:
            if type(profile) is McpStdioProfile:
                client, _ = connect_mcp_profile(profile, self.store, request,
                    environment=self.environments.get(profile.server_id, {}),
                    deadline=operation_deadline, cancelled=self.cancelled,
                    **({"protection": self.protection} if self.protection is not None else {}))
            else:
                client, _ = connect_mcp_http_profile(profile, self.store, request,
                    resolve=self.resolve, deadline=operation_deadline, cancelled=self.cancelled)
            try:
                result = client.invoke(request.session, arguments["call_id"], arguments["tool"],
                    arguments["arguments"], deadline=operation_deadline, cancelled=self.cancelled)
            finally:
                client.close()
            return {"disposition": "unknown" if result["status"] == "unknown" else
                    "rejected" if result["status"] == "rejected" else "observed",
                    "result": result, "unresolved_effects": result["status"] == "unknown"}
        except Exception:
            return {"disposition": "unknown", "reason": "mcp-operation-unverified",
                    "unresolved_effects": True}
