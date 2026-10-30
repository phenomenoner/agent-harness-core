"""Explicit optional MCP configuration; preview never copies launch/auth details."""

import os

from ahc.domain.mcp_http_profiles import McpHttpProfile
from ahc.domain.mcp_profiles import McpStdioProfile
from ahc.domain.sessions import ContractError, _identifier


def _profiles(values):
    if type(values) is not list or not 1 <= len(values) <= 16:
        raise ContractError("foreground MCP profiles invalid")
    result = []
    for value in values:
        if type(value) is not dict:
            raise ContractError("foreground MCP profile invalid")
        cls = (McpHttpProfile if value.get("schema_version") == McpHttpProfile.schema_version
               else McpStdioProfile)
        result.append(cls.from_dict(value))
    if len({p.server_id for p in result}) != len(result):
        raise ContractError("foreground MCP duplicate server")
    return tuple(result)


def parse_mcp(raw, session):
    if "mcp" not in raw:
        return {}
    value = raw["mcp"]
    if type(value) is not dict or set(value) != {"profiles", "credential_environment"}:
        raise ContractError("foreground MCP fields invalid")
    profiles = _profiles(value["profiles"])
    if any(p.agent_id != session.agent_id or (p.enabled and p.capability_digest is None)
           for p in profiles):
        raise ContractError("foreground MCP requires matching agent and pinned capability")
    bindings = value["credential_environment"]
    if type(bindings) is not dict or len(bindings) > 16:
        raise ContractError("foreground MCP credential bindings invalid")
    for ref, name in bindings.items():
        _identifier(ref, "MCP credential")
        if type(name) is not str or not name or len(name) > 128 or any(c in name for c in "=\0"):
            raise ContractError("foreground MCP credential environment invalid")
    return {"mcp": {"profiles": [p.to_dict() for p in profiles],
                    "credential_environment": dict(bindings)}}


def preview_mcp(config):
    value = config.to_dict().get("mcp")
    if value is None:
        return {}
    return {"mcp": {"servers": [{"server_id": p.server_id, "enabled": p.enabled,
        "profile_digest": p.digest, "allowed_tools": list(p.allowed_tools)}
        for p in _profiles(value["profiles"])]}}


def compose_mcp(config, store, *, cancelled):
    value = config.to_dict().get("mcp")
    if value is None:
        return None
    profiles = tuple(p for p in _profiles(value["profiles"]) if p.enabled)
    if not profiles:
        return None
    from ahc.infrastructure.mcp_schema import McpSchemaValidator
    from ahc.infrastructure.runtime_mcp_tools import RuntimeMcpTools

    McpSchemaValidator()  # Dependency readiness before any workspace/server startup.
    environments = {p.server_id: {name: os.environ[name] for name in p.environment_names
        if name in os.environ} for p in profiles if type(p) is McpStdioProfile}
    credentials = {ref: os.environ.get(name)
                   for ref, name in value["credential_environment"].items()}

    def resolve(ref, session):
        if session != config.session or not credentials.get(ref):
            raise ContractError("MCP credential unavailable")
        return credentials[ref]

    options = {}
    if "protection" in config.backend:
        from .foreground_backend_protection import native_backend_protection

        options["protection"] = native_backend_protection(config)
    return RuntimeMcpTools(store, profiles, environments=environments,
                           resolve=resolve, cancelled=cancelled, **options)
