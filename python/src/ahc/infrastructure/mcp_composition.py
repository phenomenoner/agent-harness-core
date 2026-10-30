"""Explicit stdio profile composition; no ambient credential lookup or activation."""

import math
import time

from ahc.adapters.mcp_stdio import McpStdioTransport
from ahc.application.mcp_client import McpClient
from ahc.domain.mcp_profiles import McpStdioProfile
from ahc.domain.sessions import ContractError

from .mcp_receipts import McpReceipts


def connect_mcp_profile(profile, store, request, *, environment, deadline, validate=None,
                        cancelled=lambda: False, protection=None):
    if (type(profile) is not McpStdioProfile or not profile.enabled
            or request.session.agent_id != profile.agent_id
            or type(environment) is not dict
            or set(environment) != set(profile.environment_names)
            or any(type(v) is not str or "\0" in v for v in environment.values())
            or (validate is not None and not callable(validate)) or cancelled()):
        raise ContractError("MCP profile unavailable or environment absent")
    if validate is None:
        from .mcp_schema import McpSchemaValidator

        validate = McpSchemaValidator()
    receipts = McpReceipts(store, request, profile_digest=profile.digest)
    with store._read() as connection:
        receipts._parent(connection, active=True)
    options = {}
    if protection is not None:
        remaining = deadline - time.monotonic()
        if not 0 < remaining <= 3600:
            raise ContractError("protected MCP deadline unavailable")
        options = {"protection": protection, "lifetime_seconds": math.ceil(remaining)}
    transport = McpStdioTransport.launch(profile.command, cwd=profile.cwd, env=environment,
                                         **options)

    def authorize(session, server_id, tool, capability_digest):
        return (session == request.session and session.agent_id == profile.agent_id
                and server_id == profile.server_id and tool in profile.allowed_tools
                and profile.capability_digest is not None
                and profile.capability_digest == capability_digest)

    client = McpClient(request.session, profile.server_id, transport, authorize=authorize,
                       validate=validate, receipts=receipts)
    try:
        discovery = client.discover(deadline=deadline, cancelled=cancelled)
        return client, {"profile_digest": profile.digest, **discovery}
    except BaseException:
        client.close()
        raise
