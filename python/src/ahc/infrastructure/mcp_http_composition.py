"""HTTP profile composition with parent-bound receipts and explicit credential resolution."""

from ahc.adapters.mcp_http import McpHttpTransport
from ahc.application.mcp_client import McpClient
from ahc.domain.mcp_http_profiles import McpHttpProfile
from ahc.domain.sessions import ContractError

from .mcp_receipts import McpReceipts
from .mcp_schema import McpSchemaValidator


def connect_mcp_http_profile(profile, store, request, *, deadline, resolve=None,
                             validate=None, cancelled=lambda: False):
    if (type(profile) is not McpHttpProfile or not profile.enabled
            or request.session.agent_id != profile.agent_id or cancelled()
            or (validate is not None and not callable(validate))):
        raise ContractError("MCP HTTP profile unavailable")
    receipts = McpReceipts(store, request, profile_digest=profile.digest)
    with store._read() as connection:
        receipts._parent(connection, active=True)
    validator = McpSchemaValidator() if validate is None else validate
    authorization = None
    if profile.authorization_ref is not None:
        try:
            authorization = resolve(profile.authorization_ref, request.session)
            if type(authorization) is not str or not authorization:
                raise ValueError()
        except Exception:
            raise ContractError("MCP HTTP credential unavailable") from None
    transport = McpHttpTransport(profile.endpoint, authorization=authorization)

    def authorize(session, server_id, tool, capability_digest):
        return (session == request.session and server_id == profile.server_id
                and tool in profile.allowed_tools and profile.capability_digest is not None
                and capability_digest == profile.capability_digest)

    client = McpClient(request.session, profile.server_id, transport, authorize=authorize,
                       validate=validator, receipts=receipts)
    try:
        discovery = client.discover(deadline=deadline, cancelled=cancelled)
        return client, {"profile_digest": profile.digest, **discovery}
    except BaseException:
        client.close()
        raise
