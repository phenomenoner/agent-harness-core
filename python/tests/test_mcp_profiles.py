import os
import sys
import time
from dataclasses import replace

import pytest
from test_mcp_stdio import SERVER
from test_runtime_core import setup

from ahc.domain.mcp_profiles import McpStdioProfile
from ahc.domain.sessions import ContractError
from ahc.infrastructure.mcp_composition import connect_mcp_profile
from ahc.infrastructure.mcp_receipts import McpReceipts


@pytest.mark.skipif(os.name != "nt", reason="Windows owned stdio")
@pytest.mark.parametrize("default_validator", [False, True])
def test_profile_discover_pin_invoke_and_durable_receipt(tmp_path, default_validator):
    if default_validator:
        pytest.importorskip("jsonschema")
    store, current, request = setup(tmp_path)
    store.begin(current, request)
    env = {k: os.environ[k] for k in ("SystemRoot", "WINDIR") if k in os.environ}
    server = SERVER.replace("'inputSchema':{'type':'object'}",
        "'inputSchema':{'type':'object','properties':{'text':{'type':'string'}},"
        "'required':['text'],'additionalProperties':False}")
    profile = McpStdioProfile("fixture", current.session.agent_id,
        (sys.executable, "-u", "-c", server, "echo"), str(tmp_path), tuple(env),
        ("echo",), None, True)
    assert McpStdioProfile.from_dict(profile.to_dict()) == profile
    options = {"environment": env, "validate": lambda s, a: type(a.get("text")) is str}
    if default_validator:
        options.pop("validate")
    client, discovery = connect_mcp_profile(profile, store, request,
        deadline=time.monotonic() + 5, **options)
    try:
        assert client.invoke(current.session, "invalid", "echo", {},
                             deadline=time.monotonic() + 5)["status"] == "rejected"
        assert client.invoke(current.session, "call", "echo", {"text": "hi"},
                             deadline=time.monotonic() + 5)["status"] == "rejected"
    finally:
        client.close()
    pinned = replace(profile, capability_digest=discovery["capability_digest"])
    assert pinned.digest != profile.digest
    client, _ = connect_mcp_profile(pinned, store, request,
                                   deadline=time.monotonic() + 5, **options)
    try:
        assert client.invoke(current.session, "call", "echo", {"text": "hi"},
                             deadline=time.monotonic() + 5)["status"] == "observed"
    finally:
        client.close()
    assert McpReceipts(store, request).read()[0]["status"] == "observed"
    for bad in (replace(pinned, enabled=False), replace(pinned, agent_id="foreign")):
        with pytest.raises(ContractError):
            connect_mcp_profile(bad, store, request, deadline=time.monotonic() + 5, **options)
    with pytest.raises(ContractError):
        connect_mcp_profile(pinned, store, request, environment={"unexpected": "PRIVATE"},
                            validate=options.get("validate"), deadline=time.monotonic() + 5)
