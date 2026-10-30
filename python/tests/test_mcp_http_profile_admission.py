from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.domain.mcp_http_profiles import McpHttpProfile
from ahc.domain.sessions import ContractError
from ahc.infrastructure import mcp_http_composition


def test_profile_rejections_precede_transport(tmp_path, monkeypatch):
    store, current, request = setup(tmp_path)
    store.begin(current, request)
    profile = McpHttpProfile("http", current.session.agent_id, "http://127.0.0.1:1/mcp",
                             "credential", ("echo",), "a" * 64, True)
    launched = []

    def forbidden(*args, **kwargs):
        launched.append(True)
        raise AssertionError("transport must not be constructed")

    monkeypatch.setattr(mcp_http_composition, "McpHttpTransport", forbidden)
    for candidate in (profile, replace(profile, enabled=False),
                      replace(profile, agent_id="other")):
        with pytest.raises(ContractError):
            mcp_http_composition.connect_mcp_http_profile(candidate, store, request,
                deadline=10, validate=lambda *a: True, resolve=lambda *a: None)
    assert launched == []
