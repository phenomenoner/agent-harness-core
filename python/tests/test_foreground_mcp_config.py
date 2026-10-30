import json

import pytest
from test_foreground_config_v2 import valid_config

from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_mcp import compose_mcp
from ahc.domain.mcp_http_profiles import McpHttpProfile
from ahc.domain.sessions import ContractError


def configured():
    value = valid_config()
    profile = McpHttpProfile("http", "agent", "https://example.invalid/private-endpoint",
                             "token", ("echo",), "a" * 64, True)
    value["mcp"] = {"profiles": [profile.to_dict()],
                    "credential_environment": {"token": "AHC_TEST_MCP_TOKEN"}}
    return value


def test_config_digest_preview_and_explicit_environment(monkeypatch):
    pytest.importorskip("jsonschema")
    config = parse_foreground_config_v2(configured())
    baseline = parse_foreground_config_v2(valid_config())
    assert config.configuration_digest != baseline.configuration_digest
    assert "private-endpoint" not in json.dumps(config.preview())
    monkeypatch.setenv("AHC_TEST_MCP_TOKEN", "Bearer PRIVATE")
    service = compose_mcp(config, object(), cancelled=lambda: False)
    assert service.resolve("token", config.session) == "Bearer PRIVATE"
    assert "PRIVATE" not in json.dumps(config.to_dict())
    assert "PRIVATE" not in json.dumps(config.preview())
    assert compose_mcp(parse_foreground_config_v2(valid_config()), object(),
                       cancelled=lambda: False) is None


@pytest.mark.parametrize("field,value", [("agent_id", "other"), ("capability_digest", None)])
def test_config_rejects_unbound_enabled_profile(field, value):
    config = configured()
    config["mcp"]["profiles"][0][field] = value
    with pytest.raises(ContractError):
        parse_foreground_config_v2(config)
