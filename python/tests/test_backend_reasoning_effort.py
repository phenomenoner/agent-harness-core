"""Configured effort reaches the wire and participates in request identity."""

from dataclasses import replace

import pytest
from test_codex_client import Peer, setup
from test_foreground_config_v2 import valid_config

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize("model", ["gpt-5.6-luna", "gpt-6-luna", "gpt-6-astra", "gpt-6.1-sol"])
def test_max_configuration_reaches_client_and_turn_start(model):
    raw = valid_config()
    raw["backend"].update(model=model, reasoning_effort="max")
    config = parse_foreground_config_v2(raw)
    factory = TrustedCodexBackendFactory(None, config)
    bound = factory._client(None, None, lambda: False)
    assert bound.model == model and bound.reasoning_effort == "max"
    peer, baseline, request = setup()
    peer.messages[1]["result"]["model"] = model
    client = CodexProtocolClient(peer, cwd="workspace", model=model,
                                 reasoning_effort="max", clock=lambda: 0)
    default = CodexProtocolClient(Peer(), cwd="workspace", model=model, clock=lambda: 0)
    assert client.configuration_digest != default.configuration_digest
    outcome = client.run_admitted(
        replace(request, configuration_digest=client.configuration_digest))
    assert outcome.disposition == "completed"
    assert next(m for m in peer.sent if m["method"] == "thread/start")["params"][
        "model"] == model
    assert next(m for m in peer.sent if m["method"] == "turn/start")["params"]["effort"] == "max"


@pytest.mark.parametrize("effort", [None, "", True, "max\n", "a" * 65])
def test_invalid_explicit_effort_rejects_inertly(effort):
    raw = valid_config()
    raw["backend"]["reasoning_effort"] = effort
    with pytest.raises(ContractError):
        parse_foreground_config_v2(raw)
