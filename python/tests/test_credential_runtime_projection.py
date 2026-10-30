"""Sentinel credentials at real foreground/transport persistence boundaries."""

import json
import traceback
from dataclasses import replace

import pytest
from test_channel_outbox import accepted
from test_foreground_mcp_config import configured
from test_runtime_core import setup

from ahc.adapters.channel_text import BotTextTransport
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_mcp import compose_mcp
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError, VirtualSessionIdentity
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository

SENTINEL = "AHC-TEST-SECRET-NEVER-A-REAL-CREDENTIAL"


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_transport_credential_exception_is_absent_from_traceback_and_durable_state(
        tmp_path, platform, capsys):
    repo, snapshot, template = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", snapshot.session)
    event = ChannelInboundEvent(route, "event", "hello")
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text)
    accepted(repo, snapshot, request)
    calls = []

    def post(platform, token, channel, body):
        calls.append(channel)
        assert token == SENTINEL
        raise RuntimeError("transport private detail: " + token)

    transport = BotTextTransport(route, SENTINEL, post=post)
    with pytest.raises(ContractError) as caught:
        transport.send(route, {"output_text": "answer"}, "direct-probe")
    assert SENTINEL not in "".join(traceback.format_exception(caught.value))
    result = ChannelDelivery(ChannelOutbox(repo), route, transport).deliver(
        request.operation_id, "claim")
    assert result["state"] == "unknown"
    fresh = RuntimeRepository(repo.root)
    assert ChannelOutbox(fresh).read(route)[0] == result
    with fresh._read() as connection:
        assert SENTINEL not in "\n".join(connection.iterdump())
    assert SENTINEL not in json.dumps(result)
    captured = capsys.readouterr()
    assert SENTINEL not in captured.out + captured.err
    assert len(calls) == 2  # One direct error probe and one durable dispatch.


@pytest.mark.parametrize("axis", ["agent", "route", "generation", "reference"])
def test_mcp_reference_resolution_rejects_foreign_owner_without_projecting_secret(
        monkeypatch, axis):
    pytest.importorskip("jsonschema")
    monkeypatch.setenv("AHC_TEST_MCP_TOKEN", SENTINEL)
    config = parse_foreground_config_v2(configured())
    service = compose_mcp(config, object(), cancelled=lambda: False)
    owner, reference = config.session, "token"
    if axis == "generation":
        owner = owner.successor()
    elif axis == "reference":
        reference = "other"
    else:
        owner = VirtualSessionIdentity.initial("session",
            "other" if axis == "agent" else owner.agent_id,
            "other" if axis == "route" else owner.route_id)
    with pytest.raises(ContractError) as caught:
        service.resolve(reference, owner)
    assert SENTINEL not in str(caught.value)
    assert SENTINEL not in json.dumps(config.preview())
    assert SENTINEL not in json.dumps(config.to_dict())
    assert service.resolve("token", config.session) == SENTINEL
