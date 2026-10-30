from dataclasses import replace

import pytest
from test_channel_outbox import accepted
from test_runtime_core import setup

from ahc.adapters.channel_text import BotTextTransport
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("wrong", [False, True])
def test_platform_receipt_drives_durable_outbox_without_retry(tmp_path, platform, wrong):
    repo, snapshot, template = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", snapshot.session)
    event = ChannelInboundEvent(route, "event", "hello")
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    request = replace(template, operation_id=event.operation_id,
                      goal_digest=snapshot.goal.digest, input_text=event.input_text)
    accepted(repo, snapshot, request)
    calls = []

    def post(platform, token, channel, body):
        calls.append(body)
        assert token == "fixture" and channel == "456"
        returned_channel = "999" if wrong else "456"
        if platform == "telegram":
            assert body["text"] == "answer" and body["parse_mode"] == "HTML"
            return {"ok": True, "result": {"message_id": 1, "chat": {"id": int(returned_channel)},
                                            "from": {"id": 123}}}
        assert body["allowed_mentions"] == {"parse": []}
        return {"id": "1", "channel_id": returned_channel, "author": {"id": "123"}}

    transport = BotTextTransport(route, "fixture", post=post)
    delivery = ChannelDelivery(ChannelOutbox(repo), route, transport)
    result = delivery.deliver(request.operation_id, "claim")
    assert result["state"] == ("unknown" if wrong else "sent")
    with pytest.raises(ContractError):
        delivery.deliver(request.operation_id, "claim-again")
    assert len(calls) == 1
    with pytest.raises(ContractError):
        transport.send(replace(route, account_id="other"), {"output_text": "x"}, "op")
    with pytest.raises(ContractError):
        transport.send(route, {"output_text": "\ud800"}, "op")
    assert len(calls) == 1
