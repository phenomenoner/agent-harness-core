import json
from dataclasses import replace

import pytest
from test_channel_outbox import accepted
from test_runtime_core import setup

from ahc.adapters.channel_text import BotTextTransport
from ahc.cli import main, runtime_channels
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_operator_channel_delivery_and_missing_credential(tmp_path, monkeypatch, capsys, platform):
    repo, snapshot, template = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", snapshot.session)
    event = ChannelInboundEvent(route, "event", "hello")
    common = ["--runtime-root", str(repo.root), "--route-json", json.dumps(route.to_dict())]
    submit = ["runtime-channel-submit", *common, "--event-id", event.event_id,
              "--input-text", event.input_text, "--goal-digest", snapshot.goal.digest]
    assert main(submit) == 0
    first = json.loads(capsys.readouterr().out)["submission"]
    assert first["operation_id"] == event.operation_id
    assert main(submit) == 0
    assert json.loads(capsys.readouterr().out)["submission"] == first
    changed = list(submit)
    changed[changed.index("--input-text") + 1] = "changed"
    assert main(changed) == 2
    capsys.readouterr()
    request = replace(template, operation_id=event.operation_id,
                      goal_digest=snapshot.goal.digest, input_text=event.input_text)
    accepted(repo, snapshot, request)
    send = ["runtime-channel-send", *common, "--operation-id", request.operation_id,
            "--token-env", "AHC_TEST_CHANNEL_TOKEN"]
    monkeypatch.delenv("AHC_TEST_CHANNEL_TOKEN", raising=False)
    assert main(send) == 2
    assert json.loads(capsys.readouterr().out)["error"]["kind"] == "contract"
    assert main(["runtime-channel-outbox", *common]) == 0
    assert json.loads(capsys.readouterr().out)["outbox"][0]["state"] == "pending"
    calls = []

    def post(platform, token, channel, body):
        calls.append(body)
        assert token == "fixture-private"
        if platform == "telegram":
            return {"ok": True, "result": {"message_id": 1, "chat": {"id": 456},
                                            "from": {"id": 123}}}
        return {"id": "1", "channel_id": "456", "author": {"id": "123"}}

    monkeypatch.setattr(runtime_channels, "BotTextTransport",
                        lambda route, token: BotTextTransport(route, token, post=post))
    monkeypatch.setenv("AHC_TEST_CHANNEL_TOKEN", "fixture-private")
    assert main(send) == 0
    output = capsys.readouterr().out
    assert "fixture-private" not in output
    assert json.loads(output)["delivery"]["state"] == "sent"
    assert main(send) == 2
    assert "fixture-private" not in capsys.readouterr().out
    assert len(calls) == 1
