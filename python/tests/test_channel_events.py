import copy

import pytest
from test_runtime_core import setup

from ahc.adapters.channel_events import decode_channel_event
from ahc.application.channel_ingress import ChannelIngress
from ahc.domain.channel_ingress import ChannelRoute
from ahc.infrastructure.runtime_inbox import RuntimeInbox


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_platform_event_admission_and_route_filtering(tmp_path, platform):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", snapshot.session)
    if platform == "telegram":
        payload = {"update_id": 1, "message": {"message_id": 2, "chat": {"id": 456},
                   "from": {"id": 789, "is_bot": False}, "text": "hello"}}
    else:
        payload = {"op": 0, "t": "MESSAGE_CREATE", "d": {"id": "2", "channel_id": "456",
                   "author": {"id": "789"}, "type": 0, "content": "hello"}}
    before = copy.deepcopy(payload)
    event = decode_channel_event(route, payload)
    inbox = RuntimeInbox(repo)
    ingress = ChannelIngress(inbox, route)
    first = ingress.submit(event, snapshot.goal.digest)
    assert ingress.submit(decode_channel_event(route, payload), snapshot.goal.digest) == first
    assert inbox.next_pending(route.session)["input_text"] == "hello"
    assert payload == before
    for change in ("user", "channel", "bot", "empty", "edit"):
        other = copy.deepcopy(payload)
        if platform == "telegram":
            message = other["message"]
            if change == "user":
                message["from"]["id"] = 999
            elif change == "channel":
                message["chat"]["id"] = 999
            elif change == "bot":
                message["from"]["is_bot"] = True
            elif change == "empty":
                message.pop("text")
            else:
                other["edited_message"] = other.pop("message")
        else:
            message = other["d"]
            if change == "user":
                message["author"]["id"] = "999"
            elif change == "channel":
                message["channel_id"] = "999"
            elif change == "bot":
                message["author"]["bot"] = True
            elif change == "empty":
                message["content"] = ""
            else:
                other["t"] = "MESSAGE_UPDATE"
        assert decode_channel_event(route, other) is None
