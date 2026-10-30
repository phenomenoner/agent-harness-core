"""File-aware event decoding retains route filtering and cannot silently lose files."""

from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.adapters.channel_events import decode_channel_event
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_inbox import RuntimeInbox


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("caption", [None, "Read this report"])
def test_file_event_roundtrip_and_admission_guard(tmp_path, platform, caption):
    repo, snapshot, _ = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", snapshot.session)
    if platform == "telegram":
        payload = {"update_id": 1, "message": {
            "message_id": 2, "chat": {"id": 456}, "from": {"id": 789, "is_bot": False},
            "document": {"file_id": "2", "file_size": 4, "file_name": "report.txt"},
            "caption": caption}}
    else:
        payload = {"op": 0, "t": "MESSAGE_CREATE", "d": {
            "id": "2", "channel_id": "456", "author": {"id": "789"}, "type": 0,
            "content": caption or "", "attachments": [{"id": "3", "size": 4,
                "filename": "report.txt",
                "url": "https://cdn.discordapp.com/attachments/456/3/report.txt?ex=fixture"}]}}
    event = decode_channel_event(route, payload, include_files=True)
    assert event.input_text == (caption or "Please examine the attached files.")
    assert len(event.files) == 1
    assert ChannelInboundEvent.from_dict(event.to_dict()) == event
    assert "ex=fixture" not in repr(event)
    assert decode_channel_event(replace(route, user_id="999"), payload,
                                include_files=True) is None
    assert decode_channel_event(replace(route, channel_id="999"), payload,
                                include_files=True) is None
    inbox = RuntimeInbox(repo)
    with pytest.raises(ContractError, match="durable import"):
        inbox.submit(route.session, event.operation_id, snapshot.goal.digest,
                     event.input_text, channel_event=event)
    assert inbox.next_pending(route.session) is None
    with pytest.raises(ContractError, match="files invalid"):
        replace(event, route=replace(route, channel_id="999"))
