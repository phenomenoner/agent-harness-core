"""Inbound metadata cannot select a foreign host/channel or oversized artifact."""

from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.adapters.channel_file_refs import decode_file_refs
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_bounded_attachment_reference(tmp_path, platform):
    _, snapshot, _ = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", snapshot.session)
    message = ({"document": {"file_id": "file-1", "file_size": 6, "file_name": "report.txt"}}
               if platform == "telegram" else {"attachments": [{"id": "2", "size": 6,
                   "filename": "report.txt",
                   "url": "https://cdn.discordapp.com/attachments/456/2/report.txt?ex=fixture"}]})
    (value,) = decode_file_refs(route, message)
    assert value.filename == "report.txt" and value.byte_count == 6
    assert "ex=fixture" not in repr(value)
    with pytest.raises(ContractError):
        replace(value, byte_count=1048577)
    with pytest.raises(ContractError):
        replace(value, filename="../report.txt")
    if platform == "discord":
        for url in ["http://cdn.discordapp.com/attachments/456/2/report.txt",
                    "https://example.org/attachments/456/2/report.txt",
                    "https://cdn.discordapp.com/attachments/999/2/report.txt",
                    "https://cdn.discordapp.com/attachments/456/3/report.txt"]:
            with pytest.raises(ContractError):
                replace(value, download_url=url)
        with pytest.raises(ContractError):
            decode_file_refs(route, {"attachments": message["attachments"] * 2})
    else:
        with pytest.raises(ContractError):
            replace(value, download_url="https://example.org")
