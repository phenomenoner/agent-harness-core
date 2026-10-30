"""Real downloader with controlled HTTP verifies bounded fetch and credential routing."""

import json

import pytest

from ahc.adapters import channel_file_download as module
from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelRoute
from ahc.domain.sessions import ContractError, VirtualSessionIdentity


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("failure", [None, "redirect", "oversize", "short", "encoding"])
def test_download_fixed_endpoint_and_bounds(monkeypatch, platform, failure):
    route = ChannelRoute(platform, "123", "456", "789",
                         VirtualSessionIdentity.initial("s", "a", "r"))
    reference = ChannelFile(platform, "456", "2", "report.txt", 4, "text/plain",
                            None if platform == "telegram" else
                            "https://cdn.discordapp.com/attachments/456/2/report.txt?ex=fixture")
    calls, closed, reads = [], [], []

    class Connection:
        def __init__(self, host, timeout):
            self.host = host
            assert timeout == 10

        def request(self, method, path, body, headers):
            self.metadata = method == "POST"
            calls.append((self.host, method, path, body, headers))

        def getresponse(self):
            self.status = 302 if failure == "redirect" and not self.metadata else 200
            return self

        def getheader(self, name, default):
            return "gzip" if failure == "encoding" and not self.metadata else default

        def read(self, count):
            reads.append(count)
            if self.metadata:
                return json.dumps({"ok": True, "result": {
                    "file_id": "2", "file_size": 4, "file_path": "documents/report.txt"
                }}).encode()
            return {"oversize": b"12345", "short": b"123"}.get(failure, b"1234")

        def close(self):
            closed.append(self.host)

    monkeypatch.setattr(module.http.client, "HTTPSConnection", Connection)
    downloader = module.BotFileDownload(route, "fixture-token")
    if failure:
        with pytest.raises(ContractError, match="^inbound file download unverified$"):
            downloader.fetch(reference)
    else:
        assert downloader.fetch(reference) == b"1234"
    assert len(calls) == len(closed) == (2 if platform == "telegram" else 1)
    assert reads[-1] == 5
    if platform == "discord":
        assert calls[0][0] == "cdn.discordapp.com"
        assert "fixture-token" not in repr(calls)
        assert calls[0][2].endswith("?ex=fixture")
    else:
        assert calls[0][2] == "/botfixture-token/getFile"
        assert calls[1][2] == "/file/botfixture-token/documents/report.txt"


@pytest.mark.parametrize("file_path", ["../secret", "/absolute", "a//b", "a?token=x"])
def test_telegram_invalid_file_path_never_fetches(file_path):
    route = ChannelRoute("telegram", "123", "456", "789",
                         VirtualSessionIdentity.initial("s", "a", "r"))
    reference = ChannelFile("telegram", "456", "2", "report.txt", 4, "text/plain")
    calls = []

    def request(*args):
        calls.append(args)
        return json.dumps({"ok": True, "result": {
            "file_id": "2", "file_size": 4, "file_path": file_path}}).encode()

    with pytest.raises(ContractError, match="download unverified"):
        module.BotFileDownload(route, "fixture-token", request=request).fetch(reference)
    assert len(calls) == 1
