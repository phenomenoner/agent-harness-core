"""Exercise the production HTTP path with a controlled connection boundary."""

import json

import pytest

from ahc.adapters import channel_text


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("failure", [None, "status", "oversize", "duplicate", "nan",
                                     "request", "read"])
def test_http_request_response_and_cleanup(monkeypatch, platform, failure):
    calls = []

    class Response:
        status = 302 if failure == "status" else 200

        def read(self, limit):
            assert limit == 65537
            if failure == "read":
                raise OSError("fixture read failed")
            if failure == "oversize":
                return b" " * 65537
            if failure == "duplicate":
                return b'{"result":{"id":1,"id":2}}'
            if failure == "nan":
                return b'{"id":NaN}'
            return b'{"id":"123"}'

    class Connection:
        def __init__(self, host, timeout):
            calls.append(("open", host, timeout))

        def request(self, method, path, body, headers):
            calls.append(("request", method, path, json.loads(body), headers))
            if failure == "request":
                raise OSError("fixture request failed")

        def getresponse(self):
            return Response()

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr(channel_text.http.client, "HTTPSConnection", Connection)
    if failure:
        with pytest.raises((ValueError, OSError)):
            channel_text._post(platform, "fixture", "456", {"text": "你好"})
    else:
        assert channel_text._post(platform, "fixture", "456", {"text": "你好"}) == {
            "id": "123"}
    assert len(calls) == 3 and calls[-1] == ("close",)
    assert calls[0] == ("open", "api.telegram.org" if platform == "telegram"
                        else "discord.com", 10)
    _, method, path, body, headers = calls[1]
    assert method == "POST" and body == {"text": "你好"}
    if platform == "telegram":
        assert path == "/botfixture/sendMessage" and "Authorization" not in headers
    else:
        assert path == "/api/v10/channels/456/messages"
        assert headers["Authorization"] == "Bot fixture"
